"""
Kalshi API Client — NHL Goal Sniper
Handles auth, market lookup, and instant order execution.
API Docs: https://trading-api.readme.io/reference

Auth: RSA private key (fastest) with email/password fallback.
All config loaded from .env file — never hardcode credentials.
"""

import base64
import json
import os
import time
from dataclasses import dataclass
from typing import Optional

import httpx
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG (from .env) ────────────────────────────────────────────────────────
# ⚠️  Default is DEMO. Switch to live URL only when fully tested.
# Demo: https://demo-api.kalshi.co/trade-api/v2
# Live: https://api.elections.kalshi.com/trade-api/v2
KALSHI_API_BASE         = os.getenv("KALSHI_API_BASE", "https://demo-api.kalshi.co/trade-api/v2")
KALSHI_KEY_ID           = os.getenv("KALSHI_KEY_ID")
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "kalshi_key.pem")
KALSHI_EMAIL            = os.getenv("KALSHI_EMAIL")
KALSHI_PASSWORD         = os.getenv("KALSHI_PASSWORD")

# ── RISK CONTROLS (from .env with defaults) ───────────────────────────────────
MAX_BET_DOLLARS    = int(os.getenv("MAX_BET_DOLLARS", 25))
MAX_BETS_PER_HOUR  = int(os.getenv("MAX_BETS_PER_HOUR", 10))
MIN_BALANCE_BUFFER = int(os.getenv("MIN_BALANCE_BUFFER", 50))
BET_SIZE_DOLLARS   = int(os.getenv("BET_SIZE_DOLLARS", 10))

# Balance cache TTL — avoids latency hit on every bet
BALANCE_CACHE_TTL = 60


@dataclass
class KalshiOrder:
    ticker:        str
    side:          str        # "yes" or "no"
    count:         int
    price:         int        # cents (1-99)
    order_type:    str = "limit"
    time_in_force: str = "ioc"  # immediate-or-cancel


@dataclass
class OrderResult:
    success:      bool
    order_id:     Optional[str]
    filled_price: Optional[float]
    filled_count: Optional[int]
    latency_ms:   float
    error:        Optional[str]


class KalshiClient:
    def __init__(self):
        self.client = httpx.AsyncClient(
            base_url=KALSHI_API_BASE,
            timeout=httpx.Timeout(5.0, connect=3.0),
        )
        self.token:              Optional[str]   = None
        self.private_key                         = None
        self.bets_this_hour:     int             = 0
        self.hour_reset_at:      float           = time.time() + 3600
        self._cached_balance:    Optional[float] = None
        self._balance_fetched_at: float          = 0.0

    # ── AUTH ──────────────────────────────────────────────────────────────────

    def _load_private_key(self):
        try:
            with open(KALSHI_PRIVATE_KEY_PATH, "rb") as f:
                self.private_key = serialization.load_pem_private_key(
                    f.read(), password=None, backend=default_backend()
                )
            print(f"[Kalshi] Private key loaded from {KALSHI_PRIVATE_KEY_PATH}")
        except FileNotFoundError:
            print(f"[Kalshi] Key file not found at {KALSHI_PRIVATE_KEY_PATH}")
        except Exception as e:
            print(f"[Kalshi] Key load error: {e}")

    def _sign_request(self, method: str, path: str, body: str = "") -> dict:
        ts = str(int(time.time() * 1000))
        message = (ts + method.upper() + path + body).encode()
        signature = self.private_key.sign(message, padding.PKCS1v15(), hashes.SHA256())
        return {
            "KALSHI-ACCESS-KEY":       KALSHI_KEY_ID,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    async def login_email(self):
        r = await self.client.post("/login", json={"email": KALSHI_EMAIL, "password": KALSHI_PASSWORD})
        r.raise_for_status()
        self.token = r.json()["token"]
        self.client.headers.update({"Authorization": f"Bearer {self.token}"})
        print("[Kalshi] Logged in via email/password")

    async def setup(self):
        self._load_private_key()
        if self.private_key:
            print("[Kalshi] Using RSA key auth")
        elif KALSHI_EMAIL and KALSHI_PASSWORD:
            await self.login_email()
        else:
            raise RuntimeError("No Kalshi credentials in .env")

    # ── BALANCE (cached) ──────────────────────────────────────────────────────

    async def get_balance(self, force_refresh: bool = False) -> float:
        now = time.time()
        if force_refresh or (now - self._balance_fetched_at) > BALANCE_CACHE_TTL:
            try:
                r = await self.client.get("/portfolio/balance")
                r.raise_for_status()
                self._cached_balance    = r.json().get("balance", 0) / 100
                self._balance_fetched_at = now
            except Exception as e:
                print(f"[Kalshi] Balance fetch error: {e}")
                return self._cached_balance or 0.0
        return self._cached_balance or 0.0

    # ── MARKET LOOKUP ─────────────────────────────────────────────────────────

    async def get_market(self, ticker: str) -> Optional[dict]:
        try:
            r = await self.client.get(f"/markets/{ticker}")
            r.raise_for_status()
            return r.json().get("market")
        except Exception as e:
            print(f"[Kalshi] Market fetch error {ticker}: {e}")
            return None

    async def search_prop_markets(self, query: str, sport: str = None, status: str = "open") -> list:
        params = {"status": status, "limit": 100}
        if sport:
            params["series_ticker"] = sport.upper()
        try:
            r = await self.client.get("/markets", params=params)
            r.raise_for_status()
            markets = r.json().get("markets", [])
            q = query.lower()
            return [m for m in markets
                    if q in m.get("title", "").lower() or q in m.get("subtitle", "").lower()]
        except Exception as e:
            print(f"[Kalshi] Market search error: {e}")
            return []

    # ── ORDER EXECUTION ───────────────────────────────────────────────────────

    def _check_rate_limit(self) -> bool:
        now = time.time()
        if now > self.hour_reset_at:
            self.bets_this_hour = 0
            self.hour_reset_at  = now + 3600
        return self.bets_this_hour < MAX_BETS_PER_HOUR

    async def place_order(self, order: KalshiOrder) -> OrderResult:
        t_start = time.time()

        if not self._check_rate_limit():
            return OrderResult(False, None, None, None, 0.0,
                               f"Rate limit: {MAX_BETS_PER_HOUR}/hr reached")

        cost = (order.price / 100) * order.count
        if cost > MAX_BET_DOLLARS:
            return OrderResult(False, None, None, None, 0.0,
                               f"Bet ${cost:.2f} > MAX ${MAX_BET_DOLLARS}")

        balance = await self.get_balance()
        if balance - cost < MIN_BALANCE_BUFFER:
            return OrderResult(False, None, None, None, 0.0,
                               f"Balance ${balance:.2f} too low (buffer ${MIN_BALANCE_BUFFER})")

        price_key = "yes_price" if order.side == "yes" else "no_price"
        body = {
            "ticker":          order.ticker,
            "action":          "buy",
            "side":            order.side,
            "count":           order.count,
            "type":            order.order_type,
            price_key:         order.price,
            "time_in_force":   order.time_in_force,
            "client_order_id": f"sniper-{int(time.time()*1000)}",
        }
        body_str = json.dumps(body, separators=(",", ":"))

        extra_headers = {}
        if self.private_key:
            extra_headers = self._sign_request("POST", "/trade-api/v2/portfolio/orders", body_str)

        try:
            r = await self.client.post(
                "/portfolio/orders",
                content=body_str,
                headers={"Content-Type": "application/json", **extra_headers},
            )
            latency_ms = (time.time() - t_start) * 1000
            r.raise_for_status()
            data = r.json().get("order", {})
            self.bets_this_hour += 1
            self._balance_fetched_at = 0.0  # invalidate cache after fill
            return OrderResult(True, data.get("order_id"),
                               data.get("yes_price") or data.get("no_price"),
                               data.get("count"), latency_ms, None)
        except httpx.HTTPStatusError as e:
            return OrderResult(False, None, None, None,
                               (time.time()-t_start)*1000,
                               f"HTTP {e.response.status_code}: {e.response.text[:300]}")
        except Exception as e:
            return OrderResult(False, None, None, None,
                               (time.time()-t_start)*1000, str(e))

    async def close(self):
        await self.client.aclose()
