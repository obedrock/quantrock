"""
NHL Goal Sniper — X API + Kalshi
Monitors X (Twitter) filtered stream for NHL goal tweets.
When a goal is detected, instantly fires a YES bet on Kalshi
before the market reprices.

Flow:
  X stream tweet received
  → is_goal_tweet() filter
  → extract_player_from_tweet()
  → find_goal_market() on Kalshi
  → place_order() IOC
  → log result + update stats
"""

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from typing import Optional

import httpx
from dotenv import load_dotenv

from kalshi_client import KalshiClient, KalshiOrder, BET_SIZE_DOLLARS

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")
X_STREAM_URL   = "https://api.twitter.com/2/tweets/search/stream"
X_RULES_URL    = "https://api.twitter.com/2/tweets/search/stream/rules"

COOLDOWN_SECONDS     = 30    # Prevent double-bet on same player/event
MAX_MARKET_PRICE     = 50    # Don't bet if market already > 50¢ (already repriced)
LOG_FILE             = "goal_sniper_log.json"
STATS_FILE           = "goal_sniper_stats.json"

# ── NHL OFFICIAL X ACCOUNTS ───────────────────────────────────────────────────
# Ordered roughly by tweet speed based on community observation.
# @NHL posts every goal. Team accounts post their own goals fastest.
NHL_ACCOUNTS = [
    "NHL",
    "EdmontonOilers", "MapleLeafs",      "NHLBruins",       "NYRangers",
    "NJDevils",        "NYIsles",         "Capitals",        "DetroitRedWings",
    "BlueJacketsNHL",  "PittsburghPens",  "Canes",           "TBLightning",
    "FlaPanthers",     "NashvillePreds",  "AnaheimDucks",    "LAKings",
    "SJSharks",        "ColoradoAvalanche","VGKGoldenKnights","SeattleKraken",
    "DallasStars",     "CalgaryFlames",   "Canucks",         "OttawaSenators",
    "CanadiensMTL",    "BuffaloSabres",   "MNWild",          "NHLJets",
    "StLouisBlues",    "ChicagoBlackhawks","NHLFlyers",
]

# ── GOAL DETECTION KEYWORDS ───────────────────────────────────────────────────
GOAL_KEYWORDS = [
    "GOAL", "scores", "SCORES", "scored", "SCORED",
    "🚨",                    # Goal siren — used almost universally
    "lights the lamp",
    "LIGHT THE LAMP",
    "GOAL ALERT",
]

# False-positive exclusions
EXCLUDE_KEYWORDS = [
    "highlights", "last night", "yesterday", "recap",
    "preview", "schedule", "ticket", "on this day",
    "goal of the year", "goal of the week", "all-time",
    "career goal", "milestone",
]

# ── KNOWN PLAYERS (for fast name extraction) ──────────────────────────────────
NHL_PLAYERS = [
    "McDavid", "Matthews", "MacKinnon", "Draisaitl", "Ovechkin",
    "Crosby",  "Hedman",   "Makar",     "Kaprizov",  "Tkachuk",
    "Pastrnak","Marchand",  "Tavares",   "Marner",    "Scheifele",
    "Rantanen","Huberdeau", "Nylander",  "Rielly",    "Forsberg",
    "Stamkos", "Kucherov",  "Point",     "Barkov",    "Reinhart",
    "Giroux",  "Gaudreau",  "Couturier", "Voracek",   "Bergeron",
    "Ekblad",  "Fox",       "Slavin",    "Josi",      "Hamilton",
    "Pettersson","Quinn",   "Elias",     "Horvat",    "Boeser",
    "Laine",   "Dubois",    "Ehlers",    "Wheeler",   "Connor",
    "Trocheck","Panarin",   "Zibanejad", "Kreider",   "Lafreniere",
]


# ── DATA MODELS ───────────────────────────────────────────────────────────────

@dataclass
class GoalEvent:
    timestamp:           str
    tweet_id:            str
    tweet_text:          str
    author:              str
    player_name:         Optional[str]
    team:                Optional[str]
    tweet_to_detect_ms:  float   # Time from tweet receipt to goal confirmed


@dataclass
class BetRecord:
    timestamp:           str
    player_name:         Optional[str]
    tweet_text:          str
    tweet_author:        str
    kalshi_ticker:       Optional[str]
    market_price_cents:  Optional[int]      # Price we saw before betting
    contracts:           int
    bet_dollars:         float
    bet_fired:           bool
    filled:              bool
    filled_price_cents:  Optional[int]      # Actual fill price
    fill_latency_ms:     Optional[float]    # Tweet received → order filled
    pnl_est:             Optional[float]    # Estimated P&L if resolves YES
    miss_reason:         Optional[str]      # Why bet wasn't fired / filled
    false_positive:      bool = False       # Manually marked after review


# ── SESSION STATS ─────────────────────────────────────────────────────────────

@dataclass
class SessionStats:
    session_start:        str   = field(default_factory=lambda: datetime.now().isoformat())
    goals_detected:       int   = 0
    false_positives:      int   = 0
    markets_found:        int   = 0
    markets_not_found:    int   = 0
    bets_fired:           int   = 0
    bets_filled:          int   = 0
    bets_missed:          int   = 0    # Fired but not filled (IOC miss)
    total_wagered:        float = 0.0
    total_pnl_est:        float = 0.0  # Estimated if all open YES resolve
    latencies_ms:         list  = field(default_factory=list)
    fill_prices_cents:    list  = field(default_factory=list)

    @property
    def fill_rate(self) -> float:
        return (self.bets_filled / self.bets_fired * 100) if self.bets_fired else 0.0

    @property
    def market_hit_rate(self) -> float:
        total = self.markets_found + self.markets_not_found
        return (self.markets_found / total * 100) if total else 0.0

    @property
    def avg_latency_ms(self) -> float:
        return sum(self.latencies_ms) / len(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def min_latency_ms(self) -> float:
        return min(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def max_latency_ms(self) -> float:
        return max(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def avg_fill_price_cents(self) -> float:
        return sum(self.fill_prices_cents) / len(self.fill_prices_cents) if self.fill_prices_cents else 0.0

    def print_summary(self):
        uptime = datetime.now() - datetime.fromisoformat(self.session_start)
        hours  = str(uptime).split(".")[0]
        print("\n" + "="*60)
        print("  SESSION STATS")
        print("="*60)
        print(f"  Uptime:              {hours}")
        print(f"  Goals detected:      {self.goals_detected}")
        print(f"  False positives:     {self.false_positives}")
        print(f"  Market hit rate:     {self.market_hit_rate:.1f}%  ({self.markets_found}/{self.markets_found+self.markets_not_found})")
        print(f"  Bets fired:          {self.bets_fired}")
        print(f"  Fill rate:           {self.fill_rate:.1f}%  ({self.bets_filled}/{self.bets_fired})")
        print(f"  Total wagered:       ${self.total_wagered:.2f}")
        print(f"  Est. open P&L:       ${self.total_pnl_est:.2f}")
        print(f"  Avg fill price:      {self.avg_fill_price_cents:.0f}¢")
        print(f"  Avg latency:         {self.avg_latency_ms:.0f}ms")
        print(f"  Min latency:         {self.min_latency_ms:.0f}ms")
        print(f"  Max latency:         {self.max_latency_ms:.0f}ms")
        print("="*60 + "\n")


# ── TWEET ANALYSIS ────────────────────────────────────────────────────────────

def is_goal_tweet(text: str) -> bool:
    t = text.lower()
    if not any(kw.lower() in t for kw in GOAL_KEYWORDS):
        return False
    if any(kw.lower() in t for kw in EXCLUDE_KEYWORDS):
        return False
    return True


def extract_player(text: str) -> Optional[str]:
    # Check known list first — fastest path
    for player in NHL_PLAYERS:
        if player.lower() in text.lower():
            return player
    # Regex patterns for unknown players
    for pattern in [
        r'([A-Z][a-z]+\s[A-Z][a-z]+)\s(?:scores|SCORES|scored)',
        r'GOAL[!🚨\s]+([A-Z][a-z]+\s[A-Z][a-z]+)',
        r'([A-Z][a-z]+-?[A-Z]?[a-z]*)\s(?:scores|SCORES|nets|buries|rips)',
    ]:
        m = re.search(pattern, text)
        if m:
            return m.group(1)
    return None


# ── KALSHI MARKET FINDER ──────────────────────────────────────────────────────

async def find_goal_market(kalshi: KalshiClient, player_name: Optional[str]) -> Optional[dict]:
    """
    Search Kalshi for an open anytime goal scorer market.
    Returns the market dict if found and not yet repriced, else None.
    """
    query = player_name.split()[-1] if player_name else "goal"
    markets = await kalshi.search_prop_markets(query, sport="NHL")

    for m in markets:
        title = m.get("title", "").lower()
        if "goal" not in title:
            continue
        if m.get("status") != "open":
            continue
        yes_ask = m.get("yes_ask", 100)
        if yes_ask >= MAX_MARKET_PRICE:
            print(f"   ⚠️  Market already repriced: {m.get('ticker')} @ {yes_ask}¢")
            continue
        return m

    return None


# ── X FILTERED STREAM ─────────────────────────────────────────────────────────

class XGoalStream:
    def __init__(self):
        self.headers = {
            "Authorization": f"Bearer {X_BEARER_TOKEN}",
            "Content-Type":  "application/json",
        }
        self._cooldowns: dict = {}  # key → last fired timestamp

    def _on_cooldown(self, key: str) -> bool:
        last = self._cooldowns.get(key, 0)
        return (time.time() - last) < COOLDOWN_SECONDS

    def _set_cooldown(self, key: str):
        self._cooldowns[key] = time.time()

    async def _setup_rules(self, client: httpx.AsyncClient):
        # Delete old rules
        r = await client.get(X_RULES_URL, headers=self.headers)
        existing = r.json().get("data", [])
        if existing:
            ids = [rule["id"] for rule in existing]
            await client.post(X_RULES_URL, headers=self.headers,
                              json={"delete": {"ids": ids}})
            print(f"[X] Cleared {len(ids)} old stream rules")

        kw = '"GOAL" OR "scores" OR "🚨" OR "SCORES" OR "lights the lamp"'

        # Split accounts into two rules (X Basic: 512 chars/rule)
        rules = []
        for i, chunk in enumerate([NHL_ACCOUNTS[:16], NHL_ACCOUNTS[16:]]):
            accounts = " OR ".join(f"from:{a}" for a in chunk)
            rules.append({
                "value": f"({accounts}) ({kw}) -is:retweet lang:en",
                "tag":   f"nhl_goals_{i+1}",
            })

        r = await client.post(X_RULES_URL, headers=self.headers, json={"add": rules})
        summary = r.json().get("meta", {}).get("summary", {})
        print(f"[X] Stream rules set: {summary}")

    async def stream(self, on_goal):
        if not X_BEARER_TOKEN:
            raise RuntimeError("X_BEARER_TOKEN not set in .env")

        params = {
            "tweet.fields": "created_at,author_id,text",
            "expansions":   "author_id",
            "user.fields":  "username",
        }

        async with httpx.AsyncClient(timeout=None) as client:
            await self._setup_rules(client)
            print("[X] Connecting to filtered stream...")

            while True:
                try:
                    async with client.stream(
                        "GET", X_STREAM_URL,
                        headers=self.headers, params=params,
                        timeout=httpx.Timeout(None, connect=10),
                    ) as resp:
                        print(f"[X] Stream connected (HTTP {resp.status_code})")

                        async for line in resp.aiter_lines():
                            if not line.strip():
                                continue   # heartbeat keepalive

                            t_recv = time.time()
                            try:
                                data    = json.loads(line)
                                tweet   = data.get("data", {})
                                text    = tweet.get("text", "")
                                users   = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
                                author  = users.get(tweet.get("author_id", ""), {}).get("username", "?")

                                if not is_goal_tweet(text):
                                    continue

                                player = extract_player(text)
                                detect_ms = (time.time() - t_recv) * 1000

                                goal = GoalEvent(
                                    timestamp=datetime.now().isoformat(),
                                    tweet_id=tweet.get("id", ""),
                                    tweet_text=text,
                                    author=author,
                                    player_name=player,
                                    team=None,
                                    tweet_to_detect_ms=detect_ms,
                                )

                                print(f"\n🚨 GOAL TWEET: @{author} — {text[:80]}")
                                print(f"   Player extracted: {player or 'unknown'}")

                                key = player or author
                                if self._on_cooldown(key):
                                    print(f"   ⏳ Cooldown active — duplicate skipped")
                                    continue
                                self._set_cooldown(key)
                                await on_goal(goal)

                            except json.JSONDecodeError:
                                pass
                            except Exception as e:
                                print(f"[X] Parse error: {e}")

                except Exception as e:
                    print(f"[X] Disconnected: {e} — reconnecting in 5s...")
                    await asyncio.sleep(5)


# ── MAIN SNIPER ────────────────────────────────────────────────────────────────

class NHLGoalSniper:
    def __init__(self):
        self.kalshi  = KalshiClient()
        self.stream  = XGoalStream()
        self.records: list[BetRecord] = []
        self.stats   = SessionStats()

    async def on_goal(self, goal: GoalEvent):
        self.stats.goals_detected += 1
        t_start = time.time()

        market = await find_goal_market(self.kalshi, goal.player_name)

        if not market:
            self.stats.markets_not_found += 1
            print(f"   ❌ No Kalshi market found for {goal.player_name or 'unknown player'}")
            self._record(BetRecord(
                timestamp=goal.timestamp, player_name=goal.player_name,
                tweet_text=goal.tweet_text, tweet_author=goal.author,
                kalshi_ticker=None, market_price_cents=None, contracts=0,
                bet_dollars=0, bet_fired=False, filled=False,
                filled_price_cents=None, fill_latency_ms=None, pnl_est=None,
                miss_reason="No market found",
            ))
            return

        self.stats.markets_found += 1
        ticker     = market.get("ticker")
        yes_ask    = market.get("yes_ask", 50)
        contracts  = max(1, int(BET_SIZE_DOLLARS / (yes_ask / 100)))
        bet_amount = (yes_ask / 100) * contracts

        print(f"   📋 Market: {ticker} @ {yes_ask}¢ — firing {contracts} contracts (${bet_amount:.2f})")

        order = KalshiOrder(
            ticker=ticker, side="yes", count=contracts,
            price=yes_ask, order_type="limit", time_in_force="ioc",
        )
        result = await self.kalshi.place_order(order)
        total_latency = (time.time() - t_start) * 1000

        # Estimated P&L if resolves YES: payout - cost
        pnl_est = None
        if result.success:
            payout  = contracts * 1.00          # $1 per contract
            fee_est = payout * 0.07             # ~7% Kalshi fee
            pnl_est = round(payout - fee_est - bet_amount, 2)

        rec = BetRecord(
            timestamp=goal.timestamp, player_name=goal.player_name,
            tweet_text=goal.tweet_text, tweet_author=goal.author,
            kalshi_ticker=ticker, market_price_cents=yes_ask,
            contracts=contracts, bet_dollars=bet_amount,
            bet_fired=True, filled=result.success,
            filled_price_cents=result.filled_price,
            fill_latency_ms=total_latency, pnl_est=pnl_est,
            miss_reason=result.error if not result.success else None,
        )
        self._record(rec)

        # Update stats
        self.stats.bets_fired += 1
        if result.success:
            self.stats.bets_filled   += 1
            self.stats.total_wagered += bet_amount
            self.stats.total_pnl_est += (pnl_est or 0)
            self.stats.latencies_ms.append(total_latency)
            if result.filled_price:
                self.stats.fill_prices_cents.append(result.filled_price)
            print(f"   ✅ FILLED {contracts} @ {yes_ask}¢ | latency {total_latency:.0f}ms | est P&L ${pnl_est:.2f}")
        else:
            self.stats.bets_missed += 1
            print(f"   ❌ Miss: {result.error}")

        self.stats.print_summary()
        self._save_stats()

    def _record(self, rec: BetRecord):
        self.records.insert(0, rec)
        self.records = self.records[:200]  # keep last 200
        with open(LOG_FILE, "w") as f:
            json.dump([asdict(r) for r in self.records], f, indent=2)

    def _save_stats(self):
        s = self.stats
        out = {
            "session_start":       s.session_start,
            "uptime":              str(datetime.now() - datetime.fromisoformat(s.session_start)).split(".")[0],
            "goals_detected":      s.goals_detected,
            "false_positives":     s.false_positives,
            "market_hit_rate_pct": round(s.market_hit_rate, 1),
            "bets_fired":          s.bets_fired,
            "bets_filled":         s.bets_filled,
            "fill_rate_pct":       round(s.fill_rate, 1),
            "total_wagered":       round(s.total_wagered, 2),
            "total_pnl_est":       round(s.total_pnl_est, 2),
            "avg_fill_price_cents":round(s.avg_fill_price_cents, 1),
            "avg_latency_ms":      round(s.avg_latency_ms, 0),
            "min_latency_ms":      round(s.min_latency_ms, 0),
            "max_latency_ms":      round(s.max_latency_ms, 0),
            "last_updated":        datetime.now().isoformat(),
        }
        with open(STATS_FILE, "w") as f:
            json.dump(out, f, indent=2)

    async def run(self):
        print("\n" + "="*60)
        print("  🏒 NHL GOAL SNIPER")
        print("="*60)
        print(f"  Bet size:    ${BET_SIZE_DOLLARS} per goal")
        print(f"  Max price:   {MAX_MARKET_PRICE}¢ (skip if already repriced)")
        print(f"  Cooldown:    {COOLDOWN_SECONDS}s per player")
        print(f"  Environment: {os.getenv('KALSHI_API_BASE', 'DEMO')}")
        print("="*60 + "\n")

        await self.kalshi.setup()
        await self.stream.stream(self.on_goal)


if __name__ == "__main__":
    sniper = NHLGoalSniper()
    asyncio.run(sniper.run())
