"""
NHL Goal Sniper — NHL Stats API + Kalshi
Polls the official NHL Stats API for live goal events.
When a goal is detected, instantly fires a YES bet on Kalshi
before the market reprices.

Flow:
  NHL Stats API poll (every 2s per live game)
  → new goal event detected
  → extract player name from roster data
  → find_goal_market() on Kalshi
  → place_order() IOC
  → log result + update stats
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Optional

import httpx
from dotenv import load_dotenv

from kalshi_client import KalshiClient, KalshiOrder, BET_SIZE_DOLLARS

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
NHL_API_BASE     = "https://api-web.nhle.com/v1"
POLL_INTERVAL    = 1.0   # seconds between poll cycles (all games run in parallel)
NO_GAMES_WAIT    = 30.0  # seconds to wait when no live games found
COOLDOWN_SECONDS = 30    # prevent double-bet on same player/event
MAX_MARKET_PRICE = 50    # don't bet if market already > 50¢ (already repriced)
LOG_FILE         = "goal_sniper_log.json"
STATS_FILE       = "goal_sniper_stats.json"


# ── DATA MODELS ───────────────────────────────────────────────────────────────

@dataclass
class GoalEvent:
    timestamp:         str
    event_id:          int
    game_id:           int
    description:       str           # human-readable goal description
    player_name:       Optional[str]
    team:              Optional[str]
    period:            int
    time_in_period:    str
    detect_latency_ms: float


@dataclass
class BetRecord:
    timestamp:          str
    player_name:        Optional[str]
    event_description:  str
    game_id:            int
    kalshi_ticker:      Optional[str]
    market_price_cents: Optional[int]
    contracts:          int
    bet_dollars:        float
    bet_fired:          bool
    filled:             bool
    filled_price_cents: Optional[int]
    fill_latency_ms:    Optional[float]
    pnl_est:            Optional[float]
    miss_reason:        Optional[str]
    false_positive:     bool = False


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
    bets_missed:          int   = 0
    total_wagered:        float = 0.0
    total_pnl_est:        float = 0.0
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


# ── NHL STATS API POLLER ───────────────────────────────────────────────────────

class NHLStatsPoller:
    """
    Polls the NHL Stats API for live goal events.
    No API key required — the NHL API is public.
    """

    def __init__(self):
        self._seen_event_ids: set = set()
        self._cooldowns: dict = {}
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0),
            headers={"User-Agent": "nhl-goal-sniper/1.0"},
        )

    def _on_cooldown(self, key: str) -> bool:
        last = self._cooldowns.get(key, 0)
        return (time.time() - last) < COOLDOWN_SECONDS

    def _set_cooldown(self, key: str):
        self._cooldowns[key] = time.time()

    async def _get_live_game_ids(self) -> list:
        """Fetch IDs of all currently live NHL games."""
        try:
            r = await self._client.get(f"{NHL_API_BASE}/score/now")
            r.raise_for_status()
            games = r.json().get("games", [])
            live = [g["id"] for g in games if g.get("gameState") in ("LIVE", "CRIT")]
            return live
        except Exception as e:
            print(f"[NHL] Score fetch error: {e}")
            return []

    async def _get_new_goals(self, game_id: int) -> list:
        """
        Fetch play-by-play for a game and return unseen goal events.
        Each goal is returned as a GoalEvent-ready tuple.
        """
        try:
            r = await self._client.get(f"{NHL_API_BASE}/gamecenter/{game_id}/play-by-play")
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[NHL] Play-by-play error (game {game_id}): {e}")
            return []

        # Build playerId → full name map from roster
        roster = {}
        for spot in data.get("rosterSpots", []):
            pid = spot.get("playerId")
            fname = spot.get("firstName", {}).get("default", "")
            lname = spot.get("lastName", {}).get("default", "")
            if pid:
                roster[pid] = f"{fname} {lname}".strip()

        # Build teamId → abbreviation map
        home = data.get("homeTeam", {})
        away = data.get("awayTeam", {})
        team_map = {
            home.get("id"): home.get("abbrev", "?"),
            away.get("id"): away.get("abbrev", "?"),
        }

        new_goals = []
        for play in data.get("plays", []):
            if play.get("typeDescKey") != "goal":
                continue

            event_id = play.get("eventId")
            if event_id is None or event_id in self._seen_event_ids:
                continue

            details       = play.get("details", {})
            scorer_id     = details.get("scoringPlayerId")
            player_name   = roster.get(scorer_id)
            team_id       = details.get("eventOwnerTeamId")
            team          = team_map.get(team_id, "?")
            period        = play.get("periodDescriptor", {}).get("number", 0)
            time_in_period = play.get("timeInPeriod", "")
            home_score    = details.get("homeScore", "?")
            away_score    = details.get("awayScore", "?")

            description = (
                f"GOAL — {player_name or 'Unknown'} ({team}) "
                f"P{period} {time_in_period} | "
                f"{away.get('abbrev','?')} {away_score}–{home.get('abbrev','?')} {home_score}"
            )

            new_goals.append({
                "event_id":       event_id,
                "player_name":    player_name,
                "team":           team,
                "period":         period,
                "time_in_period": time_in_period,
                "description":    description,
            })

        return new_goals

    async def _poll_game(self, game_id: int, on_goal) -> None:
        """Fetch and process new goals for a single game."""
        goals = await self._get_new_goals(game_id)
        for g in goals:
            t_detect = time.time()
            self._seen_event_ids.add(g["event_id"])

            print(f"\n🚨 GOAL DETECTED (game {game_id}): {g['description']}")
            print(f"   Player: {g['player_name'] or 'unknown'}")

            key = g["player_name"] or str(g["event_id"])
            if self._on_cooldown(key):
                print(f"   ⏳ Cooldown active — duplicate skipped")
                continue
            self._set_cooldown(key)

            goal = GoalEvent(
                timestamp=datetime.now().isoformat(),
                event_id=g["event_id"],
                game_id=game_id,
                description=g["description"],
                player_name=g["player_name"],
                team=g["team"],
                period=g["period"],
                time_in_period=g["time_in_period"],
                detect_latency_ms=(time.time() - t_detect) * 1000,
            )
            await on_goal(goal)

    async def poll(self, on_goal):
        print("[NHL] Starting live game poller...")

        while True:
            try:
                game_ids = await self._get_live_game_ids()

                if not game_ids:
                    print(f"[NHL] No live games — checking again in {int(NO_GAMES_WAIT)}s")
                    await asyncio.sleep(NO_GAMES_WAIT)
                    continue

                print(f"[NHL] Monitoring {len(game_ids)} live game(s): {game_ids}")

                # Poll all live games simultaneously
                await asyncio.gather(*[self._poll_game(gid, on_goal) for gid in game_ids])

                await asyncio.sleep(POLL_INTERVAL)

            except Exception as e:
                print(f"[NHL] Poller error: {e} — retrying in 5s")
                await asyncio.sleep(5)

    async def close(self):
        await self._client.aclose()


# ── MAIN SNIPER ────────────────────────────────────────────────────────────────

class NHLGoalSniper:
    def __init__(self):
        self.kalshi  = KalshiClient()
        self.poller  = NHLStatsPoller()
        self.records: list = []
        self.stats   = SessionStats()

    async def on_goal(self, goal: GoalEvent):
        self.stats.goals_detected += 1
        t_start = time.time()

        market = await find_goal_market(self.kalshi, goal.player_name)

        if not market:
            self.stats.markets_not_found += 1
            print(f"   ❌ No Kalshi market found for {goal.player_name or 'unknown player'}")
            self._record(BetRecord(
                timestamp=goal.timestamp,
                player_name=goal.player_name,
                event_description=goal.description,
                game_id=goal.game_id,
                kalshi_ticker=None,
                market_price_cents=None,
                contracts=0,
                bet_dollars=0,
                bet_fired=False,
                filled=False,
                filled_price_cents=None,
                fill_latency_ms=None,
                pnl_est=None,
                miss_reason="No market found",
            ))
            return

        self.stats.markets_found += 1
        ticker    = market.get("ticker")
        yes_ask   = market.get("yes_ask", 50)
        contracts = max(1, int(BET_SIZE_DOLLARS / (yes_ask / 100)))
        bet_amount = (yes_ask / 100) * contracts

        print(f"   📋 Market: {ticker} @ {yes_ask}¢ — firing {contracts} contracts (${bet_amount:.2f})")

        order = KalshiOrder(
            ticker=ticker, side="yes", count=contracts,
            price=yes_ask, order_type="limit", time_in_force="ioc",
        )
        result = await self.kalshi.place_order(order)
        total_latency = (time.time() - t_start) * 1000

        pnl_est = None
        if result.success:
            payout  = contracts * 1.00
            fee_est = payout * 0.07
            pnl_est = round(payout - fee_est - bet_amount, 2)

        rec = BetRecord(
            timestamp=goal.timestamp,
            player_name=goal.player_name,
            event_description=goal.description,
            game_id=goal.game_id,
            kalshi_ticker=ticker,
            market_price_cents=yes_ask,
            contracts=contracts,
            bet_dollars=bet_amount,
            bet_fired=True,
            filled=result.success,
            filled_price_cents=result.filled_price,
            fill_latency_ms=total_latency,
            pnl_est=pnl_est,
            miss_reason=result.error if not result.success else None,
        )
        self._record(rec)

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
        self.records = self.records[:200]
        with open(LOG_FILE, "w") as f:
            json.dump([asdict(r) for r in self.records], f, indent=2)

    def _save_stats(self):
        s = self.stats
        out = {
            "session_start":        s.session_start,
            "uptime":               str(datetime.now() - datetime.fromisoformat(s.session_start)).split(".")[0],
            "goals_detected":       s.goals_detected,
            "false_positives":      s.false_positives,
            "market_hit_rate_pct":  round(s.market_hit_rate, 1),
            "bets_fired":           s.bets_fired,
            "bets_filled":          s.bets_filled,
            "fill_rate_pct":        round(s.fill_rate, 1),
            "total_wagered":        round(s.total_wagered, 2),
            "total_pnl_est":        round(s.total_pnl_est, 2),
            "avg_fill_price_cents": round(s.avg_fill_price_cents, 1),
            "avg_latency_ms":       round(s.avg_latency_ms, 0),
            "min_latency_ms":       round(s.min_latency_ms, 0),
            "max_latency_ms":       round(s.max_latency_ms, 0),
            "last_updated":         datetime.now().isoformat(),
        }
        with open(STATS_FILE, "w") as f:
            json.dump(out, f, indent=2)

    async def run(self):
        print("\n" + "="*60)
        print("  🏒 NHL GOAL SNIPER")
        print("="*60)
        print(f"  Source:      NHL Stats API (api-web.nhle.com)")
        print(f"  Poll rate:   every {POLL_INTERVAL}s (all games in parallel)")
        print(f"  Bet size:    ${BET_SIZE_DOLLARS} per goal")
        print(f"  Max price:   {MAX_MARKET_PRICE}¢ (skip if already repriced)")
        print(f"  Cooldown:    {COOLDOWN_SECONDS}s per player")
        print(f"  Environment: {os.getenv('KALSHI_API_BASE', 'DEMO')}")
        print("="*60 + "\n")

        await self.kalshi.setup()
        await self.poller.poll(self.on_goal)


if __name__ == "__main__":
    sniper = NHLGoalSniper()
    asyncio.run(sniper.run())
