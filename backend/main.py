import asyncio
import io
import time
from datetime import date
from typing import Optional

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Strikes and Downs API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "https://strikes-and-downs.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PARQUET_URL = "https://raw.githubusercontent.com/jameskroeker/mlb-betting-data-pipeline/main/data/master/master_template.parquet"
DAILY_CSV_URL = "https://raw.githubusercontent.com/jameskroeker/mlb-betting-data-pipeline/main/data/daily/MLB_Combined_Odds_Results_{date}.csv"
ARCHIVE_CSV_URL = "https://raw.githubusercontent.com/jameskroeker/mlb-betting-data-pipeline/main/data/archive/MLB/2026/MLB_Combined_Odds_Results_{date}.csv"

# Maps full team names (as they appear in daily CSV) to parquet team_abbr values
TEAM_NAME_TO_ABBR: dict[str, str] = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KCR",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Athletics": "ATH",
    "Oakland Athletics": "ATH",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SDP",
    "San Francisco Giants": "SFG",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TBR",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}

TEAM_DIVISION: dict[str, str] = {
    "NYY": "AL East", "BOS": "AL East", "TBR": "AL East", "TOR": "AL East", "BAL": "AL East",
    "CWS": "AL Central", "CLE": "AL Central", "DET": "AL Central", "KCR": "AL Central", "MIN": "AL Central",
    "ATH": "AL West", "HOU": "AL West", "LAA": "AL West", "SEA": "AL West", "TEX": "AL West",
    "ATL": "NL East", "MIA": "NL East", "NYM": "NL East", "PHI": "NL East", "WSH": "NL East",
    "CHC": "NL Central", "CIN": "NL Central", "MIL": "NL Central", "PIT": "NL Central", "STL": "NL Central",
    "ARI": "NL West", "COL": "NL West", "LAD": "NL West", "SDP": "NL West", "SFG": "NL West",
}

TEAM_LEAGUE: dict[str, str] = {
    abbr: div.split()[0] for abbr, div in TEAM_DIVISION.items()
}

PARQUET_CACHE_TTL = 30 * 60  # 30 minutes

_master_df: Optional[pd.DataFrame] = None
_master_df_loaded_at: float = 0.0


async def fetch_master_df() -> pd.DataFrame:
    global _master_df, _master_df_loaded_at
    if _master_df is not None and (time.monotonic() - _master_df_loaded_at) < PARQUET_CACHE_TTL:
        return _master_df
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(PARQUET_URL)
        response.raise_for_status()
    df = pd.read_parquet(io.BytesIO(response.content))

    # Pre-compute bucketed columns once on load — avoids recomputing on every request
    df["_team_bucket"] = df["Win_Pct"].apply(win_pct_bucket)
    df["_odds_bucket"] = df["h2h_own_odds"].apply(
        lambda x: odds_bucket(x) if pd.notna(x) else None
    )
    df["_total_bucket"] = df["Total"].apply(
        lambda x: total_bucket(x) if pd.notna(x) else None
    )

    # Vectorized opponent bucket — merge opponent win_pct by game_id + opponent_abbr
    opp_pcts = df.groupby(["game_id", "team_abbr"])["Win_Pct"].first().reset_index()
    opp_pcts.columns = ["game_id", "opponent_abbr", "_opp_win_pct"]
    df = df.merge(opp_pcts, on=["game_id", "opponent_abbr"], how="left")
    df["_opp_bucket"] = df["_opp_win_pct"].apply(
        lambda x: win_pct_bucket(float(x)) if pd.notna(x) else None
    )

    # Rolling L10 bucket per team (historical pattern matching)
    df = df.sort_values(["team_abbr", "game_date_et"])
    df["_l10_wins"] = df.groupby("team_abbr")["team_won"].transform(
        lambda x: x.rolling(10, min_periods=5).sum().shift(1)
    )
    df["_l10_bucket"] = df["_l10_wins"].apply(
        lambda x: last_10_bucket(int(x)) if pd.notna(x) else None
    )

    # L10 runs scored and allowed per team (rolling average, shift forward 1)
    df = df.sort_values(["team_abbr", "season", "game_date_et"])
    df["_runs_scored"] = df.apply(
        lambda r: r["home_score"] if r["is_home"] else r["away_score"], axis=1
    )
    df["_runs_allowed"] = df.apply(
        lambda r: r["away_score"] if r["is_home"] else r["home_score"], axis=1
    )
    df["_l10_runs_scored"] = df.groupby(["team_abbr", "season"])["_runs_scored"].transform(
        lambda x: x.rolling(10, min_periods=5).mean().shift(1)
    ).round(1)
    df["_l10_runs_allowed"] = df.groupby(["team_abbr", "season"])["_runs_allowed"].transform(
        lambda x: x.rolling(10, min_periods=5).mean().shift(1)
    ).round(1)

    # Game count bucket — games played in season at time of each game
    df["_game_num"] = df.groupby(["team_abbr", "season"]).cumcount() + 1
    df["_game_count_bucket"] = df["_game_num"].apply(game_count_bucket)
    # Shift streak forward by 1 — streak bucket represents streak ENTERING the game
    # Win_Streak/Loss_Streak on each row is post-game, so the entering streak
    # for game N is the streak recorded after game N-1
    df = df.sort_values(["team_abbr", "season", "game_date_et"])
    df["_entering_win_streak"] = df.groupby(["team_abbr", "season"])["Win_Streak"].shift(1).fillna(0).astype(int)
    df["_entering_loss_streak"] = df.groupby(["team_abbr", "season"])["Loss_Streak"].shift(1).fillna(0).astype(int)
    df["_streak_bucket"] = df.apply(
        lambda r: streak_bucket(int(r["_entering_win_streak"]), int(r["_entering_loss_streak"])), axis=1
    )

    _master_df = df
    _master_df_loaded_at = time.monotonic()
    return _master_df


# Daily CSV cache — 5 minute TTL per date
_daily_csv_cache: dict[str, tuple[pd.DataFrame, float]] = {}
DAILY_CSV_CACHE_TTL = 5 * 60  # 5 minutes


# Signal accuracy cache — 30 minute TTL
_accuracy_cache: dict = {}
ACCURACY_CACHE_TTL = 30 * 60

# Signal accuracy cache — 30 minute TTL
_accuracy_cache: dict = {}
ACCURACY_CACHE_TTL = 30 * 60
# Signals cache — 5 minute TTL per date
_signals_cache: dict[str, tuple[dict, float]] = {}
SIGNALS_CACHE_TTL = 5 * 60  # 5 minutes

async def fetch_daily_csv(game_date: str) -> pd.DataFrame:
    global _daily_csv_cache
    cached = _daily_csv_cache.get(game_date)
    if cached is not None and (time.monotonic() - cached[1]) < DAILY_CSV_CACHE_TTL:
        return cached[0]
    url = DAILY_CSV_URL.format(date=game_date)
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url)
        if response.status_code == 404:
            archive_url = ARCHIVE_CSV_URL.format(date=game_date)
            response = await client.get(archive_url)
        response.raise_for_status()
    df = pd.read_csv(io.StringIO(response.text))
    _daily_csv_cache[game_date] = (df, time.monotonic())
    return df


def format_time_et(val) -> str:
    """Format a game time to '2:15 PM ET'.

    Handles pandas Timestamp objects, full datetime strings like
    '2026-03-28 14:15:00' or '2026-03-28 14:15:00nan', and bare
    time strings like '14:15 ET' or '14:15'.
    """
    # pandas Timestamp / datetime object — use attributes directly
    if hasattr(val, "hour"):
        h, m = val.hour, val.minute
        period = "AM" if h < 12 else "PM"
        return f"{h % 12 or 12}:{m:02d} {period} ET"

    raw = str(val).strip()
    if not raw or raw.lower() in ("nan", "none", "nat", ""):
        return "TBD"

    # Full datetime string "2026-03-28 14:15:00..." → slice off the time portion
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        raw = raw[11:]  # "14:15:00" or "14:15:00nan"

    # raw is now something like "14:15:00nan", "14:15 ET", or "14:15"
    parts = raw.split(":")
    try:
        h = int(parts[0])
        # strip any non-digit suffix from the minutes field ("00nan" → 0, "15 ET" → 15)
        m = int("".join(c for c in parts[1] if c.isdigit()))
        period = "AM" if h < 12 else "PM"
        return f"{h % 12 or 12}:{m:02d} {period} ET"
    except (ValueError, IndexError):
        return "TBD"


def get_team_stats(master_df: pd.DataFrame, team_abbr: str, season: int) -> dict:
    """Return the most recent stats for a team filtered to the given season."""
    team_data = master_df[
        (master_df["team_abbr"] == team_abbr) & (master_df["season"] == season)
    ].copy()
    if team_data.empty:
        return {"wins": 0, "losses": 0, "win_pct": 0.0, "streak": ""}

    team_data["game_date_et"] = pd.to_datetime(team_data["game_date_et"], errors="coerce")
    latest = team_data.sort_values("game_date_et").iloc[-1]

    win_streak = int(latest.get("Win_Streak") or 0)
    loss_streak = int(latest.get("Loss_Streak") or 0)
    streak = f"W{win_streak}" if win_streak > 0 else (f"L{loss_streak}" if loss_streak > 0 else "")

    l10_scored = latest.get("_l10_runs_scored")
    l10_allowed = latest.get("_l10_runs_allowed")

    return {
        "wins": int(latest.get("Wins") or 0),
        "losses": int(latest.get("Losses") or 0),
        "win_pct": round(float(latest.get("Win_Pct") or 0.0), 3),
        "streak": streak,
        "l10_runs_scored": round(float(l10_scored), 1) if pd.notna(l10_scored) else None,
        "l10_runs_allowed": round(float(l10_allowed), 1) if pd.notna(l10_allowed) else None,
    }


def compute_tags(
    home_stats: dict,
    away_stats: dict,
    ml_home: Optional[float],
    ml_away: Optional[float],
) -> list[str]:
    tags: list[str] = []

    # Favorite / underdog tags
    if ml_home is not None and ml_away is not None:
        if ml_home < ml_away:
            tags.append("Home Favorite")
            tags.append("Away Underdog")
        else:
            tags.append("Away Favorite")
            tags.append("Home Underdog")

    for label, stats, own_ml, opp_ml in [
        ("Home", home_stats, ml_home, ml_away),
        ("Away", away_stats, ml_away, ml_home),
    ]:
        streak = stats.get("streak", "")
        if not streak or len(streak) < 2:
            continue
        direction, n_str = streak[0], streak[1:]
        n = int(n_str) if n_str.isdigit() else 0
        win_pct = stats.get("win_pct", 0.0)
        wins = stats.get("wins", 0)
        losses = stats.get("losses", 0)
        total_games = wins + losses

        # Hot / Cold streak tags
        if direction == "W" and n >= 3:
            tags.append(f"{label} Hot ({streak})")
        elif direction == "L" and n >= 3:
            tags.append(f"{label} Cold ({streak})")

        # Streak-based situational tags — mutually exclusive, most specific wins
        if direction == "L" and win_pct > 0.500 and own_ml is not None and opp_ml is not None and own_ml < opp_ml:
            if n >= 5:
                # Cold Streak Fade — winning record, L5+, favorite
                # Historical: 14-18% win rate next game
                tags.append(f"{label} Fade Spot (18% hist)")
            elif n >= 3:
                # Bounce Back Spot — winning record, L3-4, favorite
                # Historical: 65.5% win rate next game (n=476)
                tags.append(f"{label} Bounce Back Spot (65% hist)")

        # Hot Streak Bounce — losing record, W3+, underdog
        # Historical: 54.5% win rate next game (n=319)
        if (direction == "W" and n >= 3 and
            win_pct < 0.500 and
            own_ml is not None and opp_ml is not None and
            own_ml > opp_ml):
            tags.append(f"{label} Hot Underdog (54% hist)")

        # Bad team heavy favorite — early warning
        # Historical: 23.5% win rate (n=34, games 1-20)
        if (win_pct < 0.400 and
            own_ml is not None and opp_ml is not None and
            own_ml < 1.667 and  # -150 or shorter
            total_games <= 20):
            tags.append(f"{label} Bad Team Favored (24% hist)")

    return tags


def safe_float(val) -> Optional[float]:
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def safe_int(val) -> Optional[int]:
    try:
        f = float(val)
        return None if pd.isna(f) else int(f)
    except (TypeError, ValueError):
        return None



@app.on_event("startup")
async def warmup_cache():
    """Pre-warm accuracy cache on startup so first user request is instant."""
    import asyncio
    async def _warm():
        try:
            await get_signal_accuracy()
            print("Accuracy cache warmed on startup")
        except Exception as e:
            print(f"Warmup failed: {e}")
    asyncio.create_task(_warm())

@app.get("/api/games/today")
async def get_today_games():
    return await get_games_for_date(date.today().strftime("%Y-%m-%d"))


@app.get("/api/games/{game_date}")
async def get_games_for_date(game_date: str):
    try:
        daily_df, master_df = await asyncio.gather(
            fetch_daily_csv(game_date),
            fetch_master_df(),
        )
    except httpx.HTTPStatusError:
        raise HTTPException(status_code=404, detail=f"No game data found for {game_date}")

    games = []
    for _, row in daily_df.iterrows():
        home_name = str(row["home_team"])
        away_name = str(row["away_team"])
        home_abbr = TEAM_NAME_TO_ABBR.get(home_name, home_name[:3].upper())
        away_abbr = TEAM_NAME_TO_ABBR.get(away_name, away_name[:3].upper())

        season = int(game_date[:4])
        home_stats = get_team_stats(master_df, home_abbr, season)
        away_stats = get_team_stats(master_df, away_abbr, season)

        ml_home = safe_float(row.get("moneyline_home"))
        ml_away = safe_float(row.get("moneyline_away"))

        games.append({
            "game_id": str(row["game_id"]),
            "game_date": str(row["game_date"]),
            "start_time_et": format_time_et(row.get("start_time_et")),
            "status": "Scheduled" if pd.isna(row.get("status")) else str(row.get("status")),
            "home_team": {"abbr": home_abbr, "name": home_name, **home_stats},
            "away_team": {"abbr": away_abbr, "name": away_name, **away_stats},
            "odds": {
                "moneyline_home": ml_home,
                "moneyline_away": ml_away,
                "total_line": safe_float(row.get("total_line")),
                "over_odds": safe_float(row.get("over_odds")),
                "under_odds": safe_float(row.get("under_odds")),
            },
            "tags": compute_tags(home_stats, away_stats, ml_home, ml_away),
            "home_score": safe_int(row.get("home_score")),
            "away_score": safe_int(row.get("away_score")),
        })

    return {"date": game_date, "games": games}


@app.get("/api/debug/abbrs/{game_date}")
async def debug_abbrs(game_date: str):
    """Show abbr resolution and parquet match counts for every team in a daily CSV."""
    try:
        daily_df, master_df = await asyncio.gather(
            fetch_daily_csv(game_date),
            fetch_master_df(),
        )
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=404, detail=str(e))

    season = int(game_date[:4])
    season_df = master_df[master_df["season"] == season]
    available_abbrs = sorted(season_df["team_abbr"].unique().tolist())

    rows = []
    for _, row in daily_df.iterrows():
        for role, col in [("home", "home_team"), ("away", "away_team")]:
            name = str(row[col])
            abbr = TEAM_NAME_TO_ABBR.get(name, f"UNMAPPED:{name[:3].upper()}")
            matched = int((season_df["team_abbr"] == abbr).sum())
            rows.append({"role": role, "csv_name": name, "abbr": abbr, "season_rows": matched})

    return {
        "game_date": game_date,
        "season": season,
        "available_abbrs_in_parquet": available_abbrs,
        "lookups": rows,
    }


@app.get("/health")
async def health():
    age = time.monotonic() - _master_df_loaded_at if _master_df is not None else None
    return {
        "status": "ok",
        "parquet_cached": _master_df is not None,
        "parquet_age_seconds": round(age) if age is not None else None,
        "parquet_ttl_seconds": PARQUET_CACHE_TTL,
    }


# ── Situations Engine ──────────────────────────────────────────────

def win_pct_bucket(win_pct: float) -> str:
    if win_pct >= 0.59: return "elite"
    if win_pct >= 0.53: return "good"
    if win_pct >= 0.47: return "average"
    if win_pct >= 0.41: return "poor"
    return "bad"

def total_bucket(total: float) -> str:
    if total < 7.0:   return "low"
    if total <= 7.5:  return "low-mid"
    if total <= 8.5:  return "mid-high"
    return "high"

def game_count_bucket(game_num: int) -> str:
    """Bucket by games played in season — captures early/late season context."""
    if game_num <= 20:   return "early"       # Games 1-20
    if game_num <= 60:   return "mid-early"   # Games 21-60
    if game_num <= 100:  return "mid"         # Games 61-100
    if game_num <= 130:  return "mid-late"    # Games 101-130
    return "late"                             # Games 131-162

GAME_COUNT_BUCKET_LABELS = {
    "early":    "Early season (G1-20)",
    "mid-early": "Pre-May (G21-60)",
    "mid":      "Mid-season (G61-100)",
    "mid-late": "Post All-Star (G101-130)",
    "late":     "Stretch run (G131-162)",
}

def streak_bucket(win_streak: int, loss_streak: int) -> str:
    """Bucket current streak — positive = wins, negative = losses."""
    if win_streak >= 3:   return "hot"
    if win_streak >= 1:   return "warm"
    if loss_streak >= 3:  return "ice"
    if loss_streak >= 1:  return "cold"
    return "neutral"

STREAK_BUCKET_LABELS = {
    "hot":     "W3+ streak",
    "warm":    "W1-2 streak",
    "neutral": "No streak",
    "cold":    "L1-2 streak",
    "ice":     "L3+ streak",
}

def odds_bucket(decimal_odds: float) -> str:
    """Bucket a team's own moneyline odds — 10 tiers with -200 and +150 as key anchors."""
    if decimal_odds < 1.40:  return "heavy_favorite"    # -400 to -250
    if decimal_odds < 1.50:  return "strong_favorite"   # -250 to -200
    if decimal_odds < 1.65:  return "favorite"          # -200 to -154
    if decimal_odds < 1.75:  return "mild_favorite"     # -154 to -133
    if decimal_odds < 2.00:  return "slight_favorite"   # -133 to even
    if decimal_odds < 2.10:  return "pick"              # even to +110
    if decimal_odds < 2.30:  return "slight_underdog"   # +110 to +130
    if decimal_odds < 2.50:  return "underdog"          # +130 to +150
    if decimal_odds < 3.25:  return "clear_underdog"    # +150 to +225
    return "big_underdog"                               # +225+

ODDS_BUCKET_LABELS = {
    "heavy_favorite":  "Heavy Favorite (-400 to -250)",
    "strong_favorite": "Strong Favorite (-250 to -200)",
    "favorite":        "Favorite (-200 to -154)",
    "mild_favorite":   "Mild Favorite (-154 to -133)",
    "slight_favorite": "Slight Favorite (-133 to even)",
    "pick":            "Pick (even to +110)",
    "slight_underdog": "Slight Underdog (+110 to +130)",
    "underdog":        "Underdog (+130 to +150)",
    "clear_underdog":  "Clear Underdog (+150 to +225)",
    "big_underdog":    "Big Underdog (+225+)",
}

WIN_PCT_BUCKET_LABELS = {
    "elite":   "Elite (59%+)",
    "good":    "Good (53-58%)",
    "average": "Average (47-52%)",
    "poor":    "Poor (41-46%)",
    "bad":     "Bad (<40%)",
}

TOTAL_BUCKET_LABELS = {
    "low":      "Total <7.0",
    "low-mid":  "Total 7.0-7.5",
    "mid-high": "Total 7.5-8.5",
    "high":     "Total 8.5+",
}

L10_BUCKET_LABELS = {
    "hot":     "L10 Hot (8-10 wins)",
    "average": "L10 Average (5-7 wins)",
    "cold":    "L10 Cold (0-4 wins)",
}

def get_last_10(team_df: pd.DataFrame) -> int:
    """Return number of wins in last 10 games for a team."""
    sorted_df = team_df.sort_values("game_date_et")
    last_10 = sorted_df.tail(10)
    return int(last_10["team_won"].sum())

def last_10_bucket(wins: int) -> str:
    if wins >= 8: return "hot"
    if wins >= 5: return "average"
    return "cold"

def deviation_score(wins: int, total: int) -> float:
    """How far win% deviates from 50% — used for ranking."""
    if total == 0: return 0.0
    return abs((wins / total) - 0.5)

def build_situation_label(filters: dict) -> str:
    parts = []
    role = "Home" if filters.get("is_home") else "Away"
    if filters.get("odds_bucket"):
        parts.append(f"{role} — {ODDS_BUCKET_LABELS.get(filters['odds_bucket'], filters['odds_bucket'])}")
    else:
        parts.append(role)
    if filters.get("team_bucket"):
        parts.append(WIN_PCT_BUCKET_LABELS.get(filters["team_bucket"], filters["team_bucket"]))
    if filters.get("l10_bucket"):
        parts.append(L10_BUCKET_LABELS.get(filters["l10_bucket"], filters["l10_bucket"]))
    if filters.get("total_bucket"):
        parts.append(TOTAL_BUCKET_LABELS.get(filters["total_bucket"], filters["total_bucket"]))
    if filters.get("opp_bucket"):
        parts.append(f"vs {WIN_PCT_BUCKET_LABELS.get(filters['opp_bucket'], filters['opp_bucket'])} opp")
    if filters.get("game_count_bucket"):
        parts.append(GAME_COUNT_BUCKET_LABELS.get(filters["game_count_bucket"], filters["game_count_bucket"]))
    if filters.get("streak_bucket"):
        parts.append(STREAK_BUCKET_LABELS.get(filters["streak_bucket"], filters["streak_bucket"]))
    return " | ".join(parts)

def query_situation(hist_df: pd.DataFrame, filters: dict, min_n: int = 8) -> Optional[dict]:
    """Apply filters to historical df and return situation result if n >= min_n."""
    df = hist_df.copy()

    if "is_home" in filters:
        df = df[df["is_home"] == filters["is_home"]]
    if "is_favorite" in filters:
        if filters["is_favorite"]:
            df = df[df["h2h_own_odds"] < df["h2h_opp_odds"]]
        else:
            df = df[df["h2h_own_odds"] >= df["h2h_opp_odds"]]
    if "team_bucket" in filters:
        df = df[df["_team_bucket"] == filters["team_bucket"]]
    if "opp_bucket" in filters:
        df = df[df["_opp_bucket"] == filters["opp_bucket"]]
    if "l10_bucket" in filters:
        df = df[df["_l10_bucket"] == filters["l10_bucket"]]
    if "odds_bucket" in filters:
        df = df[df["_odds_bucket"] == filters["odds_bucket"]]
    if "total_bucket" in filters:
        df = df[df["_total_bucket"] == filters["total_bucket"]]
    if "game_count_bucket" in filters:
        df = df[df["_game_count_bucket"] == filters["game_count_bucket"]]
    if "streak_bucket" in filters:
        df = df[df["_streak_bucket"] == filters["streak_bucket"]]

    # Only use completed games with known result
    df = df[df["team_won"].notna()]
    n = len(df)
    if n < min_n:
        return None

    wins = int(df["team_won"].sum())
    win_pct = round(wins / n, 3)
    dev = round(deviation_score(wins, n), 3)

    # Only surface meaningful deviations
    min_deviation = 0.08
    if dev < min_deviation:
        return None

    return {
        "label": build_situation_label(filters),
        "wins": wins,
        "losses": n - wins,
        "n": n,
        "win_pct": win_pct,
        "deviation": dev,
        "implied_prob": None,  # filled in by caller who knows the odds
        "value_gap": None,     # filled in by caller
    }


def query_league_situation(
    hist_df: pd.DataFrame,
    filters: dict,
    exclude_abbr: str,
) -> Optional[dict]:
    """League-wide situation query excluding the specific team."""
    # Bucketed columns already pre-computed on parquet load
    df = hist_df[hist_df["team_abbr"] != exclude_abbr].copy()

    if "is_home" in filters:
        df = df[df["is_home"] == filters["is_home"]]
    if "odds_bucket" in filters:
        df = df[df["_odds_bucket"] == filters["odds_bucket"]]
    if "team_bucket" in filters:
        df = df[df["_team_bucket"] == filters["team_bucket"]]
    if "opp_bucket" in filters:
        df = df[df["_opp_bucket"] == filters["opp_bucket"]]
    if "game_count_bucket" in filters:
        df = df[df["_game_count_bucket"] == filters["game_count_bucket"]]
    if "streak_bucket" in filters:
        df = df[df["_streak_bucket"] == filters["streak_bucket"]]

    df = df[df["team_won"].notna()]
    n = len(df)
    if n < 15:
        return None

    wins = int(df["team_won"].sum())
    win_pct = round(wins / n, 3)
    dev = round(deviation_score(wins, n), 3)

    if dev < 0.05:
        return None

    return {
        "label": build_situation_label(filters),
        "wins": wins,
        "losses": n - wins,
        "n": n,
        "win_pct": win_pct,
        "deviation": dev,
        "implied_prob": None,
        "value_gap": None,
    }


@app.get("/api/games/{game_id}/situations")
async def get_game_situations(game_id: str, game_date: str):
    """Return historical situational patterns for both teams in a game."""
    try:
        daily_df, master_df = await asyncio.gather(
            fetch_daily_csv(game_date),
            fetch_master_df(),
        )
    except httpx.HTTPStatusError:
        raise HTTPException(status_code=404, detail=f"No game data found for {game_date}")

    # Find the game in daily CSV
    game_row = daily_df[daily_df["game_id"].astype(str) == str(game_id)]
    if game_row.empty:
        raise HTTPException(status_code=404, detail=f"Game {game_id} not found for {game_date}")
    row = game_row.iloc[0]

    home_name = str(row["home_team"])
    away_name = str(row["away_team"])
    home_abbr = TEAM_NAME_TO_ABBR.get(home_name, home_name[:3].upper())
    away_abbr = TEAM_NAME_TO_ABBR.get(away_name, away_name[:3].upper())

    ml_home = safe_float(row.get("moneyline_home"))
    ml_away = safe_float(row.get("moneyline_away"))
    total = safe_float(row.get("total_line"))

    season = int(game_date[:4])

    # Use only historical seasons (not current) for pattern matching
    hist_df = master_df[master_df["season"] < season].copy()

    results = {}

    for abbr, is_home, own_ml, opp_ml in [
        (home_abbr, True, ml_home, ml_away),
        (away_abbr, False, ml_away, ml_home),
    ]:
        # Get this team's historical rows
        team_hist = hist_df[hist_df["team_abbr"] == abbr].copy()
        if team_hist.empty:
            results[abbr] = {"team_situations": [], "league_situations": []}
            continue

        # Get current season stats for bucketing
        season_stats = get_team_stats(master_df, abbr, season)
        team_bucket = win_pct_bucket(season_stats["win_pct"])

        # Calculate last 10 for historical rows per game (rolling)
        # For situational matching we use season win_pct at time of each game
        # Bucketed columns already pre-computed on parquet load — no recomputation needed

        is_fav = (own_ml < opp_ml) if (own_ml and opp_ml) else None
        t_bucket = total_bucket(total) if total else None
        own_odds_bucket = odds_bucket(own_ml) if own_ml else None

        # Current team's L10 from current season only (omit if fewer than 10 games played)
        current_season_df = master_df[
            (master_df["team_abbr"] == abbr) & (master_df["season"] == season)
        ].sort_values("game_date_et")
        current_l10_wins = int(current_season_df.tail(10)["team_won"].sum())
        current_l10_bucket = last_10_bucket(current_l10_wins) if len(current_season_df) >= 10 else None

        # Current team's opp bucket
        opp_stats = get_team_stats(master_df, away_abbr if is_home else home_abbr, season)
        opp_bucket = win_pct_bucket(opp_stats["win_pct"])

        # Define situations to test — from broadest to most specific
        # Note: total line excluded from win/loss situations — it's a scoring environment
        # signal not a winner predictor. Reserved for O/U analysis later.
        # Current team's game count bucket in this season
        current_game_num = len(current_season_df)
        current_gc_bucket = game_count_bucket(current_game_num) if current_game_num > 0 else None

        # Current streak bucket
        latest_row = current_season_df.iloc[-1] if not current_season_df.empty else None
        current_win_streak = int(latest_row["Win_Streak"] or 0) if latest_row is not None else 0
        current_loss_streak = int(latest_row["Loss_Streak"] or 0) if latest_row is not None else 0
        current_streak_bucket = streak_bucket(current_win_streak, current_loss_streak)

        situation_filters = [
            # Broadest baseline
            {"is_home": is_home},
            {"is_home": is_home, "odds_bucket": own_odds_bucket},
            # Team quality
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket},
            # Opponent quality
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "opp_bucket": opp_bucket},
            # Team vs opponent
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "opp_bucket": opp_bucket},
            # Team quality alone (no odds bucket — broader sample)
            {"is_home": is_home, "team_bucket": team_bucket, "opp_bucket": opp_bucket},
            # Game count context — early/late season patterns
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "game_count_bucket": current_gc_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "game_count_bucket": current_gc_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "opp_bucket": opp_bucket, "game_count_bucket": current_gc_bucket},
            # Streak context
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "streak_bucket": current_streak_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "streak_bucket": current_streak_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "opp_bucket": opp_bucket, "streak_bucket": current_streak_bucket},
            # 4-dimension combinations — streak + game count
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "streak_bucket": current_streak_bucket, "game_count_bucket": current_gc_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "opp_bucket": opp_bucket, "streak_bucket": current_streak_bucket, "game_count_bucket": current_gc_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "opp_bucket": opp_bucket, "streak_bucket": current_streak_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "opp_bucket": opp_bucket, "game_count_bucket": current_gc_bucket},
            # L10 form (only once 10+ games played)
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "l10_bucket": current_l10_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "l10_bucket": current_l10_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "opp_bucket": opp_bucket, "l10_bucket": current_l10_bucket},
        ]

        # Remove any filters with None values
        situation_filters = [
            {k: v for k, v in f.items() if v is not None}
            for f in situation_filters
        ]

        # Implied probability from today's odds
        implied_prob = round(1 / own_ml, 3) if own_ml and own_ml > 0 else None

        # Team-specific situations
        team_situations = []
        seen_labels = set()
        for filters in situation_filters:
            result = query_situation(team_hist, filters)
            if result and result["label"] not in seen_labels:
                seen_labels.add(result["label"])
                if implied_prob:
                    result["implied_prob"] = implied_prob
                    result["value_gap"] = round(result["win_pct"] - implied_prob, 3)
                team_situations.append(result)
        team_situations.sort(key=lambda x: x["deviation"], reverse=True)

        # League context situations — fixed filter set, exclude this team
        league_filters = [
            {"is_home": is_home, "odds_bucket": own_odds_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "opp_bucket": opp_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "opp_bucket": opp_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "game_count_bucket": current_gc_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "game_count_bucket": current_gc_bucket},
        ]
        league_filters = [
            {k: v for k, v in f.items() if v is not None}
            for f in league_filters
        ]
        league_situations = []
        seen_league_labels = set()
        for filters in league_filters:
            result = query_league_situation(hist_df, filters, exclude_abbr=abbr)
            if result and result["label"] not in seen_league_labels:
                seen_league_labels.add(result["label"])
                if implied_prob:
                    result["implied_prob"] = implied_prob
                    result["value_gap"] = round(result["win_pct"] - implied_prob, 3)
                league_situations.append(result)
        league_situations.sort(key=lambda x: x["deviation"], reverse=True)

        results[abbr] = {
            "team_situations": team_situations[:3],
            "league_situations": league_situations[:3],
        }

    return {
        "game_id": game_id,
        "game_date": game_date,
        "home_team": home_abbr,
        "away_team": away_abbr,
        "situations": results,
    }


# ── Signals Engine ─────────────────────────────────────────────────


@app.get("/api/query/ou")
async def query_ou(
    team_abbr: Optional[str] = None,
    is_home: Optional[str] = None,
    total_bucket: Optional[str] = None,
    home_l10_scored: Optional[str] = None,
    away_l10_scored: Optional[str] = None,
    team_bucket: Optional[str] = None,
    opp_bucket: Optional[str] = None,
    division_game: Optional[str] = None,
    season_filter: Optional[str] = None,
):
    """O/U query endpoint. Returns over/under/push rates for historical games matching filters."""
    master_df = await fetch_master_df()
    season = date.today().year
    hist_df = master_df[master_df["season"] < season].copy()

    if hist_df.empty:
        return {"error": "No historical data available"}

    # Work from home team perspective to avoid double counting
    home_df = hist_df[hist_df["is_home"] == True].copy()

    # Compute total result
    home_df = home_df[home_df["home_score"].notna() & home_df["Total"].notna()].copy()
    home_df["_total_result"] = home_df.apply(
        lambda r: "Over" if (r["home_score"] + r["away_score"]) > r["Total"]
        else ("Under" if (r["home_score"] + r["away_score"]) < r["Total"] else "Push"),
        axis=1
    )

    # Join away team L10 runs
    away_l10 = hist_df[hist_df["is_home"] == False][["game_id", "season", "_l10_runs_scored", "_l10_runs_allowed"]].copy()
    away_l10.columns = ["game_id", "season", "_away_l10_scored", "_away_l10_allowed"]
    home_df = home_df.merge(away_l10, on=["game_id", "season"], how="left")

    # Apply filters
    if team_abbr:
        home_df = home_df[home_df["team_abbr"] == team_abbr]

    if is_home is not None:
        if is_home.lower() == "true":
            pass  # already filtering home team rows only
        elif is_home.lower() == "false":
            # Flip to away perspective — use away team as subject
            away_perspective = hist_df[hist_df["is_home"] == False].copy()
            away_perspective = away_perspective[away_perspective["home_score"].notna() & away_perspective["Total"].notna()].copy()
            away_perspective["_total_result"] = away_perspective.apply(
                lambda r: "Over" if (r["home_score"] + r["away_score"]) > r["Total"]
                else ("Under" if (r["home_score"] + r["away_score"]) < r["Total"] else "Push"),
                axis=1
            )
            home_l10 = hist_df[hist_df["is_home"] == True][["game_id", "season", "_l10_runs_scored", "_l10_runs_allowed"]].copy()
            home_l10.columns = ["game_id", "season", "_away_l10_scored", "_away_l10_allowed"]
            away_perspective = away_perspective.merge(home_l10, on=["game_id", "season"], how="left")
            away_perspective["_l10_runs_scored"] = away_perspective["_l10_runs_scored"]
            home_df = away_perspective

    if team_bucket:
        home_df = home_df[home_df["_team_bucket"] == team_bucket]

    if opp_bucket:
        home_df = home_df[home_df["_opp_bucket"] == opp_bucket]

    if total_bucket:
        if total_bucket == "low":
            home_df = home_df[home_df["Total"] <= 7.0]
        elif total_bucket == "7.5":
            home_df = home_df[home_df["Total"] == 7.5]
        elif total_bucket == "8":
            home_df = home_df[home_df["Total"] == 8.0]
        elif total_bucket == "8.5":
            home_df = home_df[home_df["Total"] == 8.5]
        elif total_bucket == "9":
            home_df = home_df[home_df["Total"] == 9.0]
        elif total_bucket == "high":
            home_df = home_df[home_df["Total"] >= 9.5]

    def apply_l10_filter(df, col, val):
        if val == "under4":    return df[df[col] < 4.0]
        if val == "4to4.5":    return df[(df[col] >= 4.0) & (df[col] < 4.5)]
        if val == "4.5to5":    return df[(df[col] >= 4.5) & (df[col] < 5.0)]
        if val == "5to5.5":    return df[(df[col] >= 5.0) & (df[col] < 5.5)]
        if val == "5.5to6":    return df[(df[col] >= 5.5) & (df[col] < 6.0)]
        if val == "6plus":     return df[df[col] >= 6.0]
        return df

    if home_l10_scored:
        home_df = apply_l10_filter(home_df, "_l10_runs_scored", home_l10_scored)

    if away_l10_scored:
        home_df = apply_l10_filter(home_df, "_away_l10_scored", away_l10_scored)

    if division_game is not None:
        game_opp = hist_df.groupby("game_id")["team_abbr"].apply(list).to_dict()
        def check_division(row):
            teams = game_opp.get(row["game_id"], [])
            opp = [t for t in teams if t != row["team_abbr"]]
            if not opp: return False
            return TEAM_DIVISION.get(row["team_abbr"]) == TEAM_DIVISION.get(opp[0])
        mask = home_df.apply(check_division, axis=1)
        home_df = home_df[mask] if division_game.lower() == "true" else home_df[~mask]

    if season_filter:
        home_df = home_df[home_df["season"] == int(season_filter)]

    n = len(home_df)
    if n == 0:
        return {"over": 0, "under": 0, "push": 0, "n": 0, "over_pct": None, "under_pct": None, "message": "No matching games found"}

    over = int((home_df["_total_result"] == "Over").sum())
    under = int((home_df["_total_result"] == "Under").sum())
    push = int((home_df["_total_result"] == "Push").sum())
    decided = over + under

    return {
        "over": over,
        "under": under,
        "push": push,
        "n": n,
        "over_pct": round(over / decided, 3) if decided > 0 else None,
        "under_pct": round(under / decided, 3) if decided > 0 else None,
        "sample_warning": n < 20,
    }

@app.get("/api/query")
async def query_historical(
    is_home: Optional[str] = None,
    odds_bucket: Optional[str] = None,
    team_bucket: Optional[str] = None,
    opp_bucket: Optional[str] = None,
    game_count_bucket: Optional[str] = None,
    streak_entering: Optional[int] = None,
    streak_direction: Optional[str] = None,
    rest: Optional[str] = None,
    division_game: Optional[str] = None,
    interleague: Optional[str] = None,
    team_abbr: Optional[str] = None,
):
    """Flexible historical query endpoint for the query builder page."""
    master_df = await fetch_master_df()
    season = date.today().year
    hist_df = master_df[master_df["season"] < season].copy()

    if hist_df.empty:
        return {"error": "No historical data available"}

    df = hist_df.copy()

    if team_abbr:
        df = df[df["team_abbr"] == team_abbr]
    if is_home is not None:
        df = df[df["is_home"] == (is_home.lower() == "true")]
    if odds_bucket:
        df = df[df["_odds_bucket"] == odds_bucket]
    if team_bucket:
        df = df[df["_team_bucket"] == team_bucket]
    if opp_bucket:
        df = df[df["_opp_bucket"] == opp_bucket]
    if game_count_bucket:
        df = df[df["_game_count_bucket"] == game_count_bucket]

    if streak_entering is not None and streak_direction:
        if streak_direction.upper() == "W":
            if streak_entering == 10:
                df = df[df["_entering_win_streak"] >= 10]
            else:
                df = df[df["_entering_win_streak"] == streak_entering]
        elif streak_direction.upper() == "L":
            if streak_entering == 10:
                df = df[df["_entering_loss_streak"] >= 10]
            else:
                df = df[df["_entering_loss_streak"] == streak_entering]

    if rest is not None:
        df = df.sort_values(["team_abbr", "game_date_et"])
        df["_prev_game_date"] = df.groupby("team_abbr")["game_date_et"].shift(1)
        df["_days_rest"] = (
            pd.to_datetime(df["game_date_et"]) - pd.to_datetime(df["_prev_game_date"])
        ).dt.days - 1
        if rest.lower() == "b2b":
            df = df[df["_days_rest"] == 0]
        elif rest.lower() == "rest":
            df = df[df["_days_rest"] >= 1]

    if division_game is not None:
        game_opp = hist_df.groupby("game_id")["team_abbr"].apply(list).to_dict()
        def check_division(row):
            teams = game_opp.get(row["game_id"], [])
            opp = [t for t in teams if t != row["team_abbr"]]
            if not opp:
                return False
            return TEAM_DIVISION.get(row["team_abbr"]) == TEAM_DIVISION.get(opp[0])
        mask = df.apply(check_division, axis=1)
        df = df[mask] if division_game.lower() == "true" else df[~mask]

    if interleague is not None:
        game_opp = hist_df.groupby("game_id")["team_abbr"].apply(list).to_dict()
        def check_interleague(row):
            teams = game_opp.get(row["game_id"], [])
            opp = [t for t in teams if t != row["team_abbr"]]
            if not opp:
                return False
            return TEAM_LEAGUE.get(row["team_abbr"]) != TEAM_LEAGUE.get(opp[0])
        mask = df.apply(check_interleague, axis=1)
        df = df[mask] if interleague.lower() == "true" else df[~mask]

    df = df[df["team_won"].notna()]
    n = len(df)

    if n == 0:
        return {"wins": 0, "losses": 0, "n": 0, "win_pct": None, "deviation": None, "message": "No matching games found"}

    wins = int(df["team_won"].sum())
    losses = n - wins
    win_pct = round(wins / n, 3)
    dev = round(deviation_score(wins, n), 3)

    return {
        "wins": wins,
        "losses": losses,
        "n": n,
        "win_pct": win_pct,
        "deviation": dev,
        "sample_warning": n < 15,
    }




@app.get("/api/signals/accuracy")
async def get_signal_accuracy():
    """T1 accuracy since 2026-04-18. Reads locked signal files from GitHub — fast, drift-free."""
    global _accuracy_cache
    cached = _accuracy_cache.get("accuracy")
    if cached is not None and (time.monotonic() - cached[1]) < ACCURACY_CACHE_TTL:
        return cached[0]

    from datetime import date, timedelta

    master_df = await fetch_master_df()
    season = 2026
    season_df = master_df[master_df["season"] == season].copy()

    # Build outcomes lookup from parquet
    completed = season_df[season_df["team_won"].notna()]
    outcomes = {}
    for _, row in completed.iterrows():
        outcomes[(int(row["game_id"]), row["team_abbr"])] = bool(row["team_won"])

    SIGNALS_BASE_URL = "https://raw.githubusercontent.com/jameskroeker/mlb-betting-data-pipeline/main/data/signals/signals_{date}.json"

    track_start = date(2026, 4, 18)
    today = date.today()
    wins = 0
    losses = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        d = track_start
        while d < today:
            date_str = d.strftime("%Y-%m-%d")
            try:
                resp = await client.get(SIGNALS_BASE_URL.format(date=date_str))
                if resp.status_code == 404:
                    d += timedelta(days=1)
                    continue
                resp.raise_for_status()
                data = resp.json()
                for g in data.get("signals", []):
                    if g.get("tier") != 1:
                        continue
                    signal_team = g.get("signal_team")
                    if not signal_team:
                        continue
                    key = (int(g["game_id"]), signal_team)
                    if key not in outcomes:
                        continue
                    if outcomes[key]:
                        wins += 1
                    else:
                        losses += 1
            except Exception:
                pass
            d += timedelta(days=1)

    total = wins + losses
    result = {
        "wins": wins,
        "losses": losses,
        "total": total,
        "win_pct": round(wins / total, 3) if total > 0 else None,
        "since": str(track_start),
        "season": season,
    }
    _accuracy_cache["accuracy"] = (result, time.monotonic())
    return result

@app.get("/api/signals/{game_date}")
async def get_date_signals(game_date: str):
    """Consensus-based signal engine. Scores each game by how strongly both teams'
    historical patterns agree on an outcome. Returns tiered signals (T1=consensus, T2=single strong)."""

    global _signals_cache
    cached = _signals_cache.get(game_date)
    if cached is not None and (time.monotonic() - cached[1]) < SIGNALS_CACHE_TTL:
        return cached[0]

    daily_df, master_df = await asyncio.gather(
        fetch_daily_csv(game_date),
        fetch_master_df(),
    )
    season = int(game_date[:4])
    hist_df = master_df[master_df["season"] < season].copy()
    if daily_df is None or daily_df.empty:
        return {"date": game_date, "games_with_signals": 0, "total_games": 0, "signals": []}

    today_games = daily_df.drop_duplicates(subset=["game_id"])
    signals = []

    def dimension_multiplier(filters: dict) -> float:
        has_odds = "odds_bucket" in filters
        has_team = "team_bucket" in filters
        has_opp = "opp_bucket" in filters
        count = sum([has_odds, has_team, has_opp])
        if count == 3:
            return 1.5
        elif count == 2:
            return 1.0
        else:
            return 0.75

    def score_patterns(patterns: list, implied_prob: float) -> tuple:
        """Returns (directional_score, best_pattern) for a team's pattern list."""
        total_score = 0.0
        best = None
        best_score = 0.0
        for result in patterns:
            if result["n"] < 15 or result["deviation"] < 0.15:
                continue
            if implied_prob:
                result["value_gap"] = round(result["win_pct"] - implied_prob, 3)
                result["implied_prob"] = implied_prob
            score = result["deviation"] * dimension_multiplier(result.get("filters", {}))
            direction = 1.0 if result["win_pct"] > 0.50 else -1.0
            directional = score * direction
            total_score += directional
            if score > best_score:
                best_score = score
                best = result
        return total_score, best

    for _, row in today_games.iterrows():
        game_id = str(row["game_id"])
        home_abbr = TEAM_NAME_TO_ABBR.get(str(row["home_team"]), str(row["home_team"]))
        away_abbr = TEAM_NAME_TO_ABBR.get(str(row["away_team"]), str(row["away_team"]))

        season = int(game_date[:4])
        current_season_home = master_df[(master_df["team_abbr"] == home_abbr) & (master_df["season"] == season)]
        current_season_away = master_df[(master_df["team_abbr"] == away_abbr) & (master_df["season"] == season)]

        home_stats = get_team_stats(master_df, home_abbr, season)
        away_stats = get_team_stats(master_df, away_abbr, season)
        home_bucket = win_pct_bucket(home_stats["win_pct"])
        away_bucket = win_pct_bucket(away_stats["win_pct"])

        home_ml = row.get("moneyline_home")
        away_ml = row.get("moneyline_away")
        home_implied = round(1 / home_ml, 3) if home_ml and home_ml > 0 else None
        away_implied = round(1 / away_ml, 3) if away_ml and away_ml > 0 else None

        home_odds_bucket = odds_bucket(home_ml) if home_ml else None
        away_odds_bucket = odds_bucket(away_ml) if away_ml else None

        home_latest = current_season_home.iloc[-1] if not current_season_home.empty else None
        away_latest = current_season_away.iloc[-1] if not current_season_away.empty else None

        home_win_streak = int(home_latest["Win_Streak"] or 0) if home_latest is not None else 0
        home_loss_streak = int(home_latest["Loss_Streak"] or 0) if home_latest is not None else 0
        away_win_streak = int(away_latest["Win_Streak"] or 0) if away_latest is not None else 0
        away_loss_streak = int(away_latest["Loss_Streak"] or 0) if away_latest is not None else 0

        home_streak_bkt = streak_bucket(home_win_streak, home_loss_streak)
        away_streak_bkt = streak_bucket(away_win_streak, away_loss_streak)

        home_l10 = int(current_season_home.tail(10)["team_won"].sum())
        away_l10 = int(current_season_away.tail(10)["team_won"].sum())
        home_l10_bkt = last_10_bucket(home_l10) if len(current_season_home) >= 10 else None
        away_l10_bkt = last_10_bucket(away_l10) if len(current_season_away) >= 10 else None

        games_played_home = len(current_season_home)
        games_played_away = len(current_season_away)
        home_gc_bucket = game_count_bucket(games_played_home)
        away_gc_bucket = game_count_bucket(games_played_away)

        home_hist = hist_df[hist_df["team_abbr"] == home_abbr].copy()
        away_hist = hist_df[hist_df["team_abbr"] == away_abbr].copy()

        def build_filters(is_home, odds_bkt, team_bkt, opp_bkt, streak_bkt, l10_bkt, gc_bkt):
            base = [
                {"is_home": is_home, "odds_bucket": odds_bkt, "team_bucket": team_bkt, "opp_bucket": opp_bkt},
                {"is_home": is_home, "odds_bucket": odds_bkt, "team_bucket": team_bkt, "game_count_bucket": gc_bkt},
                {"is_home": is_home, "odds_bucket": odds_bkt, "opp_bucket": opp_bkt, "game_count_bucket": gc_bkt},
                {"is_home": is_home, "odds_bucket": odds_bkt, "team_bucket": team_bkt, "opp_bucket": opp_bkt, "game_count_bucket": gc_bkt},
                {"is_home": is_home, "odds_bucket": odds_bkt, "team_bucket": team_bkt, "opp_bucket": opp_bkt, "streak_bucket": streak_bkt},
                # Without odds bucket — captures strong team/opp patterns regardless of price
                {"is_home": is_home, "team_bucket": team_bkt, "opp_bucket": opp_bkt, "game_count_bucket": gc_bkt},
                {"is_home": is_home, "team_bucket": team_bkt, "opp_bucket": opp_bkt},
            ]
            if l10_bkt:
                base.append({"is_home": is_home, "odds_bucket": odds_bkt, "team_bucket": team_bkt, "opp_bucket": opp_bkt, "l10_bucket": l10_bkt})
            cleaned = [{k: v for k, v in f.items() if v is not None} for f in base]
            return [f for f in cleaned if len(f) >= 3]

        # Collect all patterns for home team
        home_filters = build_filters(True, home_odds_bucket, home_bucket, away_bucket, home_streak_bkt, home_l10_bkt, home_gc_bucket)
        home_patterns = []
        for f in home_filters:
            r = query_situation(home_hist, f)
            if r:
                r["filters"] = f
                r["source"] = "team"
                home_patterns.append(r)
            r2 = query_league_situation(hist_df, f, exclude_abbr=home_abbr)
            if r2:
                r2["filters"] = f
                r2["source"] = "league"
                home_patterns.append(r2)

        # Collect all patterns for away team
        away_filters = build_filters(False, away_odds_bucket, away_bucket, home_bucket, away_streak_bkt, away_l10_bkt, away_gc_bucket)
        away_patterns = []
        for f in away_filters:
            r = query_situation(away_hist, f)
            if r:
                r["filters"] = f
                r["source"] = "team"
                away_patterns.append(r)
            r2 = query_league_situation(hist_df, f, exclude_abbr=away_abbr)
            if r2:
                r2["filters"] = f
                r2["source"] = "league"
                away_patterns.append(r2)

        # Exclude heavy favorites from signals — juice not worth it
        if (home_odds_bucket and home_odds_bucket == "heavy_favorite") or (away_odds_bucket and away_odds_bucket == "heavy_favorite"):
            signals.append({
                "game_id": game_id,
                "home_team": home_abbr,
                "away_team": away_abbr,
                "tier": 0,
                "consensus_score": 0.0,
                "signal_team": None,
                "signal": None,
            })
            continue

        # Score both teams — positive = team wins, from home perspective
        home_score, home_best = score_patterns(home_patterns, home_implied)
        away_score, away_best = score_patterns(away_patterns, away_implied)

        # Away score: positive means away team wins = negative from home perspective
        consensus_score = round(home_score - away_score, 3)

        # Determine tier
        abs_consensus = abs(consensus_score)
        home_abs = abs(home_score)
        away_abs = abs(away_score)
        single_score = max(home_abs, away_abs)

        if abs_consensus >= 1.0:
            tier = 1
            signal_team = home_abbr if consensus_score > 0 else away_abbr
            best_pattern = home_best if consensus_score > 0 else away_best
        elif False:  # T2 disabled — insufficient hit rate (42.9% vs T1 70.3%)
            tier = 2
            if home_abs >= away_abs:
                signal_team = home_abbr if home_score > 0 else away_abbr
                best_pattern = home_best
            else:
                signal_team = away_abbr if away_score > 0 else home_abbr
                best_pattern = away_best
        else:
            tier = 0
            signal_team = None
            best_pattern = None

        signals.append({
            "game_id": game_id,
            "home_team": home_abbr,
            "away_team": away_abbr,
            "tier": tier,
            "consensus_score": consensus_score,
            "signal_team": signal_team,
            "signal": best_pattern,
        })

    signals.sort(key=lambda x: abs(x["consensus_score"]), reverse=True)

    result = {
        "date": game_date,
        "games_with_signals": sum(1 for s in signals if s["tier"] > 0),
        "tier1_signals": sum(1 for s in signals if s["tier"] == 1),
        "tier2_signals": sum(1 for s in signals if s["tier"] == 2),
        "total_games": len(signals),
        "signals": signals,
    }
    _signals_cache[game_date] = (result, time.monotonic())
    return result
