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
    _master_df = pd.read_parquet(io.BytesIO(response.content))
    _master_df_loaded_at = time.monotonic()
    return _master_df


async def fetch_daily_csv(game_date: str) -> pd.DataFrame:
    url = DAILY_CSV_URL.format(date=game_date)
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url)
        response.raise_for_status()
    return pd.read_csv(io.StringIO(response.text))


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

    return {
        "wins": int(latest.get("Wins") or 0),
        "losses": int(latest.get("Losses") or 0),
        "win_pct": round(float(latest.get("Win_Pct") or 0.0), 3),
        "streak": streak,
    }


def compute_tags(
    home_stats: dict,
    away_stats: dict,
    ml_home: Optional[float],
    ml_away: Optional[float],
) -> list[str]:
    tags: list[str] = []

    if ml_home is not None and ml_away is not None:
        if ml_home < ml_away:
            tags.append("Home Favorite")
            tags.append("Away Underdog")
        else:
            tags.append("Away Favorite")
            tags.append("Home Underdog")

    for label, stats in [("Home", home_stats), ("Away", away_stats)]:
        streak = stats.get("streak", "")
        if not streak or len(streak) < 2:
            continue
        direction, n_str = streak[0], streak[1:]
        n = int(n_str) if n_str.isdigit() else 0
        if direction == "W" and n >= 3:
            tags.append(f"{label} Hot ({streak})")
        elif direction == "L" and n >= 3:
            tags.append(f"{label} Cold ({streak})")

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

def odds_bucket(decimal_odds: float) -> str:
    """Bucket a team's own moneyline odds based on actual data distribution."""
    if decimal_odds < 1.50:  return "heavy_favorite"   # -400 to -200
    if decimal_odds < 1.75:  return "favorite"          # -200 to -133
    if decimal_odds < 1.95:  return "slight_favorite"   # -133 to -105
    if decimal_odds < 2.10:  return "pick"              # -105 to +110
    if decimal_odds < 2.50:  return "slight_underdog"   # +110 to +150
    if decimal_odds < 3.25:  return "underdog"          # +150 to +225
    return "big_underdog"                               # +225 and up

ODDS_BUCKET_LABELS = {
    "heavy_favorite":  "Heavy Favorite (-400 to -200)",
    "favorite":        "Favorite (-200 to -133)",
    "slight_favorite": "Slight Favorite (-133 to -105)",
    "pick":            "Pick (-105 to +110)",
    "slight_underdog": "Slight Underdog (+110 to +150)",
    "underdog":        "Underdog (+150 to +225)",
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
    return " | ".join(parts)

def query_situation(hist_df: pd.DataFrame, filters: dict, min_n: int = 15) -> Optional[dict]:
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
    }


def query_league_situation(
    hist_df: pd.DataFrame,
    filters: dict,
    exclude_abbr: str,
) -> Optional[dict]:
    """League-wide situation query excluding the specific team."""
    df = hist_df[hist_df["team_abbr"] != exclude_abbr].copy()
    df["_team_bucket"] = df["Win_Pct"].apply(win_pct_bucket)
    df["_odds_bucket"] = df["h2h_own_odds"].apply(
        lambda x: odds_bucket(x) if pd.notna(x) else None
    )
    opp_win_pcts = hist_df.groupby(["game_id", "team_abbr"])["Win_Pct"].first().reset_index()
    opp_win_pcts.columns = ["game_id", "opponent_abbr", "_opp_win_pct"]
    df = df.merge(opp_win_pcts, on=["game_id", "opponent_abbr"], how="left")
    df["_opp_bucket"] = df["_opp_win_pct"].apply(
        lambda x: win_pct_bucket(float(x)) if pd.notna(x) else None
    )

    if "is_home" in filters:
        df = df[df["is_home"] == filters["is_home"]]
    if "odds_bucket" in filters:
        df = df[df["_odds_bucket"] == filters["odds_bucket"]]
    if "team_bucket" in filters:
        df = df[df["_team_bucket"] == filters["team_bucket"]]
    if "opp_bucket" in filters:
        df = df[df["_opp_bucket"] == filters["opp_bucket"]]

    df = df[df["team_won"].notna()]
    n = len(df)
    if n < 30:
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
        team_hist["_team_bucket"] = team_hist["Win_Pct"].apply(win_pct_bucket)
        team_hist["_odds_bucket"] = team_hist["h2h_own_odds"].apply(
            lambda x: odds_bucket(x) if pd.notna(x) else None
        )
        team_hist["_total_bucket"] = team_hist["Total"].apply(
            lambda x: total_bucket(x) if pd.notna(x) else None
        )

        # Opponent bucket — join opponent win_pct at time of game
        opp_win_pcts = hist_df.groupby(["game_id", "team_abbr"])["Win_Pct"].first().reset_index()
        opp_win_pcts.columns = ["game_id", "opponent_abbr", "_opp_win_pct"]
        team_hist = team_hist.merge(opp_win_pcts, on=["game_id", "opponent_abbr"], how="left")
        team_hist["_opp_bucket"] = team_hist["_opp_win_pct"].apply(
            lambda x: win_pct_bucket(float(x)) if pd.notna(x) else None
        )

        # Last 10 bucket — rolling wins in last 10 for each game
        team_hist = team_hist.sort_values("game_date_et")
        team_hist["_l10_wins"] = team_hist["team_won"].rolling(10, min_periods=5).sum().shift(1)
        team_hist["_l10_bucket"] = team_hist["_l10_wins"].apply(
            lambda x: last_10_bucket(int(x)) if pd.notna(x) else None
        )

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
            # L10 form (only once 10+ games played)
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "l10_bucket": current_l10_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "l10_bucket": current_l10_bucket},
        ]

        # Remove any filters with None values
        situation_filters = [
            {k: v for k, v in f.items() if v is not None}
            for f in situation_filters
        ]

        # Team-specific situations
        team_situations = []
        seen_labels = set()
        for filters in situation_filters:
            result = query_situation(team_hist, filters)
            if result and result["label"] not in seen_labels:
                seen_labels.add(result["label"])
                team_situations.append(result)
        team_situations.sort(key=lambda x: x["deviation"], reverse=True)

        # League context situations — fixed filter set, exclude this team
        league_filters = [
            {"is_home": is_home, "odds_bucket": own_odds_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "opp_bucket": opp_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket},
            {"is_home": is_home, "odds_bucket": own_odds_bucket, "team_bucket": team_bucket, "opp_bucket": opp_bucket},
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
                league_situations.append(result)
        league_situations.sort(key=lambda x: x["deviation"], reverse=True)

        results[abbr] = {
            "team_situations": team_situations[:5],
            "league_situations": league_situations[:4],
        }

    return {
        "game_id": game_id,
        "game_date": game_date,
        "home_team": home_abbr,
        "away_team": away_abbr,
        "situations": results,
    }
