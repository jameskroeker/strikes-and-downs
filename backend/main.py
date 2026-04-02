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
