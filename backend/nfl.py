"""NFL query builder API.

Self-contained router: loads the NFL master parquet from the public pipeline repo,
precomputes filter buckets once per cache window, and serves /api/nfl/* endpoints.

All situational filters use pre-game values only (entering_* / prev_* columns), so a
game's own result never leaks into the bucket it is filtered by.
"""
import io
import time
from typing import Optional

import httpx
import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/nfl", tags=["nfl"])

NFL_PARQUET_URL = (
    "https://raw.githubusercontent.com/jameskroeker/nfl-betting-data-pipeline/main/data/nfl_master_2022_2025.parquet"
)
NFL_CACHE_TTL = 30 * 60  # 30 minutes
HIST_SEASONS = [2022, 2023, 2024, 2025]
SAMPLE_WARNING_N = 15
MAX_GAMES_RETURNED = 50

_nfl_df: Optional[pd.DataFrame] = None
_nfl_loaded_at: float = 0.0


# ---------------------------------------------------------------- buckets

def _spread_band(s: pd.Series) -> pd.Series:
    a = s.abs()
    mag = np.select([a <= 2.5, a <= 3.5, a <= 6.5, a <= 9.5], ["0.5-2.5", "3-3.5", "4-6.5", "7-9.5"], "10+")
    side = np.where(s < 0, "fav_", "dog_")
    out = pd.Series(np.char.add(side.astype(str), mag.astype(str)), index=s.index)
    return out.where(s.notna() & (s != 0))


def _pct_band(p: pd.Series, games: pd.Series) -> pd.Series:
    band = pd.Series(np.select([p < 0.4, p < 0.6], ["lt400", "400-599"], "600plus"), index=p.index)
    return band.where(games >= 3)  # bands only once a team has 3+ decided games


def _tri(s: pd.Series, yes: str, no: str) -> pd.Series:
    """Map True/False/None object column to labels, keeping None as missing."""
    return s.map({True: yes, False: no})


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["_week_num"] = df["week"].str.extract(r"^W(\d+)$")[0].astype(float)
    df["_phase"] = np.select(
        [df["_week_num"] <= 6, df["_week_num"] <= 12, df["_week_num"] <= 18], ["w1-6", "w7-12", "w13-18"], "playoffs"
    )
    df["_completed"] = df["team_score"].notna() & df["opponent_score"].notna()

    df["_spread_band"] = _spread_band(df["team_spread"])
    df["_total_band"] = pd.cut(
        df["game_total"], [0, 39.9, 44.9, 49.9, 99], labels=["lt40", "40-44.5", "45-49.5", "50plus"]
    ).astype(object)

    df["_kickoff"] = np.where(
        df["kickoff_slot"].isin(["TNF", "SNF", "MNF"]),
        df["kickoff_slot"],
        np.where(df["is_primetime"] == True, "other_night", "day"),
    )
    df["_rest"] = np.select(
        [df["is_off_bye"] == True, df["rest_status"] == "short_rest", df["rest_status"] == "extended_rest",
         df["rest_status"] == "normal_rest"],
        ["off_bye", "short", "extended", "normal"], "opener",
    )
    df["_road"] = np.select([df["road_trip_game"] == 0, df["road_trip_game"] == 1], ["home", "road1"], "road2plus")
    df["_home_after_road"] = np.select(
        [df["home_after_road_trip"] == 0, df["home_after_road_trip"] == 1], ["0", "1"], "2plus"
    )

    su_games = df["entering_season_wins"] + df["entering_season_losses"] + df["entering_season_ties"]
    ats_games = df["entering_ats_wins"] + df["entering_ats_losses"]
    df["_wins"] = df["entering_season_wins"].astype(int)
    df["_ats_wins"] = df["entering_ats_wins"].astype(int)
    df["_win_pct"] = _pct_band(df["entering_win_pct"], su_games)
    df["_ats_pct"] = _pct_band(df["entering_ats_win_pct"], ats_games)

    df["_prev_result"] = df["prev_result"].where(df["prev_result"] != "tie")
    df["_prev_ats"] = _tri(df["prev_spread_covered"], "covered", "missed")
    df["_prev_upset"] = df["prev_upset"]
    df["_prev_ot"] = _tri(df["prev_game_was_overtime"], "true", "false")

    # Opponent versions of the situational buckets
    opp_cols = ["_rest", "_wins", "_ats_wins", "_win_pct", "_ats_pct", "_prev_result", "_prev_ats", "_prev_upset", "_road"]
    opp = df[["game_id", "team"] + opp_cols].rename(columns={"team": "opponent", **{c: f"_opp{c}" for c in opp_cols}})
    df = df.merge(opp, on=["game_id", "opponent"], how="left")
    return df


async def fetch_nfl_df() -> pd.DataFrame:
    global _nfl_df, _nfl_loaded_at
    if _nfl_df is not None and (time.monotonic() - _nfl_loaded_at) < NFL_CACHE_TTL:
        return _nfl_df
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(NFL_PARQUET_URL)
        resp.raise_for_status()
    _nfl_df = _prepare(pd.read_parquet(io.BytesIO(resp.content)))
    _nfl_loaded_at = time.monotonic()
    return _nfl_df


# ---------------------------------------------------------------- query

# query param -> bucket column (exact match)
BUCKET_FILTERS = {
    "spread_band": "_spread_band", "total_band": "_total_band", "phase": "_phase",
    "rest": "_rest", "road": "_road", "home_after_road": "_home_after_road",
    "win_pct": "_win_pct", "ats_pct": "_ats_pct",
    "prev_result": "_prev_result", "prev_ats": "_prev_ats", "prev_upset": "_prev_upset", "prev_ot": "_prev_ot",
    "opp_rest": "_opp_rest", "opp_road": "_opp_road",
    "opp_win_pct": "_opp_win_pct", "opp_ats_pct": "_opp_ats_pct", "opp_prev_result": "_opp_prev_result",
    "opp_prev_ats": "_opp_prev_ats", "opp_prev_upset": "_opp_prev_upset",
}


def _season_filter(df: pd.DataFrame, seasons: str) -> pd.DataFrame:
    if seasons == "all":
        return df
    if seasons == "hist":
        return df[df["season"].isin(HIST_SEASONS)]
    try:
        return df[df["season"] == int(seasons)]
    except ValueError:
        raise HTTPException(400, f"Invalid seasons value: {seasons}")


def _rate(a: int, b: int) -> Optional[float]:
    return round(a / (a + b), 3) if (a + b) else None


def _summarize(df: pd.DataFrame) -> dict:
    won = int((df["team_won"] == True).sum())
    lost = int((df["team_won"] == False).sum())
    tied = int(len(df) - won - lost)

    lined = df[df["team_spread"].notna()]
    cov = int((lined["spread_covered"] == True).sum())
    miss = int((lined["spread_covered"] == False).sum())
    ats_push = int(len(lined) - cov - miss)

    totaled = df[df["game_total"].notna()]
    over = int((totaled["total_hit_over"] == True).sum())
    under = int((totaled["total_hit_over"] == False).sum())
    ou_push = int(len(totaled) - over - under)

    def avg(s):
        return round(float(s.mean()), 2) if len(s.dropna()) else None

    return {
        "n": int(len(df)),
        "su": {"wins": won, "losses": lost, "ties": tied, "win_pct": _rate(won, lost)},
        "ats": {"covers": cov, "misses": miss, "pushes": ats_push, "cover_pct": _rate(cov, miss)},
        "ou": {"overs": over, "unders": under, "pushes": ou_push, "over_pct": _rate(over, under)},
        "avg_margin": avg(df["point_margin"]),
        "avg_spread": avg(lined["team_spread"]),
        "avg_cover_margin": avg(lined["spread_margin"]),
        "avg_total_points": avg(df["team_score"] + df["opponent_score"]),
        "avg_total_line": avg(totaled["game_total"]),
    }


def _rec_str(w, l, t) -> str:
    base = f"{int(w)}-{int(l)}"
    return f"{base}-{int(t)}" if t else base


def _game_row(r: pd.Series) -> dict:
    ats = {True: "cover", False: "miss"}.get(r["spread_covered"], "push" if pd.notna(r["team_spread"]) else None)
    ou = {True: "over", False: "under"}.get(r["total_hit_over"], "push" if pd.notna(r["game_total"]) else None)
    return {
        "game_date": r["game_date"].strftime("%Y-%m-%d"),
        "season": int(r["season"]),
        "week": r["week"],
        "team": r["team"],
        "opponent": r["opponent"],
        "home_away": "Neutral" if r["is_neutral_site"] else r["home_away"],
        "spread": None if pd.isna(r["team_spread"]) else float(r["team_spread"]),
        "total": None if pd.isna(r["game_total"]) else float(r["game_total"]),
        "team_score": int(r["team_score"]),
        "opponent_score": int(r["opponent_score"]),
        "ats": ats,
        "ou": ou,
        "entering_record": _rec_str(r["entering_season_wins"], r["entering_season_losses"], r["entering_season_ties"]),
        "entering_ats": _rec_str(r["entering_ats_wins"], r["entering_ats_losses"], r["entering_ats_pushes"]),
    }


@router.get("/query")
async def nfl_query(
    seasons: str = "hist",
    team: Optional[str] = None,
    opponent: Optional[str] = None,
    home_away: Optional[str] = None,          # home | away
    exclude_neutral: Optional[str] = None,    # true
    divisional: Optional[str] = None,         # true | false
    kickoff: Optional[str] = None,            # primetime | TNF | SNF | MNF | day
    week: Optional[int] = None,
    side: Optional[str] = None,               # fav | dog (any spread size)
    spread_band: Optional[str] = None, total_band: Optional[str] = None, phase: Optional[str] = None,
    rest: Optional[str] = None, road: Optional[str] = None, home_after_road: Optional[str] = None,
    wins: Optional[int] = None, ats_wins: Optional[int] = None, win_pct: Optional[str] = None, ats_pct: Optional[str] = None,
    prev_result: Optional[str] = None, prev_ats: Optional[str] = None, prev_upset: Optional[str] = None,
    prev_ot: Optional[str] = None,
    opp_rest: Optional[str] = None, opp_road: Optional[str] = None,
    opp_wins: Optional[int] = None, opp_ats_wins: Optional[int] = None, opp_win_pct: Optional[str] = None, opp_ats_pct: Optional[str] = None,
    opp_prev_result: Optional[str] = None, opp_prev_ats: Optional[str] = None, opp_prev_upset: Optional[str] = None,
):
    df = await fetch_nfl_df()
    df = _season_filter(df, seasons)
    df = df[df["_completed"]]

    if team:
        df = df[df["team"] == team]
    if opponent:
        df = df[df["opponent"] == opponent]
    if home_away in ("home", "away"):
        df = df[df["home_away"].str.lower() == home_away]
    if exclude_neutral == "true":
        df = df[df["is_neutral_site"] == False]
    if divisional in ("true", "false"):
        df = df[df["is_divisional_game"] == (divisional == "true")]
    if kickoff:
        df = df[df["is_primetime"] == True] if kickoff == "primetime" else df[df["_kickoff"] == kickoff]
    if week is not None:
        df = df[df["_week_num"] == week]
    if side == "fav":
        df = df[df["team_spread"] < 0]
    elif side == "dog":
        df = df[df["team_spread"] > 0]

    for param, col in (("wins", "_wins"), ("ats_wins", "_ats_wins"),
                       ("opp_wins", "_opp_wins"), ("opp_ats_wins", "_opp_ats_wins")):
        val = locals()[param]
        if val is not None:
            df = df[df[col] == val]

    params = locals()
    for param, col in BUCKET_FILTERS.items():
        val = params.get(param)
        if val:
            df = df[df[col] == val]

    summary = _summarize(df)
    ats_n = summary["ats"]["covers"] + summary["ats"]["misses"]

    by_season = []
    for season, g in df.groupby("season"):
        s = _summarize(g)
        by_season.append({"season": int(season), "n": s["n"], "ats": s["ats"], "su": s["su"], "ou": s["ou"]})

    recent = df.sort_values("game_date", ascending=False).head(MAX_GAMES_RETURNED)
    return {
        **summary,
        "sample_warning": ats_n < SAMPLE_WARNING_N,
        "by_season": by_season,
        "games": [_game_row(r) for _, r in recent.iterrows()],
        "games_truncated": len(df) > MAX_GAMES_RETURNED,
    }


@router.get("/meta")
async def nfl_meta():
    df = await fetch_nfl_df()
    done = df[df["_completed"]]
    latest = done.sort_values("game_date").iloc[-1]
    return {
        "teams": sorted(df["team"].unique().tolist()),
        "seasons": sorted(int(s) for s in df["season"].unique()),
        "data_through": {"season": int(latest["season"]), "week": latest["week"],
                         "date": latest["game_date"].strftime("%Y-%m-%d")},
        "games": int(done["game_id"].nunique()),
    }
