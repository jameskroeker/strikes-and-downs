# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**Strikes and Downs** — MLB betting analytics web app. Displays today's games with team context (records, streaks) and betting situation tags sourced from a Pinnacle odds pipeline.

## Commands

### Backend (FastAPI)
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### Frontend (React + Vite)
```bash
cd frontend
npm install
npm run dev        # dev server on :5173, proxies /api → :8000
npm run build      # type-check + production build
```

## Architecture

```
backend/main.py          FastAPI app — all routes and data logic in one file
frontend/src/
  types.ts               Shared TypeScript interfaces (Game, TeamStats, GameOdds)
  api.ts                 fetch wrappers for /api/games/today and /api/games/{date}
  App.tsx                Root component: date picker, fetches and renders game list
  components/GameCard    Single game card (matchup, odds, streak badges, tags)
```

### Data flow
1. **Daily CSV** (`MLB_Combined_Odds_Results_{YYYY-MM-DD}.csv`) — today's games: teams, moneylines, totals, scores, status. Odds are in American format.
2. **Master parquet** (`master_template.parquet`) — historical game-by-game records. Key columns: `team_abbr`, `game_date_et`, `Wins`, `Losses`, `Win_Pct`, `Win_Streak`, `Loss_Streak`, `h2h_own_odds`, `h2h_opp_odds`, `Total`, `Over_Price_odds`, `Under_Price_odds`. Parquet odds are Pinnacle decimal format.
3. **Join** — `TEAM_NAME_TO_ABBR` dict maps CSV full names → parquet `team_abbr`. For each game, the backend takes the most recent parquet row per team to get current-season stats.
4. **Tags** — computed server-side in `compute_tags()`: favorite/underdog from moneyline, hot/cold streak (≥3 games).

### Data source URLs
- Parquet: `https://raw.githubusercontent.com/jameskroeker/mlb-betting-data-pipeline/main/data/master/master_template.parquet`
- Daily CSV: `https://raw.githubusercontent.com/jameskroeker/mlb-betting-data-pipeline/main/data/daily/MLB_Combined_Odds_Results_{YYYY-MM-DD}.csv`

### Backend caching
The parquet file is fetched once and cached in `_master_df` (module-level). Restart the server to refresh it. Daily CSVs are fetched on each request.

### Team name mapping
If a team name from the CSV doesn't match `TEAM_NAME_TO_ABBR` in `main.py`, the backend falls back to the first 3 characters uppercased — this will produce wrong parquet lookups. Add new mappings as needed (e.g. if team names change).

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/games/today` | Today's games (redirects to date endpoint) |
| GET | `/api/games/{YYYY-MM-DD}` | Games for a specific date |
| GET | `/health` | Health check |

Response shape: `{ date, games: Game[] }` where each `Game` includes `home_team`, `away_team` (with stats), `odds`, `tags`, scores.
