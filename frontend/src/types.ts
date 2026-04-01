export interface TeamStats {
  abbr: string;
  name: string;
  wins: number;
  losses: number;
  win_pct: number;
  streak: string;
}

export interface GameOdds {
  moneyline_home: number | null;
  moneyline_away: number | null;
  total_line: number | null;
  over_odds: number | null;
  under_odds: number | null;
}

export interface Game {
  game_id: string;
  game_date: string;
  start_time_et: string;
  status: string;
  home_team: TeamStats;
  away_team: TeamStats;
  odds: GameOdds;
  tags: string[];
  home_score: number | null;
  away_score: number | null;
}

export interface GamesResponse {
  date: string;
  games: Game[];
}
