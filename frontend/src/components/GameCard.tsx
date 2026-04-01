import type { Game } from '../types'
import './GameCard.css'

interface Props {
  game: Game
}

function formatMoneyline(odds: number | null): string {
  if (odds === null) return '–'
  return odds > 0 ? `+${odds}` : `${odds}`
}

const STATUS_CLASS: Record<string, string> = {
  scheduled: 'status-scheduled',
  final: 'status-final',
  'in progress': 'status-live',
  live: 'status-live',
}

export function GameCard({ game }: Props) {
  const { home_team, away_team, odds, tags, status, start_time_et, home_score, away_score } = game
  const hasScore = home_score !== null && away_score !== null
  const statusClass = STATUS_CLASS[status.toLowerCase()] ?? 'status-scheduled'

  return (
    <div className="game-card">
      <div className="game-header">
        <span className="game-time">{start_time_et}</span>
        <span className={`game-status ${statusClass}`}>{status}</span>
      </div>

      <div className="matchup">
        <div className="team away">
          <div className="team-abbr">{away_team.abbr}</div>
          <div className="team-name">{away_team.name}</div>
          <div className="team-record">
            {away_team.wins}-{away_team.losses}
            {away_team.win_pct > 0 && (
              <span className="win-pct"> ({away_team.win_pct.toFixed(3)})</span>
            )}
          </div>
          {away_team.streak && <div className="team-streak">{away_team.streak}</div>}
        </div>

        <div className="score-divider">
          {hasScore ? (
            <div className="score">
              {away_score} <span className="score-sep">–</span> {home_score}
            </div>
          ) : (
            <div className="at">@</div>
          )}
        </div>

        <div className="team home">
          <div className="team-abbr">{home_team.abbr}</div>
          <div className="team-name">{home_team.name}</div>
          <div className="team-record">
            {home_team.wins}-{home_team.losses}
            {home_team.win_pct > 0 && (
              <span className="win-pct"> ({home_team.win_pct.toFixed(3)})</span>
            )}
          </div>
          {home_team.streak && <div className="team-streak">{home_team.streak}</div>}
        </div>
      </div>

      <div className="odds-row">
        <div className="odds-block">
          <span className="odds-label">ML</span>
          <span className="odds-value away-odds">{formatMoneyline(odds.moneyline_away)}</span>
          <span className="odds-sep">/</span>
          <span className="odds-value home-odds">{formatMoneyline(odds.moneyline_home)}</span>
        </div>
        {odds.total_line !== null && (
          <div className="odds-block">
            <span className="odds-label">O/U</span>
            <span className="odds-value">{odds.total_line}</span>
            {odds.over_odds !== null && (
              <span className="odds-sub">
                o{formatMoneyline(odds.over_odds)} / u{formatMoneyline(odds.under_odds)}
              </span>
            )}
          </div>
        )}
      </div>

      {tags.length > 0 && (
        <div className="tags">
          {tags.map((tag) => (
            <span key={tag} className="tag">
              {tag}
            </span>
          ))}
        </div>
      )}
    </div>
  )
}
