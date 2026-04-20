import type { Game } from '../types'
import './GameCard.css'
import { TEAM_COLORS } from '../teamColors'

interface Signal {
  label: string
  win_pct: number
  n: number
  deviation: number
  team: string
  source: string
  tier?: number
  signal_team?: string
}

interface Props {
  game: Game
  signal?: Signal
}

const INVALID_VALUES = new Set(['nan', 'tbd', 'none', 'nat', ''])

function cleanTime(val: string): string {
  return INVALID_VALUES.has(val.toLowerCase()) ? '' : val
}

function cleanStatus(val: string): string {
  return INVALID_VALUES.has(val.toLowerCase()) ? 'Scheduled' : val
}

function tagClass(tag: string): string {
  const t = tag.toLowerCase()
  if (t.includes('fade spot'))        return 'tag tag-fade'
  if (t.includes('bounce back'))      return 'tag tag-bounce'
  if (t.includes('bad team favored')) return 'tag tag-warning'
  if (t.includes('hot underdog'))     return 'tag tag-hot-dog'
  if (t.includes('favorite'))         return 'tag tag-favorite'
  if (t.includes('hot'))              return 'tag tag-hot'
  if (t.includes('cold'))             return 'tag tag-cold'
  if (t.includes('underdog'))         return 'tag tag-underdog'
  return 'tag'
}

function formatMoneyline(odds: number | null): string {
  if (odds === null) return '–'
  if (odds >= 2.0) {
    return `+${Math.round((odds - 1) * 100)}`
  } else {
    return `${Math.round(-(100 / (odds - 1)))}`
  }
}

const STATUS_CLASS: Record<string, string> = {
  scheduled: 'status-scheduled',
  final: 'status-final',
  'in progress': 'status-live',
  live: 'status-live',
}

export function GameCard({ game, signal }: Props) {
  const { home_team, away_team, odds, tags, status, start_time_et, home_score, away_score } = game
  const hasScore = home_score !== null && away_score !== null
  const displayStatus = cleanStatus(status)
  const statusClass = STATUS_CLASS[displayStatus.toLowerCase()] ?? 'status-scheduled'
  const gameTime = cleanTime(start_time_et)
  const cardStatusClass = displayStatus.toLowerCase() === 'final' ? 'card-final' : 'card-scheduled'

  // Determine favorite/underdog role per team
  const homeFav = odds.moneyline_home !== null && odds.moneyline_away !== null && odds.moneyline_home < odds.moneyline_away
  const awayFav = odds.moneyline_home !== null && odds.moneyline_away !== null && odds.moneyline_away < odds.moneyline_home

  // Situational tags only — max 1, highest priority
  const SITUATIONAL_PRIORITY = ['fade spot', 'bad team favored', 'bounce back', 'hot underdog', 'hot', 'cold']
  const situationalTags = tags.filter(t => {
    const tl = t.toLowerCase()
    return !tl.includes('favorite') && !tl.includes('underdog')
  })
  const topSituational = SITUATIONAL_PRIORITY
    .map(p => situationalTags.find(t => t.toLowerCase().includes(p)))
    .find(t => t !== undefined)

  return (
    <div className={`game-card ${cardStatusClass}`}>
      <div className="game-header">
        {gameTime && <span className="game-time">{gameTime}</span>}
        <span className={`game-status ${statusClass}`}>{displayStatus}</span>
      </div>

      <div className="matchup">
        <div className="team away">
          <div className="team-abbr-row">
            <div className="team-abbr" style={{ color: TEAM_COLORS[away_team.abbr] ?? '#e2e8f0' }}>{away_team.abbr}</div>
            {signal && signal.signal_team === away_team.abbr && <span className="signal-bolt" title={signal.label}>{signal.tier === 1 ? "⚡⚡" : "⚡"}</span>}
          </div>
          <div className="team-name">{away_team.name}</div>
          <div className="team-record">{away_team.wins}-{away_team.losses}</div>
          <div className="team-meta">
            {away_team.streak && <span className="team-streak">{away_team.streak}</span>}
            {awayFav && <span className="role-badge role-fav">FAV</span>}
            {!awayFav && homeFav && <span className="role-badge role-dog">DOG</span>}
          </div>
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
          <div className="team-abbr-row">
            {signal && signal.signal_team === home_team.abbr && <span className="signal-bolt" title={signal.label}>{signal.tier === 1 ? "⚡⚡" : "⚡"}</span>}
            <div className="team-abbr" style={{ color: TEAM_COLORS[home_team.abbr] ?? '#e2e8f0' }}>{home_team.abbr}</div>
          </div>
          <div className="team-name">{home_team.name}</div>
          <div className="team-record">{home_team.wins}-{home_team.losses}</div>
          <div className="team-meta">
            {home_team.streak && <span className="team-streak">{home_team.streak}</span>}
            {homeFav && <span className="role-badge role-fav">FAV</span>}
            {!homeFav && awayFav && <span className="role-badge role-dog">DOG</span>}
          </div>
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

      {topSituational && (
        <div className="tags">
          <span className={`tag ${tagClass(topSituational)}`} style={{ whiteSpace: 'nowrap' }}>
            {topSituational}
          </span>
        </div>
      )}

    </div>
  )
}
