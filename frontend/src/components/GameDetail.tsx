import { useEffect, useState } from 'react'
import { useParams, useSearchParams, useNavigate } from 'react-router-dom'

const API_BASE = `${import.meta.env.VITE_API_URL}/api`

interface Situation {
  label: string
  wins: number
  losses: number
  n: number
  win_pct: number
  deviation: number
  implied_prob: number | null
  value_gap: number | null
}

interface TeamSituations {
  team_situations: Situation[]
  league_situations: Situation[]
}

interface SituationsResponse {
  game_id: string
  game_date: string
  home_team: string
  away_team: string
  situations: Record<string, TeamSituations>
}

function WinBar({ win_pct, implied_prob }: { win_pct: number, implied_prob: number | null }) {
  const pct = Math.round(win_pct * 100)
  const color = pct >= 60 ? '#4caf50' : pct <= 40 ? '#f44336' : '#888'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
      <div style={{ flex: 1, height: '8px', background: '#2a2f3e', borderRadius: '4px', overflow: 'hidden', position: 'relative' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: '4px' }} />
        {implied_prob && (
          <div style={{
            position: 'absolute', top: '-2px', left: `${Math.round(implied_prob * 100)}%`,
            width: '2px', height: '12px', background: '#666', borderRadius: '1px'
          }} />
        )}
      </div>
      <span style={{ color, fontWeight: 'bold', minWidth: '36px', fontSize: '13px' }}>{pct}%</span>
    </div>
  )
}

function GapBadge({ value_gap }: { value_gap: number }) {
  const pct = Math.round(value_gap * 100)
  const isPositive = pct > 0
  const isStrong = Math.abs(pct) >= 15
  const color = isPositive ? '#4caf50' : '#f44336'
  const icon = isStrong ? (isPositive ? '✅' : '⚠️') : (isPositive ? '↑' : '↓')
  return (
    <div style={{
      marginTop: '6px', fontSize: '11px', color,
      display: 'flex', alignItems: 'center', gap: '4px'
    }}>
      <span>{icon}</span>
      <span>
        Odds imply {Math.round((value_gap > 0 ? (1 - Math.abs(value_gap)) : (1 - Math.abs(value_gap))) * 100)}% — history says {Math.abs(pct)}% {isPositive ? 'better' : 'worse'} than implied
        <span style={{ fontWeight: 'bold', marginLeft: '4px' }}>({isPositive ? '+' : ''}{pct}%)</span>
      </span>
    </div>
  )
}

function SituationCard({ sit }: { sit: Situation }) {
  const hasGap = sit.value_gap !== null && Math.abs(sit.value_gap) >= 0.05
  return (
    <div style={{
      background: '#1a1f2e',
      borderRadius: '8px',
      padding: '14px 16px',
      border: `1px solid ${hasGap && Math.abs(sit.value_gap!) >= 0.15 ? (sit.value_gap! > 0 ? '#2e7d32' : '#b71c1c') : '#2a2f3e'}`,
    }}>
      <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '8px', lineHeight: 1.4 }}>
        {sit.label}
      </div>
      <WinBar win_pct={sit.win_pct} implied_prob={sit.implied_prob} />
      <div style={{ color: '#475569', fontSize: '11px', marginTop: '5px' }}>
        {sit.wins}-{sit.losses} &nbsp;·&nbsp; n={sit.n}
        {sit.implied_prob && (
          <span style={{ marginLeft: '8px', color: '#374151' }}>
            · implied {Math.round(sit.implied_prob * 100)}%
          </span>
        )}
      </div>
      {hasGap && <GapBadge value_gap={sit.value_gap!} />}
    </div>
  )
}

function TeamPanel({ abbr, data }: { abbr: string, data: TeamSituations }) {
  const { team_situations, league_situations } = data
  const hasTeam = team_situations.length > 0
  const hasLeague = league_situations.length > 0

  return (
    <div style={{ marginBottom: '40px' }}>
      <h3 style={{
        color: '#e2e8f0', fontSize: '16px', fontWeight: 'bold',
        marginBottom: '20px', letterSpacing: '0.05em'
      }}>
        {abbr}
      </h3>

      {hasTeam && (
        <>
          <div style={{ color: '#93c5fd', fontSize: '11px', fontWeight: 'bold', letterSpacing: '0.08em', marginBottom: '10px' }}>
            TEAM HISTORY · 2022–2025
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', marginBottom: '24px' }}>
            {team_situations.map((sit, i) => <SituationCard key={i} sit={sit} />)}
          </div>
        </>
      )}

      {hasLeague && (
        <>
          <div style={{ color: '#86efac', fontSize: '11px', fontWeight: 'bold', letterSpacing: '0.08em', marginBottom: '10px' }}>
            LEAGUE CONTEXT · 2022–2025 · ALL TEAMS
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            {league_situations.map((sit, i) => <SituationCard key={i} sit={sit} />)}
          </div>
        </>
      )}

      {!hasTeam && !hasLeague && (
        <p style={{ color: '#475569', fontSize: '13px' }}>No significant patterns found for this situation.</p>
      )}
    </div>
  )
}

export function GameDetail() {
  const { gameId } = useParams<{ gameId: string }>()
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const gameDate = searchParams.get('date') || ''

  const [data, setData] = useState<SituationsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!gameId || !gameDate) return
    setLoading(true)
    fetch(`${API_BASE}/games/${gameId}/situations?game_date=${gameDate}`)
      .then(res => {
        if (!res.ok) throw new Error('Failed to load situations')
        return res.json()
      })
      .then(d => { setData(d); setLoading(false) })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [gameId, gameDate])

  return (
    <div className="app">
      <header className="header">
        <h1><span className="logo-icon">⚾</span> Strikes &amp; Downs</h1>
        <p className="subtitle">MLB Betting Analytics | 2026 Season</p>
      </header>

      <div style={{ padding: '0 16px 16px' }}>
        <button onClick={() => navigate(-1)} style={{
          background: 'none', border: '1px solid #2a2f3e', color: '#64748b',
          padding: '6px 14px', borderRadius: '6px', cursor: 'pointer', fontSize: '13px'
        }}>
          ← Back
        </button>
      </div>

      {loading && <div className="loading">Loading situations…</div>}
      {error && <div className="error">{error}</div>}

      {data && (
        <main style={{ padding: '0 16px 32px', maxWidth: '760px', margin: '0 auto' }}>
          <div style={{ textAlign: 'center', marginBottom: '32px' }}>
            <div style={{ color: '#e2e8f0', fontSize: '20px', fontWeight: 'bold' }}>
              {data.away_team} <span style={{ color: '#475569' }}>@</span> {data.home_team}
            </div>
            <div style={{ color: '#475569', fontSize: '13px', marginTop: '4px' }}>{data.game_date}</div>
          </div>

          {[data.away_team, data.home_team].map(abbr => {
            const teamData = data.situations[abbr]
            if (!teamData) return null
            return <TeamPanel key={abbr} abbr={abbr} data={teamData} />
          })}

          <div style={{ color: '#374151', fontSize: '11px', textAlign: 'center', borderTop: '1px solid #1a1f2e', paddingTop: '16px' }}>
            Historical patterns from 2022–2025 regular season · Sample size (n) shown for context · Not a prediction
          </div>
        </main>
      )}
    </div>
  )
}
