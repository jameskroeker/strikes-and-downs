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

function WinBar({ win_pct }: { win_pct: number }) {
  const pct = Math.round(win_pct * 100)
  const color = pct >= 60 ? '#4caf50' : pct <= 40 ? '#f44336' : '#888'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
      <div style={{
        flex: 1, height: '8px', background: '#2a2f3e', borderRadius: '4px', overflow: 'hidden'
      }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: '4px' }} />
      </div>
      <span style={{ color, fontWeight: 'bold', minWidth: '36px', fontSize: '13px' }}>{pct}%</span>
    </div>
  )
}

function SituationCard({ sit }: { sit: Situation }) {
  return (
    <div style={{
      background: '#1a1f2e', borderRadius: '8px',
      padding: '14px 16px', border: '1px solid #2a2f3e'
    }}>
      <div style={{ color: '#e2e8f0', fontSize: '13px', marginBottom: '8px', lineHeight: 1.4 }}>
        {sit.label}
      </div>
      <WinBar win_pct={sit.win_pct} />
      <div style={{ color: '#666', fontSize: '12px', marginTop: '6px' }}>
        {sit.wins}-{sit.losses} &nbsp;·&nbsp; n={sit.n}
      </div>
    </div>
  )
}

function TeamPanel({ abbr, data }: { abbr: string, data: TeamSituations }) {
  const { team_situations, league_situations } = data
  return (
    <div style={{ marginBottom: '40px' }}>
      {/* Team History */}
      <h3 style={{
        color: '#93c5fd', fontSize: '15px', fontWeight: 'bold',
        borderBottom: '1px solid #2a2f3e', paddingBottom: '8px', marginBottom: '16px'
      }}>
        {abbr} — Team History (2022–2025)
      </h3>
      {team_situations.length === 0 ? (
        <p style={{ color: '#666', fontSize: '13px', marginBottom: '24px' }}>
          Not enough historical data for this situation.
        </p>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', marginBottom: '28px' }}>
          {team_situations.map((sit, i) => <SituationCard key={i} sit={sit} />)}
        </div>
      )}

      {/* League Context */}
      <h3 style={{
        color: '#86efac', fontSize: '15px', fontWeight: 'bold',
        borderBottom: '1px solid #2a2f3e', paddingBottom: '8px', marginBottom: '16px'
      }}>
        {abbr} — League Context (2022–2025, all teams)
      </h3>
      {league_situations.length === 0 ? (
        <p style={{ color: '#666', fontSize: '13px' }}>
          No significant league patterns for this situation.
        </p>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
          {league_situations.map((sit, i) => <SituationCard key={i} sit={sit} />)}
        </div>
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
        <button
          onClick={() => navigate(-1)}
          style={{
            background: 'none', border: '1px solid #444', color: '#aaa',
            padding: '6px 14px', borderRadius: '6px', cursor: 'pointer', fontSize: '13px'
          }}
        >
          ← Back
        </button>
      </div>

      {loading && <div className="loading">Loading situations…</div>}
      {error && <div className="error">{error}</div>}

      {data && (
        <main style={{ padding: '0 16px', maxWidth: '800px', margin: '0 auto' }}>
          <h2 style={{ color: '#e2e8f0', marginBottom: '28px', textAlign: 'center' }}>
            {data.away_team} @ {data.home_team}
            <span style={{ color: '#666', fontSize: '14px', marginLeft: '12px' }}>{data.game_date}</span>
          </h2>

          {[data.away_team, data.home_team].map(abbr => {
            const teamData = data.situations[abbr]
            if (!teamData) return null
            return <TeamPanel key={abbr} abbr={abbr} data={teamData} />
          })}

          <div style={{ color: '#444', fontSize: '11px', textAlign: 'center', paddingBottom: '32px' }}>
            Patterns from 2022–2025 regular season. Sample size (n) shown for context.
            This is historical data, not a prediction.
          </div>
        </main>
      )}
    </div>
  )
}
