import { useEffect, useState } from 'react'
import { useParams, useSearchParams, useNavigate } from 'react-router-dom'

import { TEAM_COLORS, TEAM_NAMES } from '../teamColors'

const API_BASE = `${import.meta.env.VITE_API_URL}/api`

function translateLabel(label: string): string {
  const parts = label.split(' | ').map(p => p.trim()).filter(Boolean)
  const translated = parts.map(part => {
    part = part.replace(/^Home — Heavy Favorite.*/, 'at home as a heavy favorite')
    part = part.replace(/^Home — Strong Favorite.*/, 'at home as a strong favorite')
    part = part.replace(/^Home — Mild Favorite.*/, 'at home as a mild favorite')
    part = part.replace(/^Home — Slight Favorite.*/, 'at home as a slight favorite')
    part = part.replace(/^Home — Favorite.*/, 'at home as a favorite')
    part = part.replace(/^Home — Pick.*/, "at home in a pick'em")
    part = part.replace(/^Home — Slight Underdog.*/, 'at home as a slight underdog')
    part = part.replace(/^Home — Clear Underdog.*/, 'at home as a clear underdog')
    part = part.replace(/^Home — Big Underdog.*/, 'at home as a big underdog')
    part = part.replace(/^Home — Underdog.*/, 'at home as an underdog')
    part = part.replace(/^Away — Heavy Favorite.*/, 'on the road as a heavy favorite')
    part = part.replace(/^Away — Strong Favorite.*/, 'on the road as a strong favorite')
    part = part.replace(/^Away — Mild Favorite.*/, 'on the road as a mild favorite')
    part = part.replace(/^Away — Slight Favorite.*/, 'on the road as a slight favorite')
    part = part.replace(/^Away — Favorite.*/, 'on the road as a favorite')
    part = part.replace(/^Away — Pick.*/, "on the road in a pick'em")
    part = part.replace(/^Away — Slight Underdog.*/, 'on the road as a slight underdog')
    part = part.replace(/^Away — Clear Underdog.*/, 'on the road as a clear underdog')
    part = part.replace(/^Away — Big Underdog.*/, 'on the road as a big underdog')
    part = part.replace(/^Away — Underdog.*/, 'on the road as an underdog')
    part = part.replace(/^Elite \(59%\+\)$/, 'one of the better teams in baseball')
    part = part.replace(/^Good \(53-58%\)$/, 'a winning team')
    part = part.replace(/^Average \(47-52%\)$/, 'a .500 team')
    part = part.replace(/^Poor \(41-46%\)$/, 'a losing team')
    part = part.replace(/^Bad \(<40%\)$/, 'one of the worst teams in baseball')
    part = part.replace(/^vs Elite.*opp$/, 'against a strong opponent')
    part = part.replace(/^vs Good.*opp$/, 'against a winning opponent')
    part = part.replace(/^vs Average.*opp$/, 'against an average opponent')
    part = part.replace(/^vs Poor.*opp$/, 'against a losing opponent')
    part = part.replace(/^vs Bad.*opp$/, 'against a struggling opponent')
    part = part.replace(/^W3\+$/, 'on a 3+ game win streak')
    part = part.replace(/^L3\+$/, 'on a 3+ game losing streak')
    part = part.replace(/^Early season \(G1-20\)$/, 'early in the season')
    part = part.replace(/^Mid season \(G21-100\)$/, 'mid-season')
    part = part.replace(/^Late season \(G100\+\)$/, 'late in the season')
    return part
  })
  const [first, ...rest] = translated
  const capitalized = first.charAt(0).toUpperCase() + first.slice(1)
  return rest.length === 0 ? capitalized : capitalized + ' ' + rest.join(', ')
}


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

function GapBadge({ value_gap, implied_prob, win_pct }: { value_gap: number, implied_prob: number, win_pct: number }) {
  const gapPct = Math.round(value_gap * 100)
  const impliedPct = Math.round(implied_prob * 100)
  const histPct = Math.round(win_pct * 100)
  const isPositive = gapPct > 0
  const isStrong = Math.abs(gapPct) >= 15
  const color = isPositive ? '#4caf50' : '#f44336'
  const icon = isStrong ? (isPositive ? '+ VALUE' : 'FADE') : (isPositive ? 'SLIGHT +' : 'SLIGHT -')
  return (
    <div style={{
      marginTop: '8px', fontSize: '11px', color,
      display: 'flex', alignItems: 'center', gap: '6px',
      background: isPositive ? 'rgba(76,175,80,0.08)' : 'rgba(244,67,54,0.08)',
      borderRadius: '4px', padding: '4px 8px'
    }}>
      <span style={{ fontWeight: 'bold', letterSpacing: '0.05em' }}>{icon}</span>
      <span style={{ color: '#94a3b8' }}>
        Odds imply <span style={{ color: '#e2e8f0' }}>{impliedPct}%</span> — history shows <span style={{ color }}>{histPct}%</span>
        <span style={{ fontWeight: 'bold', marginLeft: '4px' }}>({isPositive ? '+' : ''}{gapPct}%)</span>
      </span>
    </div>
  )
}

function SituationCard({ sit, plain }: { sit: Situation, plain: boolean }) {
  const hasGap = sit.value_gap !== null && Math.abs(sit.value_gap) >= 0.05
  return (
    <div style={{
      background: '#1a1f2e',
      borderRadius: '8px',
      padding: '14px 16px',
      border: `1px solid ${hasGap && Math.abs(sit.value_gap!) >= 0.15 ? (sit.value_gap! > 0 ? '#2e7d32' : '#b71c1c') : '#2a2f3e'}`,
    }}>
      <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '8px', lineHeight: 1.4 }}>
        {plain ? translateLabel(sit.label) : sit.label}
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
      {hasGap && sit.implied_prob && <GapBadge value_gap={sit.value_gap!} implied_prob={sit.implied_prob} win_pct={sit.win_pct} />}
    </div>
  )
}

function TeamPanel({ abbr, data, plain, isFirst, onToggle }: {
  abbr: string
  data: TeamSituations
  plain: boolean
  isFirst: boolean
  onToggle: () => void
}) {
  const { team_situations, league_situations } = data
  const hasTeam = team_situations.length > 0
  const hasLeague = league_situations.length > 0

  return (
    <div style={{ marginBottom: '40px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '20px' }}>
        <h3 style={{ color: '#e2e8f0', fontSize: '16px', fontWeight: 'bold', margin: 0, letterSpacing: '0.05em' }}>
          {TEAM_NAMES[abbr] ?? abbr}
        </h3>
        {isFirst && (
          <button
            onClick={onToggle}
            style={{
              background: plain ? 'rgba(147,197,253,0.12)' : 'none',
              border: `1px solid ${plain ? '#93c5fd' : '#2a2f3e'}`,
              color: plain ? '#93c5fd' : '#64748b',
              padding: '4px 10px',
              borderRadius: '6px',
              cursor: 'pointer',
              fontSize: '11px',
              letterSpacing: '0.04em',
              fontWeight: plain ? 'bold' : 'normal',
              transition: 'all 0.15s'
            }}
          >
            {plain ? 'DATA VIEW' : 'PLAIN ENGLISH'}
          </button>
        )}
      </div>

      {hasTeam && (
        <>
          <div style={{ color: '#93c5fd', fontSize: '11px', fontWeight: 'bold', letterSpacing: '0.08em', marginBottom: '10px' }}>
            TEAM HISTORY · 2022–2025
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', marginBottom: '24px' }}>
            {team_situations.map((sit, i) => <SituationCard key={i} sit={sit} plain={plain} />)}
          </div>
        </>
      )}

      {hasLeague && (
        <>
          <div style={{ color: '#86efac', fontSize: '11px', fontWeight: 'bold', letterSpacing: '0.08em', marginBottom: '10px' }}>
            LEAGUE CONTEXT · 2022–2025 · ALL TEAMS
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            {league_situations.map((sit, i) => <SituationCard key={i} sit={sit} plain={plain} />)}
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
  const [gameInfo, setGameInfo] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [plainLanguage, setPlainLanguage] = useState(false)

  useEffect(() => {
    if (!gameId || !gameDate) return
    setLoading(true)
    Promise.all([
      fetch(`${API_BASE}/games/${gameId}/situations?game_date=${gameDate}`).then(r => r.json()),
      fetch(`${API_BASE}/games/${gameDate}`).then(r => r.json()),
    ]).then(([situations, gamesData]) => {
      setData(situations)
      const game = gamesData.games?.find((g: any) => String(g.game_id) === String(gameId))
      if (game) setGameInfo(game)
      setLoading(false)
    }).catch(err => { setError(err.message); setLoading(false) })
  }, [gameId, gameDate])

  return (
    <div className="app">
      <header className="header">
        <a href="/"><img src="/logo.png" alt="Strikes + Downs" style={{ width: '67%', maxWidth: '300px', display: 'block', margin: '0 auto' }} /></a>
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
              <span style={{ color: TEAM_COLORS[data.away_team] ?? '#e2e8f0' }}>{data.away_team}</span> <span style={{ color: '#475569' }}>@</span> <span style={{ color: TEAM_COLORS[data.home_team] ?? '#e2e8f0' }}>{data.home_team}</span>
            </div>
            <div style={{ color: '#475569', fontSize: '13px', marginTop: '4px' }}>{data.game_date}</div>
            {gameInfo && (
              <div style={{ display: 'flex', justifyContent: 'center', gap: '16px', marginTop: '8px', flexWrap: 'wrap' }}>
                {gameInfo.start_time_et && (
                  <span style={{ color: '#64748b', fontSize: '13px' }}>🕐 {gameInfo.start_time_et}</span>
                )}
                {gameInfo.odds?.moneyline_away != null && (
                  <span style={{ color: '#94a3b8', fontSize: '13px' }}>
                    {data.away_team} {(() => { const o = gameInfo.odds.moneyline_away; return o >= 2 ? '+' + Math.round((o-1)*100) : '-' + Math.round(100/(o-1)) })()} 
                    <span style={{ color: '#475569' }}> / </span>
                    {data.home_team} {(() => { const o = gameInfo.odds.moneyline_home; return o >= 2 ? '+' + Math.round((o-1)*100) : '-' + Math.round(100/(o-1)) })()}
                  </span>
                )}
                {gameInfo.odds?.total_line != null && (
                  <span style={{ color: '#64748b', fontSize: '13px' }}>O/U {gameInfo.odds.total_line}</span>
                )}
              </div>
            )}
          </div>

          {[data.away_team, data.home_team].map((abbr, idx) => {
            const teamData = data.situations[abbr]
            if (!teamData) return null
            return (
              <TeamPanel
                key={abbr}
                abbr={abbr}
                data={teamData}
                plain={plainLanguage}
                isFirst={idx === 0}
                onToggle={() => setPlainLanguage(p => !p)}
              />
            )
          })}

          <div style={{ color: '#374151', fontSize: '11px', textAlign: 'center', borderTop: '1px solid #1a1f2e', paddingTop: '16px' }}>
            Historical patterns from 2022–2025 regular season · Sample size (n) shown for context · Not a prediction
          </div>
        </main>
      )}
    </div>
  )
}
