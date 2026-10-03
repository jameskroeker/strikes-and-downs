import { useEffect, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import './QueryBuilder.css'

const API_BASE = import.meta.env.VITE_API_URL

type Opt = { value: string; label: string }
const ANY: Opt = { value: '', label: 'Any' }

const SEASONS: Opt[] = [
  { value: 'hist', label: '2022–2025 (completed seasons)' },
  { value: 'all', label: 'All, including 2026' },
  { value: '2026', label: '2026 only' },
  { value: '2025', label: '2025' },
  { value: '2024', label: '2024' },
  { value: '2023', label: '2023' },
  { value: '2022', label: '2022' },
]
const SIDE: Opt[] = [ANY, { value: 'fav', label: 'Favorite (any spread)' }, { value: 'dog', label: 'Underdog (any spread)' }]
const SPREAD_BANDS: Opt[] = [
  ANY,
  { value: 'fav_0.5-2.5', label: 'Fav 0.5–2.5' }, { value: 'fav_3-3.5', label: 'Fav 3–3.5' },
  { value: 'fav_4-6.5', label: 'Fav 4–6.5' }, { value: 'fav_7-9.5', label: 'Fav 7–9.5' }, { value: 'fav_10+', label: 'Fav 10+' },
  { value: 'dog_0.5-2.5', label: 'Dog 0.5–2.5' }, { value: 'dog_3-3.5', label: 'Dog 3–3.5' },
  { value: 'dog_4-6.5', label: 'Dog 4–6.5' }, { value: 'dog_7-9.5', label: 'Dog 7–9.5' }, { value: 'dog_10+', label: 'Dog 10+' },
]
const TOTAL_BANDS: Opt[] = [ANY, { value: 'lt40', label: 'Under 40' }, { value: '40-44.5', label: '40–44.5' },
  { value: '45-49.5', label: '45–49.5' }, { value: '50plus', label: '50+' }]
const PHASES: Opt[] = [ANY, { value: 'w1-6', label: 'Weeks 1–6' }, { value: 'w7-12', label: 'Weeks 7–12' },
  { value: 'w13-18', label: 'Weeks 13–18' }, { value: 'playoffs', label: 'Playoffs' }]
const WEEKS: Opt[] = [ANY, ...Array.from({ length: 18 }, (_, i) => ({ value: String(i + 1), label: `Week ${i + 1}` }))]
const HOME_AWAY: Opt[] = [ANY, { value: 'home', label: 'Home' }, { value: 'away', label: 'Away' }]
const YES_NO: Opt[] = [ANY, { value: 'true', label: 'Yes' }, { value: 'false', label: 'No' }]
const KICKOFF: Opt[] = [ANY, { value: 'thu', label: 'Thursday' }, { value: 'fri', label: 'Friday' }, { value: 'sat', label: 'Saturday' },
  { value: 'sun', label: 'Sunday (day)' }, { value: 'snf', label: 'Sunday night' }, { value: 'mnf', label: 'Monday night' },
  { value: 'primetime', label: 'Any primetime (night)' }, { value: 'other', label: 'Other (Wednesday)' }]
const REST: Opt[] = [ANY, { value: 'off_bye', label: 'Off a bye week' }, { value: 'off_thu', label: 'Off a Thursday game' },
  { value: 'off_fri_sat', label: 'Off a Friday/Saturday game' }, { value: 'off_sun', label: 'Off a Sunday game (normal)' },
  { value: 'off_mnf', label: 'Off a Monday night game' }, { value: 'opener', label: 'Season opener' }]
const ROAD: Opt[] = [ANY, { value: 'home', label: 'At home' }, { value: 'road1', label: '1st road game' }, { value: 'road2plus', label: '2nd+ straight road game' }]
const HOME_AFTER_ROAD: Opt[] = [ANY, { value: '0', label: 'No' }, { value: '1', label: 'After 1 road game' }, { value: '2plus', label: 'After 2+ road games' }]
const WINS: Opt[] = [ANY, ...Array.from({ length: 18 }, (_, i) => ({ value: String(i), label: `${i} win${i === 1 ? '' : 's'}` }))]
const PCT_BANDS: Opt[] = [ANY, { value: 'lt400', label: 'Under .400' }, { value: '400-499', label: '.400–.499' },
  { value: '500', label: 'Exactly .500' }, { value: '501-599', label: '.501–.599' }, { value: '600plus', label: '.600+' }]
const PREV_RESULT: Opt[] = [ANY, { value: 'blowout_win', label: 'Won by 17+' }, { value: 'win', label: 'Won by 1–16' },
  { value: 'loss', label: 'Lost by 1–16' }, { value: 'blowout_loss', label: 'Lost by 17+' }]
const PREV_ATS: Opt[] = [ANY, { value: 'covered', label: 'Covered' }, { value: 'missed', label: 'Did not cover' }]
const PREV_UPSET: Opt[] = [ANY, { value: 'upset_win', label: 'Won as underdog' }, { value: 'upset_loss', label: 'Lost as favorite' }, { value: 'none', label: 'No upset' }]

const EMPTY = {
  seasons: 'hist', team: '', opponent: '', home_away: '', exclude_neutral: '', international: '', divisional: '', kickoff: '',
  phase: '', week: '', side: '', spread_band: '', total_band: '',
  rest: '', road: '', home_after_road: '', wins: '', ats_wins: '', win_pct: '', ats_pct: '',
  prev_result: '', prev_ats: '', prev_upset: '', prev_ot: '', prev_intl: '',
  opp_rest: '', opp_wins: '', opp_ats_wins: '', opp_win_pct: '', opp_ats_pct: '',
  opp_prev_result: '', opp_prev_ats: '', opp_prev_upset: '',
}
type Filters = typeof EMPTY

const SUGGESTED: { label: string; description: string; filters: Partial<Filters> }[] = [
  { label: 'Early-season favorites of 7–9.5', description: 'Moderate favorites in Weeks 1–6', filters: { phase: 'w1-6', spread_band: 'fav_7-9.5' } },
  { label: 'Week 3: 2–0 ATS vs a 1–1 ATS opponent', description: 'Hot ATS starts in Week 3', filters: { week: '3', ats_wins: '2', opp_ats_wins: '1' } },
  { label: 'Week 3: no ATS wins yet', description: 'Teams without a cover in their first two games', filters: { week: '3', ats_wins: '0' } },
  { label: 'Road favorites of 10+', description: 'Double-digit favorites away from home', filters: { home_away: 'away', spread_band: 'fav_10+' } },
  { label: 'Divisional road favorites of 0.5–2.5', description: 'Short road favorites in division games', filters: { divisional: 'true', home_away: 'away', spread_band: 'fav_0.5-2.5' } },
  { label: 'Off a blowout win, now favored', description: 'Teams that won by 17+ last week', filters: { prev_result: 'blowout_win', side: 'fav' } },
]

function pct(v: number | null | undefined): string {
  return v == null ? '—' : (v * 100).toFixed(1) + '%'
}
function edgeColor(v: number | null | undefined): string {
  if (v == null) return '#94a3b8'
  const d = Math.abs(v - 0.5)
  if (d >= 0.12) return v > 0.5 ? '#4ade80' : '#f87171'
  if (d >= 0.06) return v > 0.5 ? '#86efac' : '#fca5a5'
  return '#e2e8f0'
}
function fmtSpread(s: number | null): string {
  if (s == null) return '—'
  return s > 0 ? `+${s}` : String(s)
}

function Select({ label, k, opts, f, set, disabled }: { label: string; k: keyof Filters; opts: Opt[]; f: Filters; set: (k: keyof Filters, v: string) => void; disabled?: boolean }) {
  return (
    <div className="qb-filter-group">
      <label className="qb-label">{label}</label>
      <select className="qb-select" value={f[k]} disabled={disabled} onChange={e => set(k, e.target.value)}>
        {opts.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
      </select>
    </div>
  )
}

function SectionLabel({ children }: { children: string }) {
  return <div style={{ color: '#93c5fd', fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.06em', margin: '8px 0 10px', fontWeight: 600 }}>{children}</div>
}

export function NflQueryBuilder() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const [teams, setTeams] = useState<string[]>([])
  const [meta, setMeta] = useState<any>(null)
  const [filters, setFilters] = useState<Filters>(() => {
    const f = { ...EMPTY }
    ;(Object.keys(f) as (keyof Filters)[]).forEach(k => { const v = searchParams.get(k); if (v) f[k] = v })
    return f
  })
  const [result, setResult] = useState<any>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    fetch(`${API_BASE}/api/nfl/meta`).then(r => r.json()).then(m => { setMeta(m); setTeams(m.teams || []) }).catch(() => {})
  }, [])

  // Run automatically when arriving via a shared link
  useEffect(() => {
    if ([...searchParams.keys()].length > 0) runQuery()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const set = (k: keyof Filters, v: string) => setFilters(f => ({ ...f, [k]: v }))

  function buildParams(f: Filters): URLSearchParams {
    const p = new URLSearchParams()
    Object.entries(f).forEach(([k, v]) => { if (v !== '') p.append(k, v) })
    return p
  }

  async function runQuery(f: Filters = filters) {
    setLoading(true); setError(null); setResult(null)
    const params = buildParams(f)
    setSearchParams(params)
    try {
      const res = await fetch(`${API_BASE}/api/nfl/query?${params.toString()}`)
      if (!res.ok) throw new Error()
      setResult(await res.json())
    } catch {
      setError('Failed to fetch results')
    } finally {
      setLoading(false)
    }
  }

  function loadSuggestion(s: Partial<Filters>) {
    const f = { ...EMPTY, ...s }
    setFilters(f)
    runQuery(f)
  }

  function reset() {
    setFilters(EMPTY); setResult(null); setError(null); setSearchParams({})
  }

  function share() {
    const url = `${window.location.origin}/nfl?${buildParams(filters).toString()}`
    navigator.clipboard.writeText(url).then(() => { setCopied(true); setTimeout(() => setCopied(false), 2000) })
  }

  const teamOpts: Opt[] = [ANY, ...teams.map(t => ({ value: t, label: t }))]
  const ats = result?.ats

  return (
    <div className="app">
      <header className="header">
        <a href="/"><img src="/logo.png" alt="Strikes + Downs" style={{ width: '67%', maxWidth: '300px', display: 'block', margin: '0 auto' }} /></a>
      </header>
      <div className="qb-nav">
        <button className="qb-nav-btn" onClick={() => navigate('/')}>← Home</button>
      </div>

      <div className="qb-container">
        <h2 className="qb-title">NFL Query Builder</h2>
        <p className="qb-subtitle">
          Pick a situation and see how NFL teams have performed against the spread, straight up and on totals.
          {meta?.data_through && <> Data through {meta.data_through.season} {meta.data_through.week} ({meta.games.toLocaleString()} games).</>}
        </p>

        <div style={{ marginBottom: '24px' }}>
          <div style={{ color: '#64748b', fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '10px' }}>Start with a pattern</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
            {SUGGESTED.map((s, i) => (
              <button key={i} onClick={() => loadSuggestion(s.filters)} title={s.description}
                style={{ background: '#1a1f2e', border: '1px solid #2a2f3e', borderRadius: '6px', padding: '6px 12px', color: '#93c5fd', fontSize: '12px', cursor: 'pointer' }}
                onMouseEnter={e => (e.currentTarget.style.borderColor = '#93c5fd')}
                onMouseLeave={e => (e.currentTarget.style.borderColor = '#2a2f3e')}>
                {s.label}
              </button>
            ))}
          </div>
        </div>

        <SectionLabel>Game</SectionLabel>
        <div className="qb-filters">
          <Select label="Seasons" k="seasons" opts={SEASONS} f={filters} set={set} />
          <Select label="Team" k="team" opts={teamOpts} f={filters} set={set} />
          <Select label="Opponent" k="opponent" opts={teamOpts} f={filters} set={set} />
          <Select label="Home / Away" k="home_away" opts={HOME_AWAY} f={filters} set={set} />
          <Select label="Exclude neutral-site games" k="exclude_neutral" opts={[ANY, { value: 'true', label: 'Yes' }]} f={filters} set={set} />
          <Select label="International game" k="international" opts={[ANY, { value: 'only', label: 'International only' }, { value: 'exclude', label: 'Exclude international' }]} f={filters} set={set} />
          <Select label="Divisional" k="divisional" opts={YES_NO} f={filters} set={set} />
          <Select label="Game day" k="kickoff" opts={KICKOFF} f={filters} set={set} />
          <Select label="Season phase" k="phase" opts={PHASES} f={filters} set={set} />
          <Select label="Week" k="week" opts={WEEKS} f={filters} set={set} />
        </div>

        <SectionLabel>Line</SectionLabel>
        <div className="qb-filters">
          <Select label="Favorite / Underdog" k="side" opts={SIDE} f={filters} set={set} />
          <Select label="Spread" k="spread_band" opts={SPREAD_BANDS} f={filters} set={set} />
          <Select label="Total" k="total_band" opts={TOTAL_BANDS} f={filters} set={set} />
        </div>

        <SectionLabel>Team situation (entering the game)</SectionLabel>
        <div className="qb-filters">
          <Select label="Last game played" k="rest" opts={REST} f={filters} set={set} />
          <Select label="Road trip" k="road" opts={ROAD} f={filters} set={set} />
          <Select label="Home after road trip" k="home_after_road" opts={HOME_AFTER_ROAD} f={filters} set={set} />
          <Select label="Wins entering" k="wins" opts={WINS} f={filters} set={set} />
          <Select label="ATS wins entering" k="ats_wins" opts={WINS} f={filters} set={set} />
          <Select label="Win % (3+ games)" k="win_pct" opts={PCT_BANDS} f={filters} set={set} />
          <Select label="ATS % (3+ games)" k="ats_pct" opts={PCT_BANDS} f={filters} set={set} />
          <Select label="Last game result" k="prev_result" opts={PREV_RESULT} f={filters} set={set} />
          <Select label="Last game ATS" k="prev_ats" opts={PREV_ATS} f={filters} set={set} />
          <Select label="Last game upset" k="prev_upset" opts={PREV_UPSET} f={filters} set={set} />
          <Select label="Last game went to OT" k="prev_ot" opts={YES_NO} f={filters} set={set} />
          <Select label="Last game was international" k="prev_intl" opts={YES_NO} f={filters} set={set} />
        </div>

        <SectionLabel>Opponent situation (entering the game)</SectionLabel>
        <div className="qb-filters">
          <Select label="Opp last game played" k="opp_rest" opts={REST} f={filters} set={set} />
          <Select label="Opp wins entering" k="opp_wins" opts={WINS} f={filters} set={set} />
          <Select label="Opp ATS wins entering" k="opp_ats_wins" opts={WINS} f={filters} set={set} />
          <Select label="Opp win % (3+ games)" k="opp_win_pct" opts={PCT_BANDS} f={filters} set={set} />
          <Select label="Opp ATS % (3+ games)" k="opp_ats_pct" opts={PCT_BANDS} f={filters} set={set} />
          <Select label="Opp last game result" k="opp_prev_result" opts={PREV_RESULT} f={filters} set={set} />
          <Select label="Opp last game ATS" k="opp_prev_ats" opts={PREV_ATS} f={filters} set={set} />
          <Select label="Opp last game upset" k="opp_prev_upset" opts={PREV_UPSET} f={filters} set={set} />
        </div>

        <div className="qb-actions">
          <button className="qb-btn-primary" onClick={() => runQuery()} disabled={loading}>{loading ? 'Running...' : 'Run Query'}</button>
          <button className="qb-btn-secondary" onClick={reset}>Reset</button>
          {result && (
            <button onClick={share} style={{
              background: copied ? 'rgba(74,222,128,0.12)' : 'none', border: `1px solid ${copied ? '#4ade80' : '#2a2f3e'}`,
              color: copied ? '#4ade80' : '#64748b', padding: '6px 14px', borderRadius: '6px', cursor: 'pointer', fontSize: '12px',
            }}>{copied ? '✓ Copied' : '🔗 Share'}</button>
          )}
        </div>

        {error && <div className="qb-error">{error}</div>}

        {result && (
          <div className="qb-result">
            {result.n === 0 ? (
              <p className="qb-no-results">No matching games. Try removing a filter.</p>
            ) : (
              <>
                <div className="qb-result-main">
                  <div className="qb-stat">
                    <span className="qb-stat-value" style={{ color: edgeColor(ats.cover_pct) }}>{ats.covers}-{ats.misses}{ats.pushes ? `-${ats.pushes}` : ''}</span>
                    <span className="qb-stat-label">ATS (W-L-P)</span>
                  </div>
                  <div className="qb-stat">
                    <span className="qb-stat-value" style={{ color: edgeColor(ats.cover_pct) }}>{pct(ats.cover_pct)}</span>
                    <span className="qb-stat-label">Cover rate</span>
                  </div>
                  <div className="qb-stat">
                    <span className="qb-stat-value">{result.su.wins}-{result.su.losses}{result.su.ties ? `-${result.su.ties}` : ''}</span>
                    <span className="qb-stat-label">Straight up</span>
                  </div>
                  <div className="qb-stat">
                    <span className="qb-stat-value">{result.ou.overs}-{result.ou.unders}{result.ou.pushes ? `-${result.ou.pushes}` : ''}</span>
                    <span className="qb-stat-label">Over-Under ({pct(result.ou.over_pct)} over)</span>
                  </div>
                  <div className="qb-stat">
                    <span className="qb-stat-value">{result.n}</span>
                    <span className="qb-stat-label">Games</span>
                  </div>
                </div>

                <div style={{ marginTop: '14px', fontSize: '12px', color: '#94a3b8', display: 'flex', gap: '18px', flexWrap: 'wrap' }}>
                  <span>Avg margin: <b style={{ color: '#e2e8f0' }}>{result.avg_margin ?? '—'}</b></span>
                  <span>Avg spread: <b style={{ color: '#e2e8f0' }}>{fmtSpread(result.avg_spread)}</b></span>
                  <span>Avg vs spread: <b style={{ color: '#e2e8f0' }}>{fmtSpread(result.avg_cover_margin)}</b></span>
                  <span>Avg points: <b style={{ color: '#e2e8f0' }}>{result.avg_total_points ?? '—'}</b> vs line <b style={{ color: '#e2e8f0' }}>{result.avg_total_line ?? '—'}</b></span>
                </div>

                {result.sample_warning && (
                  <p className="qb-warning">⚠️ Fewer than 15 decided games against the spread. Treat this as context, not evidence.</p>
                )}

                {result.by_season?.length > 1 && (
                  <div style={{ marginTop: '18px' }}>
                    <div style={{ color: '#64748b', fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '8px' }}>
                      By season: does it hold every year?
                    </div>
                    <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                      {result.by_season.map((s: any) => (
                        <div key={s.season} style={{ background: '#1a1f2e', borderRadius: '6px', padding: '8px 12px', minWidth: '92px' }}>
                          <div style={{ fontSize: '11px', color: '#64748b' }}>{s.season}</div>
                          <div style={{ fontSize: '15px', fontWeight: 700, color: edgeColor(s.ats.cover_pct) }}>{pct(s.ats.cover_pct)}</div>
                          <div style={{ fontSize: '11px', color: '#94a3b8' }}>{s.ats.covers}-{s.ats.misses}{s.ats.pushes ? `-${s.ats.pushes}` : ''} ATS</div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {result.games?.length > 0 && (
                  <div style={{ marginTop: '18px' }}>
                    <div style={{ color: '#64748b', fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '8px' }}>
                      Matching games {result.games_truncated ? `(most recent ${result.games.length} of ${result.n})` : ''}
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', overflowX: 'auto' }}>
                      <div style={{ display: 'grid', gridTemplateColumns: '84px 70px minmax(150px,1fr) 92px 56px 64px 44px 44px', gap: '8px', padding: '0 12px 0 15px', fontSize: '11px', color: '#475569', minWidth: '660px' }}>
                        <span>Date</span><span>Week</span><span>Matchup</span>
                        <span style={{ textAlign: 'right' }}>Entering (ATS)</span><span style={{ textAlign: 'right' }}>Spread</span>
                        <span style={{ textAlign: 'right' }}>Score</span><span style={{ textAlign: 'right' }}>ATS</span><span style={{ textAlign: 'right' }}>O/U</span>
                      </div>
                      {result.games.map((g: any, i: number) => {
                        const c = g.ats === 'cover' ? '#4caf50' : g.ats === 'miss' ? '#ef4444' : '#64748b'
                        return (
                          <div key={i} style={{
                            display: 'grid', gridTemplateColumns: '84px 70px minmax(150px,1fr) 92px 56px 64px 44px 44px', gap: '8px', alignItems: 'center',
                            background: '#1a1f2e', borderRadius: '6px', padding: '6px 12px', fontSize: '12px', color: '#94a3b8',
                            borderLeft: `3px solid ${c}`, minWidth: '660px',
                          }}>
                            <span style={{ color: '#64748b' }}>{g.game_date}</span>
                            <span style={{ color: '#64748b' }}>{g.season} {g.week}</span>
                            <span><b style={{ color: '#e2e8f0' }}>{g.team}</b> {g.home_away === 'Home' ? 'vs' : g.home_away === 'Neutral' ? 'n/' : '@'} {g.opponent}</span>
                            <span style={{ textAlign: 'right', color: '#64748b' }} title="Record and ATS record entering the game">{g.entering_record} <span style={{ color: '#475569' }}>({g.entering_ats})</span></span>
                            <span style={{ textAlign: 'right' }}>{fmtSpread(g.spread)}</span>
                            <span style={{ textAlign: 'right', color: '#e2e8f0' }}>{g.team_score}-{g.opponent_score}</span>
                            <span style={{ textAlign: 'right', fontWeight: 700, color: c }}>{g.ats === 'cover' ? 'W' : g.ats === 'miss' ? 'L' : 'P'}</span>
                            <span style={{ textAlign: 'right', color: '#64748b' }}>{g.ou === 'over' ? 'O' : g.ou === 'under' ? 'U' : 'P'}</span>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        )}

        <p style={{ color: '#475569', fontSize: '11px', marginTop: '18px', lineHeight: 1.5 }}>
          Records are from each team's perspective, so every game appears twice (once per team). With no team-specific
          filter, mirrored situations cancel out to about 50%. Situational filters use only what was known before kickoff.
          Pushes are excluded from cover and over rates.
        </p>
      </div>
    </div>
  )
}
