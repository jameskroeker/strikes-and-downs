import { useState, useEffect, useRef } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { fetchGamesForDate, fetchSignalsForDate } from '../api'
import type { Game } from '../types'
import { TEAM_COLORS } from '../teamColors'
import './QueryBuilder.css'

const ODDS_BUCKETS = [
  { value: '', label: 'Any' },
  { value: 'heavy_favorite', label: 'Heavy Favorite (-400 to -250)' },
  { value: 'strong_favorite', label: 'Strong Favorite (-250 to -200)' },
  { value: 'favorite', label: 'Favorite (-200 to -154)' },
  { value: 'mild_favorite', label: 'Mild Favorite (-154 to -133)' },
  { value: 'slight_favorite', label: 'Slight Favorite (-133 to even)' },
  { value: 'pick', label: 'Pick (even to +110)' },
  { value: 'slight_underdog', label: 'Slight Underdog (+110 to +130)' },
  { value: 'underdog', label: 'Underdog (+130 to +150)' },
  { value: 'clear_underdog', label: 'Clear Underdog (+150 to +225)' },
  { value: 'big_underdog', label: 'Big Underdog (+225+)' },
]

const TEAM_BUCKETS = [
  { value: '', label: 'Any' },
  { value: 'elite', label: 'Elite (59%+)' },
  { value: 'good', label: 'Good (53-58%)' },
  { value: 'average', label: 'Average (47-52%)' },
  { value: 'poor', label: 'Poor (41-46%)' },
  { value: 'bad', label: 'Bad (<40%)' },
]

const GAME_COUNT_BUCKETS = [
  { value: '', label: 'Any' },
  { value: 'early', label: 'Early Season (G1-20)' },
  { value: 'mid-early', label: 'Mid-Early (G21-60)' },
  { value: 'mid', label: 'Mid Season (G61-100)' },
  { value: 'mid-late', label: 'Mid-Late (G101-130)' },
  { value: 'late', label: 'Late Season (G131-162)' },
]

const STREAK_VALUES = [
  { value: '', label: 'Any' },
  ...Array.from({length: 9}, (_, i) => ({ value: String(i + 1), label: String(i + 1) })),
  { value: '10', label: '10+' },
]

const L10_RUN_BUCKETS = [
  { value: '', label: 'Any' },
  { value: 'under4', label: 'Under 4.0' },
  { value: '4to4.5', label: '4.0 – 4.5' },
  { value: '4.5to5', label: '4.5 – 5.0' },
  { value: '5to5.5', label: '5.0 – 5.5' },
  { value: '5.5to6', label: '5.5 – 6.0' },
  { value: '6plus', label: '6.0+' },
]

const TOTAL_BUCKETS = [
  { value: '', label: 'Any' },
  { value: 'low', label: '7.0 or under' },
  { value: '7.5', label: '7.5' },
  { value: '8', label: '8.0' },
  { value: '8.5', label: '8.5' },
  { value: '9', label: '9.0' },
  { value: 'high', label: '9.5+' },
]

const TEAMS = [
  '', 'ARI', 'ATL', 'ATH', 'BAL', 'BOS', 'CHC', 'CIN', 'CLE', 'COL', 'CWS',
  'DET', 'HOU', 'KCR', 'LAA', 'LAD', 'MIA', 'MIL', 'MIN', 'NYM', 'NYY',
  'PHI', 'PIT', 'SDP', 'SEA', 'SFG', 'STL', 'TBR', 'TEX', 'TOR', 'WSH'
]

// Suggested starting queries to guide first-time users
const SUGGESTED_QUERIES = [
  {
    label: 'Home favorites, hot streak',
    description: 'Good teams at home on a win streak as favorites',
    filters: { is_home: 'true', odds_bucket: 'favorite', team_bucket: 'good', streak_direction: 'W', streak_entering: '3' },
  },
  {
    label: 'Underdogs bouncing back',
    description: 'Average teams as underdogs after a losing streak',
    filters: { is_home: '', odds_bucket: 'clear_underdog', team_bucket: 'average', streak_direction: 'L', streak_entering: '3' },
  },
  {
    label: 'Elite teams vs struggling opponents',
    description: 'Elite teams facing poor opponents, mid-season',
    filters: { team_bucket: 'elite', opp_bucket: 'bad', game_count_bucket: 'mid' },
  },
  {
    label: 'Post All-Star road dogs',
    description: 'Good teams on the road as underdogs in the second half',
    filters: { is_home: 'false', odds_bucket: 'slight_underdog', team_bucket: 'good', game_count_bucket: 'mid-late' },
  },
]

const API_BASE = import.meta.env.VITE_API_URL || 'https://strikes-and-downs.onrender.com'

const EMPTY_FILTERS = {
  team_abbr: '', is_home: '', odds_bucket: '', team_bucket: '',
  opp_bucket: '', game_count_bucket: '', streak_direction: '',
  streak_entering: '', rest: '', division_game: '', interleague: '',
}

function deviationColor(dev: number): string {
  if (dev >= 0.25) return '#4caf50'
  if (dev >= 0.15) return '#fbbf24'
  return '#94a3b8'
}

function ouColor(pct: number): string {
  const edge = Math.abs(pct - 0.5)
  if (edge >= 0.07) return '#4caf50'
  if (edge >= 0.04) return '#fbbf24'
  return '#94a3b8'
}

function formatML(ml: number | null): string {
  if (!ml) return '-'
  if (ml >= 2.0) return '+' + String(Math.round((ml - 1) * 100))
  return String(Math.round(-(100 / (ml - 1))))
}

function getTodayET(): string {
  const fmt = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' })
  return fmt.format(new Date())
}

function GameStrip({ games, signals }: { games: Game[], signals: Record<string, any> }) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const scroll = (dir: number) => scrollRef.current?.scrollBy({ left: dir * 220, behavior: 'smooth' })
  return (
    <div style={{ position: 'relative' }}>
      <button onClick={() => scroll(-1)} style={{
        position: 'absolute', left: 0, top: '50%', transform: 'translateY(-50%)',
        zIndex: 2, background: 'rgba(15,20,30,0.85)', border: '1px solid #2a2f3e',
        borderRadius: '50%', width: '28px', height: '28px', cursor: 'pointer',
        color: '#94a3b8', fontSize: '14px', display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}>‹</button>
      <button onClick={() => scroll(1)} style={{
        position: 'absolute', right: 0, top: '50%', transform: 'translateY(-50%)',
        zIndex: 2, background: 'rgba(15,20,30,0.85)', border: '1px solid #2a2f3e',
        borderRadius: '50%', width: '28px', height: '28px', cursor: 'pointer',
        color: '#94a3b8', fontSize: '14px', display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}>›</button>
    <div ref={scrollRef} style={{ overflowX: 'auto', paddingBottom: '8px', marginBottom: '24px', scrollbarWidth: 'none', paddingLeft: '36px', paddingRight: '36px' }}>
      <div style={{ display: 'flex', gap: '10px', minWidth: 'max-content', padding: '4px 2px' }}>
        {games.map(game => {
          const sig = signals[game.game_id]
          const isT1 = sig && sig.tier === 1
          const awayColor = TEAM_COLORS[game.away_team.abbr] || '#94a3b8'
          const homeColor = TEAM_COLORS[game.home_team.abbr] || '#94a3b8'
          return (
            <div key={game.game_id} style={{
              background: '#1a1f2e',
              border: isT1 ? '1px solid #facc15' : '1px solid #2a2f3e',
              borderRadius: '8px', padding: '10px 14px',
              minWidth: '185px', fontSize: '12px',
              color: '#94a3b8', position: 'relative',
            }}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: awayColor, fontWeight: 'bold', fontSize: '13px' }}>{game.away_team.abbr}{isT1 && sig.signal_team === game.away_team.abbr ? ' ⚡⚡' : ''}</span>
                  <span style={{ color: '#64748b', fontSize: '11px' }}>{game.away_team.wins}-{game.away_team.losses} · {(game.away_team.win_pct * 100).toFixed(0)}%</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', color: '#64748b' }}>
                  <span>{(game.away_team as any).l10_runs_scored ?? '-'} RS · {(game.away_team as any).l10_runs_allowed ?? '-'} RA</span>
                  <span>{formatML(game.odds.moneyline_away)}</span>
                  <span style={{ color: game.away_team.streak.startsWith('W') ? '#4caf50' : '#ef4444' }}>{game.away_team.streak}</span>
                </div>
                <div style={{ borderTop: '1px solid #2a2f3e', margin: '2px 0' }} />
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: homeColor, fontWeight: 'bold', fontSize: '13px' }}>{game.home_team.abbr}{isT1 && sig.signal_team === game.home_team.abbr ? ' ⚡⚡' : ''}</span>
                  <span style={{ color: '#64748b', fontSize: '11px' }}>{game.home_team.wins}-{game.home_team.losses} · {(game.home_team.win_pct * 100).toFixed(0)}%</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', color: '#64748b' }}>
                  <span>{(game.home_team as any).l10_runs_scored ?? '-'} RS · {(game.home_team as any).l10_runs_allowed ?? '-'} RA</span>
                  <span>{formatML(game.odds.moneyline_home)}</span>
                  <span style={{ color: game.home_team.streak.startsWith('W') ? '#4caf50' : '#ef4444' }}>{game.home_team.streak}</span>
                </div>
                {game.odds.total_line && (
                  <div style={{ textAlign: 'center', color: '#64748b', fontSize: '11px', marginTop: '2px' }}>O/U {game.odds.total_line}</div>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
    </div>
  )
}

export function QueryBuilder() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const [mode, setMode] = useState<'sides' | 'ou'>('sides')
  const [games, setGames] = useState<Game[]>([])
  const [signals, setSignals] = useState<Record<string, any>>({})
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    const today = getTodayET()
    fetchGamesForDate(today).then(r => setGames(r.games)).catch(() => {})
    fetchSignalsForDate(today).then(s => setSignals(s)).catch(() => {})
  }, [])

  // Initialize filters from URL params
  const [filters, setFilters] = useState(() => {
    const f = { ...EMPTY_FILTERS }
    Object.keys(f).forEach(k => {
      const v = searchParams.get(k)
      if (v) (f as any)[k] = v
    })
    return f
  })

  const [ouFilters, setOuFilters] = useState({
    team_abbr: '', is_home: '', total_bucket: '', home_l10_scored: '',
    away_l10_scored: '', team_bucket: '', opp_bucket: '', division_game: '',
    streak_direction: '', streak_entering: '',
  })

  const [result, setResult] = useState<any>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  function setFilter(key: string, value: string) {
    setFilters(f => ({ ...f, [key]: value }))
  }

  function setOuFilter(key: string, value: string) {
    setOuFilters(f => ({ ...f, [key]: value }))
  }

  function loadSuggestion(suggested: Record<string, string>) {
    setFilters({ ...EMPTY_FILTERS, ...suggested })
    setResult(null)
    setError(null)
  }

  function copyShareLink() {
    const params = new URLSearchParams()
    Object.entries(filters).forEach(([k, v]) => { if (v !== '') params.append(k, v) })
    const url = `${window.location.origin}/query?${params.toString()}`
    navigator.clipboard.writeText(url).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  async function runQuery() {
    setLoading(true)
    setError(null)
    setResult(null)

    // Sync filters to URL
    const params = new URLSearchParams()
    Object.entries(filters).forEach(([k, v]) => { if (v !== '') params.append(k, v) })
    setSearchParams(params)

    try {
      if (mode === 'sides') {
        const res = await fetch(`${API_BASE}/api/query?${params.toString()}`)
        const data = await res.json()

        // Also fetch 2026 season record for this situation
        const params2026 = new URLSearchParams(params)
        params2026.append('season', '2026')
        let season2026 = null
        try {
          const res2026 = await fetch(`${API_BASE}/api/query?${params2026.toString()}`)
          const d2026 = await res2026.json()
          if (!d2026.message) season2026 = d2026
        } catch {}

        setResult({ mode: 'sides', ...data, season2026 })
      } else {
        const params = new URLSearchParams()
        Object.entries(ouFilters).forEach(([k, v]) => { if (v !== '') params.append(k, v) })
        const res = await fetch(`${API_BASE}/api/query/ou?${params.toString()}`)
        const data = await res.json()
        setResult({ mode: 'ou', ...data })
      }
    } catch {
      setError('Failed to fetch results')
    } finally {
      setLoading(false)
    }
  }

  function resetFilters() {
    setFilters(EMPTY_FILTERS)
    setOuFilters({
      team_abbr: '', is_home: '', total_bucket: '', home_l10_scored: '',
      away_l10_scored: '', team_bucket: '', opp_bucket: '', division_game: '',
      streak_direction: '', streak_entering: '',
    })
    setResult(null)
    setError(null)
    setSearchParams({})
  }

  const hasEdge = result && result.mode === 'sides' && !result.message &&
    result.win_pct != null && result.win_pct >= 0.58 &&
    result.deviation != null && result.deviation >= 0.15 &&
    result.n >= 20

  return (
    <div className="app">
      <header className="header">
        <a href="/"><img src="/logo.png" alt="Strikes + Downs" style={{ width: '67%', maxWidth: '300px', display: 'block', margin: '0 auto' }} /></a>
      </header>
      <div className="qb-nav">
        <button className="qb-nav-btn" onClick={() => navigate('/')}>← Back to Games</button>
      </div>
      {games.length > 0 && (
        <div style={{ padding: '0 16px', marginBottom: '8px' }}>
          <div style={{ color: '#64748b', fontSize: '11px', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Today's Games</div>
          <GameStrip games={games} signals={signals} />
        </div>
      )}

      <div className="qb-container">
        <h2 className="qb-title">Query Builder</h2>
        <p className="qb-subtitle">Define conditions and see how teams have historically performed</p>

        {/* Suggested queries */}
        <div style={{ marginBottom: '24px' }}>
          <div style={{ color: '#64748b', fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '10px' }}>Start with a pattern</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
            {SUGGESTED_QUERIES.map((s, i) => (
              <button
                key={i}
                onClick={() => loadSuggestion(s.filters)}
                title={s.description}
                style={{
                  background: '#1a1f2e', border: '1px solid #2a2f3e',
                  borderRadius: '6px', padding: '6px 12px',
                  color: '#93c5fd', fontSize: '12px', cursor: 'pointer',
                  transition: 'all 0.15s',
                }}
                onMouseEnter={e => (e.currentTarget.style.borderColor = '#93c5fd')}
                onMouseLeave={e => (e.currentTarget.style.borderColor = '#2a2f3e')}
              >
                {s.label}
              </button>
            ))}
          </div>
        </div>

        <div style={{ display: 'flex', gap: '8px', marginBottom: '24px' }}>
          <button
            onClick={() => { setMode('sides'); setResult(null) }}
            style={{
              background: mode === 'sides' ? 'rgba(147,197,253,0.12)' : 'none',
              border: `1px solid ${mode === 'sides' ? '#93c5fd' : '#2a2f3e'}`,
              color: mode === 'sides' ? '#93c5fd' : '#64748b',
              padding: '6px 16px', borderRadius: '6px', cursor: 'pointer', fontSize: '13px',
              fontWeight: mode === 'sides' ? 'bold' : 'normal'
            }}
          >
            Win / Loss
          </button>
          <button
            onClick={() => { setMode('ou'); setResult(null) }}
            style={{
              background: mode === 'ou' ? 'rgba(147,197,253,0.12)' : 'none',
              border: `1px solid ${mode === 'ou' ? '#93c5fd' : '#2a2f3e'}`,
              color: mode === 'ou' ? '#93c5fd' : '#64748b',
              padding: '6px 16px', borderRadius: '6px', cursor: 'pointer', fontSize: '13px',
              fontWeight: mode === 'ou' ? 'bold' : 'normal'
            }}
          >
            Over / Under
          </button>
        </div>

        {mode === 'sides' && (
          <div className="qb-filters">
            <div className="qb-filter-group">
              <label className="qb-label">Team</label>
              <select className="qb-select" value={filters.team_abbr} onChange={e => setFilter('team_abbr', e.target.value)}>
                <option value="">Any</option>
                {TEAMS.filter(t => t).map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Home / Away</label>
              <select className="qb-select" value={filters.is_home} onChange={e => setFilter('is_home', e.target.value)}>
                <option value="">Any</option>
                <option value="true">Home</option>
                <option value="false">Away</option>
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Odds</label>
              <select className="qb-select" value={filters.odds_bucket} onChange={e => setFilter('odds_bucket', e.target.value)}>
                {ODDS_BUCKETS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Team Quality</label>
              <select className="qb-select" value={filters.team_bucket} onChange={e => setFilter('team_bucket', e.target.value)}>
                {TEAM_BUCKETS.map(b => <option key={b.value} value={b.value}>{b.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Opponent Quality</label>
              <select className="qb-select" value={filters.opp_bucket} onChange={e => setFilter('opp_bucket', e.target.value)}>
                {TEAM_BUCKETS.map(b => <option key={b.value} value={b.value}>{b.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Game Count</label>
              <select className="qb-select" value={filters.game_count_bucket} onChange={e => setFilter('game_count_bucket', e.target.value)}>
                {GAME_COUNT_BUCKETS.map(b => <option key={b.value} value={b.value}>{b.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Streak Direction</label>
              <select className="qb-select" value={filters.streak_direction} onChange={e => setFilter('streak_direction', e.target.value)}>
                <option value="">Any</option>
                <option value="W">Win Streak</option>
                <option value="L">Losing Streak</option>
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Streak Length</label>
              <select className="qb-select" value={filters.streak_entering} onChange={e => setFilter('streak_entering', e.target.value)} disabled={!filters.streak_direction}>
                {STREAK_VALUES.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Rest</label>
              <select className="qb-select" value={filters.rest} onChange={e => setFilter('rest', e.target.value)}>
                <option value="">Any</option>
                <option value="b2b">Back to Back</option>
                <option value="rest">Had Day Off</option>
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Division Game</label>
              <select className="qb-select" value={filters.division_game} onChange={e => setFilter('division_game', e.target.value)}>
                <option value="">Any</option>
                <option value="true">Yes</option>
                <option value="false">No</option>
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Interleague</label>
              <select className="qb-select" value={filters.interleague} onChange={e => setFilter('interleague', e.target.value)}>
                <option value="">Any</option>
                <option value="true">Yes</option>
                <option value="false">No</option>
              </select>
            </div>
          </div>
        )}

        {mode === 'ou' && (
          <div className="qb-filters">
            <div className="qb-filter-group">
              <label className="qb-label">Team</label>
              <select className="qb-select" value={ouFilters.team_abbr} onChange={e => setOuFilter('team_abbr', e.target.value)}>
                <option value="">Any</option>
                {TEAMS.filter(t => t).map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Home / Away</label>
              <select className="qb-select" value={ouFilters.is_home} onChange={e => setOuFilter('is_home', e.target.value)}>
                <option value="">Any</option>
                <option value="true">Home</option>
                <option value="false">Away</option>
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Total Line</label>
              <select className="qb-select" value={ouFilters.total_bucket} onChange={e => setOuFilter('total_bucket', e.target.value)}>
                {TOTAL_BUCKETS.map(b => <option key={b.value} value={b.value}>{b.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Team L10 Offense</label>
              <select className="qb-select" value={ouFilters.home_l10_scored} onChange={e => setOuFilter('home_l10_scored', e.target.value)}>
                {L10_RUN_BUCKETS.map(b => <option key={b.value} value={b.value}>{b.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Opponent L10 Offense</label>
              <select className="qb-select" value={ouFilters.away_l10_scored} onChange={e => setOuFilter('away_l10_scored', e.target.value)}>
                {L10_RUN_BUCKETS.map(b => <option key={b.value} value={b.value}>{b.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Team Quality</label>
              <select className="qb-select" value={ouFilters.team_bucket} onChange={e => setOuFilter('team_bucket', e.target.value)}>
                {TEAM_BUCKETS.map(b => <option key={b.value} value={b.value}>{b.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Opponent Quality</label>
              <select className="qb-select" value={ouFilters.opp_bucket} onChange={e => setOuFilter('opp_bucket', e.target.value)}>
                {TEAM_BUCKETS.map(b => <option key={b.value} value={b.value}>{b.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Streak Direction</label>
              <select className="qb-select" value={ouFilters.streak_direction} onChange={e => setOuFilter('streak_direction', e.target.value)}>
                <option value="">Any</option>
                <option value="W">Win Streak</option>
                <option value="L">Losing Streak</option>
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Streak Length</label>
              <select className="qb-select" value={ouFilters.streak_entering} onChange={e => setOuFilter('streak_entering', e.target.value)} disabled={!ouFilters.streak_direction}>
                {STREAK_VALUES.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
              </select>
            </div>
            <div className="qb-filter-group">
              <label className="qb-label">Division Game</label>
              <select className="qb-select" value={ouFilters.division_game} onChange={e => setOuFilter('division_game', e.target.value)}>
                <option value="">Any</option>
                <option value="true">Yes</option>
                <option value="false">No</option>
              </select>
            </div>
          </div>
        )}

        <div className="qb-actions">
          <button className="qb-btn-primary" onClick={runQuery} disabled={loading}>
            {loading ? 'Running...' : 'Run Query'}
          </button>
          <button className="qb-btn-secondary" onClick={resetFilters}>Reset</button>
          {result && mode === 'sides' && (
            <button
              onClick={copyShareLink}
              style={{
                background: copied ? 'rgba(74,222,128,0.12)' : 'none',
                border: `1px solid ${copied ? '#4ade80' : '#2a2f3e'}`,
                color: copied ? '#4ade80' : '#64748b',
                padding: '6px 14px', borderRadius: '6px', cursor: 'pointer', fontSize: '12px',
              }}
            >
              {copied ? '✓ Copied' : '🔗 Share'}
            </button>
          )}
        </div>

        {error && <div className="qb-error">{error}</div>}

        {result && result.mode === 'sides' && (
          <div className="qb-result" style={{
            border: hasEdge ? '1px solid #4caf50' : '1px solid #2a2f3e',
            borderRadius: '10px', padding: '16px',
            background: hasEdge ? 'rgba(76,175,80,0.04)' : 'transparent',
          }}>
            {result.message ? (
              <p className="qb-no-results">{result.message}</p>
            ) : (
              <>
                {hasEdge && (
                  <div style={{
                    display: 'inline-flex', alignItems: 'center', gap: '6px',
                    background: 'rgba(76,175,80,0.12)', border: '1px solid #4caf50',
                    borderRadius: '6px', padding: '4px 10px', marginBottom: '12px',
                    fontSize: '11px', fontWeight: 'bold', color: '#4caf50', letterSpacing: '0.06em'
                  }}>
                    ⚡ STRONG PATTERN — above historical baseline
                  </div>
                )}
                <div className="qb-result-main">
                  <div className="qb-stat">
                    <span className="qb-stat-value">{result.wins}-{result.losses}</span>
                    <span className="qb-stat-label">All-Time Record</span>
                  </div>
                  <div className="qb-stat">
                    <span className="qb-stat-value" style={{ color: deviationColor(result.deviation ?? 0) }}>
                      {result.win_pct != null ? (result.win_pct * 100).toFixed(1) + "%" : "—"}
                    </span>
                    <span className="qb-stat-label">Win Rate</span>
                  </div>
                  <div className="qb-stat">
                    <span className="qb-stat-value">{result.n}</span>
                    <span className="qb-stat-label">Sample Size</span>
                  </div>
                  <div className="qb-stat">
                    <span className="qb-stat-value" style={{ color: deviationColor(result.deviation ?? 0) }}>
                      {result.deviation != null ? (result.deviation * 100).toFixed(1) + "%" : "—"}
                    </span>
                    <span className="qb-stat-label">Deviation</span>
                  </div>
                  {result.implied_prob != null && (
                    <div className="qb-stat">
                      <span className="qb-stat-value" style={{
                        color: result.value_gap != null && result.value_gap > 0 ? '#4caf50' : '#ef4444'
                      }}>
                        {result.implied_prob != null ? (result.implied_prob * 100).toFixed(1) + "%" : "—"}
                      </span>
                      <span className="qb-stat-label">Mkt Implied</span>
                    </div>
                  )}
                  {result.value_gap != null && (
                    <div className="qb-stat">
                      <span className="qb-stat-value" style={{
                        color: result.value_gap > 0 ? '#4caf50' : '#ef4444'
                      }}>
                        {result.value_gap > 0 ? '+' : ''}{(result.value_gap * 100).toFixed(1)}%
                      </span>
                      <span className="qb-stat-label">Value Gap</span>
                    </div>
                  )}
                </div>

                {/* 2026 season record */}
                {result.season2026 && (
                  <div style={{
                    marginTop: '12px', padding: '8px 12px',
                    background: '#1a1f2e', borderRadius: '6px',
                    display: 'flex', alignItems: 'center', gap: '16px',
                    fontSize: '12px', color: '#64748b'
                  }}>
                    <span style={{ fontWeight: 'bold', color: '#93c5fd', fontSize: '11px', letterSpacing: '0.05em' }}>2026 SEASON</span>
                    <span style={{ color: '#e2e8f0' }}>{result.season2026.wins}-{result.season2026.losses}</span>
                    <span style={{ color: deviationColor(result.season2026.deviation ?? 0) }}>
                      {result.season2026.win_pct != null ? (result.season2026.win_pct * 100).toFixed(1) + "% win rate" : ""}
                    </span>
                    <span>n={result.season2026.n}</span>
                  </div>
                )}

                {result.sample_warning && (
                  <p className="qb-warning">⚠️ Small sample size — interpret with caution</p>
                )}
                {result.sample_games && result.sample_games.length > 0 && (
                  <div style={{ marginTop: '16px' }}>
                    <div style={{ color: '#64748b', fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '8px' }}>Recent Matching Games</div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0 12px', fontSize: '11px', color: '#475569', marginBottom: '2px' }}>
                        <span style={{ minWidth: '85px' }}>Date</span>
                        <span style={{ minWidth: '40px' }}>Team</span>
                        <span>Matchup</span>
                        <span style={{ minWidth: '45px', textAlign: 'right' }}>ML</span>
                        <span style={{ minWidth: '20px', textAlign: 'right' }}>Result</span>
                      </div>
                      {result.sample_games.map((g: any, i: number) => (
                        <div key={i} style={{
                          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                          background: '#1a1f2e', borderRadius: '6px', padding: '6px 12px',
                          fontSize: '12px', color: '#94a3b8',
                          borderLeft: g.team_won ? '3px solid #4caf50' : '3px solid #ef4444',
                        }}>
                          <span style={{ color: '#64748b', minWidth: '85px' }}>{g.game_date}</span>
                          <span style={{ fontWeight: 'bold', minWidth: '40px' }}>{g.team}</span>
                          <span style={{ color: '#64748b' }}>{g.is_home ? 'vs' : '@'} {g.opponent}</span>
                          <span style={{ minWidth: '45px', textAlign: 'right', color: '#64748b' }}>
                            {g.moneyline ? (g.moneyline >= 2.0 ? '+' + Math.round((g.moneyline - 1) * 100) : String(Math.round(-(100 / (g.moneyline - 1))))) : '-'}
                          </span>
                          <span style={{ fontWeight: 'bold', color: g.team_won ? '#4caf50' : '#ef4444', minWidth: '20px', textAlign: 'right' }}>
                            {g.team_won ? 'W' : 'L'}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        )}

        {result && result.mode === 'ou' && (
          <div className="qb-result">
            {result.message ? (
              <p className="qb-no-results">{result.message}</p>
            ) : (
              <>
                <div className="qb-result-main">
                  <div className="qb-stat">
                    <span className="qb-stat-value" style={{ color: result.over_pct != null ? ouColor(result.over_pct) : '#94a3b8' }}>
                      {result.over_pct != null ? (result.over_pct * 100).toFixed(1) + "%" : "—"}
                    </span>
                    <span className="qb-stat-label">Over ({result.over})</span>
                  </div>
                  <div className="qb-stat">
                    <span className="qb-stat-value" style={{ color: result.under_pct != null ? ouColor(result.under_pct) : '#94a3b8' }}>
                      {result.under_pct != null ? (result.under_pct * 100).toFixed(1) + "%" : "—"}
                    </span>
                    <span className="qb-stat-label">Under ({result.under})</span>
                  </div>
                  <div className="qb-stat">
                    <span className="qb-stat-value">{result.n}</span>
                    <span className="qb-stat-label">Sample Size</span>
                  </div>
                  <div className="qb-stat">
                    <span className="qb-stat-value" style={{ color: '#64748b' }}>{result.push}</span>
                    <span className="qb-stat-label">Pushes</span>
                  </div>
                </div>
                {result.sample_warning && (
                  <p className="qb-warning">⚠️ Small sample size — interpret with caution</p>
                )}
                {result.sample_games && result.sample_games.length > 0 && (
                  <div style={{ marginTop: '16px' }}>
                    <div style={{ color: '#64748b', fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '8px' }}>Recent Matching Games</div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0 12px', fontSize: '11px', color: '#475569', marginBottom: '2px' }}>
                        <span style={{ minWidth: '85px' }}>Date</span>
                        <span style={{ minWidth: '40px' }}>Team</span>
                        <span style={{ flex: 1 }}>Matchup</span>
                        <span style={{ minWidth: '50px', textAlign: 'right' }}>Total</span>
                        <span style={{ minWidth: '40px', textAlign: 'right' }}>Runs</span>
                        <span style={{ minWidth: '30px', textAlign: 'right' }}>O/U</span>
                      </div>
                      {result.sample_games.map((g: any, i: number) => (
                        <div key={i} style={{
                          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                          background: '#1a1f2e', borderRadius: '6px', padding: '6px 12px',
                          fontSize: '12px', color: '#94a3b8',
                          borderLeft: g.result === 'Over' ? '3px solid #4caf50' : g.result === 'Under' ? '3px solid #ef4444' : '3px solid #64748b',
                        }}>
                          <span style={{ color: '#64748b', minWidth: '85px' }}>{g.game_date}</span>
                          <span style={{ fontWeight: 'bold', minWidth: '40px' }}>{g.team}</span>
                          <span style={{ color: '#64748b', flex: 1 }}>{g.is_home ? 'vs' : '@'} {g.opponent}</span>
                          <span style={{ minWidth: '50px', textAlign: 'right', color: '#64748b' }}>{g.total_line ?? '-'}</span>
                          <span style={{ minWidth: '40px', textAlign: 'right', color: '#64748b' }}>{g.total_runs ?? '-'}</span>
                          <span style={{ fontWeight: 'bold', minWidth: '30px', textAlign: 'right', color: g.result === 'Over' ? '#4caf50' : g.result === 'Under' ? '#ef4444' : '#64748b' }}>
                            {g.result === 'Over' ? 'O' : g.result === 'Under' ? 'U' : 'P'}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        )}

      </div>
    </div>
  )
}
