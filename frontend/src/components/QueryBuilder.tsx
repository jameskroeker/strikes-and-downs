import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
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
  { value: 'mid_early', label: 'Mid-Early (G21-60)' },
  { value: 'mid', label: 'Mid Season (G61-100)' },
  { value: 'mid_late', label: 'Mid-Late (G101-130)' },
  { value: 'late', label: 'Late Season (G131-162)' },
]

const STREAK_VALUES = [
  { value: '', label: 'Any' },
  ...Array.from({length: 9}, (_, i) => ({ value: String(i + 1), label: String(i + 1) })),
  { value: '10', label: '10+' },
]

const TEAMS = [
  '', 'ARI', 'ATL', 'ATH', 'BAL', 'BOS', 'CHC', 'CIN', 'CLE', 'COL', 'CWS',
  'DET', 'HOU', 'KCR', 'LAA', 'LAD', 'MIA', 'MIL', 'MIN', 'NYM', 'NYY',
  'PHI', 'PIT', 'SDP', 'SEA', 'SFG', 'STL', 'TBR', 'TEX', 'TOR', 'WSH'
]

const API_BASE = import.meta.env.VITE_API_URL || 'https://strikes-and-downs.onrender.com'

function deviationColor(dev: number): string {
  if (dev >= 0.25) return '#4caf50'
  if (dev >= 0.15) return '#fbbf24'
  return '#94a3b8'
}

export function QueryBuilder() {
  const navigate = useNavigate()
  const [filters, setFilters] = useState({
    team_abbr: '', is_home: '', odds_bucket: '', team_bucket: '',
    opp_bucket: '', game_count_bucket: '', streak_direction: '',
    streak_entering: '', rest: '', division_game: '', interleague: '',
  })
  const [result, setResult] = useState<any>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  function setFilter(key: string, value: string) {
    setFilters(f => ({ ...f, [key]: value }))
  }

  async function runQuery() {
    setLoading(true)
    setError(null)
    setResult(null)
    const params = new URLSearchParams()
    Object.entries(filters).forEach(([k, v]) => { if (v !== '') params.append(k, v) })
    try {
      const res = await fetch(`${API_BASE}/api/query?${params.toString()}`)
      const data = await res.json()
      setResult(data)
    } catch {
      setError('Failed to fetch results')
    } finally {
      setLoading(false)
    }
  }

  function resetFilters() {
    setFilters({ team_abbr: '', is_home: '', odds_bucket: '', team_bucket: '',
      opp_bucket: '', game_count_bucket: '', streak_direction: '',
      streak_entering: '', rest: '', division_game: '', interleague: '' })
    setResult(null)
    setError(null)
  }

  return (
    <div className="app">
      <header className="header">
        <a href="/"><img src="/logo.png" alt="Strikes + Downs" style={{ width: '67%', maxWidth: '300px', display: 'block', margin: '0 auto' }} /></a>
      </header>
      <div className="qb-nav">
        <button className="qb-nav-btn" onClick={() => navigate('/')}>← Back to Games</button>
      </div>
      <div className="qb-container">
        <h2 className="qb-title">Query Builder</h2>
        <p className="qb-subtitle">Define conditions and see how teams have historically performed</p>
        <div className="qb-filters">
          <div className="qb-filter-group">
            <label className="qb-label">Team</label>
            <select className="qb-select" value={filters.team_abbr} onChange={e => setFilter('team_abbr', e.target.value)}>
              <option value="">All Teams</option>
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
        <div className="qb-actions">
          <button className="qb-btn-primary" onClick={runQuery} disabled={loading}>
            {loading ? 'Running...' : 'Run Query'}
          </button>
          <button className="qb-btn-secondary" onClick={resetFilters}>Reset</button>
        </div>
        {error && <div className="qb-error">{error}</div>}
        {result && (
          <div className="qb-result">
            {result.message ? (
              <p className="qb-no-results">{result.message}</p>
            ) : (
              <>
                <div className="qb-result-main">
                  <div className="qb-stat">
                    <span className="qb-stat-value">{result.wins}-{result.losses}</span>
                    <span className="qb-stat-label">Record</span>
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
                </div>
                {result.sample_warning && (
                  <p className="qb-warning">⚠️ Small sample size — interpret with caution</p>
                )}
              </>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
