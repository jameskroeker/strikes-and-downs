import { useEffect, useState } from 'react'
import { BrowserRouter, Routes, Route, useNavigate, useSearchParams } from 'react-router-dom'
import { fetchGamesForDate, fetchSignalsForDate } from './api'
import type { Game } from './types'
import { GameCard } from './components/GameCard'
import { GameDetail } from './components/GameDetail'
import { QueryBuilder } from './components/QueryBuilder'
import { Methodology } from './components/Methodology'
import './App.css'

function todayStr(): string {
  // Use ET (UTC-4 during EDT) so the app doesn't flip to tomorrow after 8 PM MT
  const now = new Date()
  const etOffset = -4 * 60 // EDT = UTC-4
  const etTime = new Date(now.getTime() + (etOffset - now.getTimezoneOffset()) * 60000)
  return etTime.toISOString().slice(0, 10)
}

function offsetDate(dateStr: string, days: number): string {
  const d = new Date(dateStr + 'T12:00:00')
  d.setDate(d.getDate() + days)
  return d.toISOString().slice(0, 10)
}

function GamesList() {
  const [selectedDate, setSelectedDate] = useState(todayStr())
  const [games, setGames] = useState<Game[]>([])
  const [signals, setSignals] = useState<Record<string, any>>({})
  const [accuracy, setAccuracy] = useState<any>(null)
  const [displayDate, setDisplayDate] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const isAdmin = searchParams.get('admin') === 'true'

  useEffect(() => {
    setLoading(true)
    setError(null)
    const API_BASE = `${import.meta.env.VITE_API_URL}/api`
    Promise.all([
      fetchGamesForDate(selectedDate),
      fetchSignalsForDate(selectedDate),
      fetch(`${API_BASE}/signals/accuracy`).then(r => r.json()).catch(() => null)
    ])
      .then(([gamesData, signalsData, accuracyData]) => {
        setGames(gamesData.games)
        setDisplayDate(gamesData.date)
        setSignals(signalsData)
        setAccuracy(accuracyData)
        setLoading(false)
      })
      .catch((err: Error) => {
        setError(err.message)
        setLoading(false)
      })
  }, [selectedDate])

  return (
    <div className="app">
      <header className="header" style={{ padding: 0, margin: 0 }}>
        <a href="/"><img src="/logo.png" alt="Strikes + Downs" style={{ width: '67%', maxWidth: '300px', display: 'block', margin: '0 auto' }} /></a>
        {isAdmin && accuracy && accuracy.total > 0 && (
          <div style={{ textAlign: 'center', marginTop: '6px' }}>
            <span style={{
              fontSize: '12px', color: '#4caf50', background: 'rgba(76,175,80,0.08)',
              border: '1px solid rgba(76,175,80,0.25)', borderRadius: '6px',
              padding: '3px 10px', letterSpacing: '0.04em'
            }}>
              T1 {accuracy.wins}-{accuracy.losses} &nbsp;·&nbsp; {Math.round(accuracy.win_pct * 100)}% &nbsp;·&nbsp; since {accuracy.since?.slice(5).replace('-', '/')}
            </span>
          </div>
        )}
        <p className="subtitle">MLB Betting Analytics | 2026 Season</p>
      </header>

      <div className="date-nav">
        <button onClick={() => setSelectedDate((d) => offsetDate(d, -1))}>&#8592;</button>
        <input
          type="date"
          value={selectedDate}
          onChange={(e) => setSelectedDate(e.target.value)}
        />
        <button onClick={() => setSelectedDate((d) => offsetDate(d, 1))}>&#8594;</button>
      </div>

      {loading && <div className="loading">Loading games for {selectedDate}…</div>}
      {error && <div className="error">{error}</div>}
      {!loading && !error && (
        <>
          {games.length === 0 ? (
            <div className="no-games">No games found for {displayDate}.</div>
          ) : (
            <>
              <div style={{ display: 'flex', justifyContent: 'flex-end', padding: '0 0 8px 0' }}>
                <a href="/query" style={{ color: '#64748b', fontSize: '13px', textDecoration: 'none', border: '1px solid #2a2f3e', padding: '4px 12px', borderRadius: '6px' }}>Query Builder →</a>
              </div>
              <main className="games-grid">
              {games.map((game) => (
                <div
                  key={game.game_id}
                  onClick={() => navigate(`/game/${game.game_id}?date=${selectedDate}`)}
                  style={{ cursor: 'pointer' }}
                >
                  <GameCard game={game} signal={signals[game.game_id]} />
                </div>
              ))}
              </main>
            </>
          )}
        </>
      )}
    </div>
  )
}

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<GamesList />} />
        <Route path="/game/:gameId" element={<GameDetail />} />
        <Route path="/query" element={<QueryBuilder />} />
        <Route path="/methodology" element={<Methodology />} />
      </Routes>
    </BrowserRouter>
  )
}

export default App
