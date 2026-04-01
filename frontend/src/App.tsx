import { useEffect, useState } from 'react'
import { fetchGamesForDate } from './api'
import type { Game } from './types'
import { GameCard } from './components/GameCard'
import './App.css'

function todayStr(): string {
  return new Date().toISOString().slice(0, 10)
}

function App() {
  const [selectedDate, setSelectedDate] = useState(todayStr())
  const [games, setGames] = useState<Game[]>([])
  const [displayDate, setDisplayDate] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetchGamesForDate(selectedDate)
      .then((data) => {
        setGames(data.games)
        setDisplayDate(data.date)
        setLoading(false)
      })
      .catch((err: Error) => {
        setError(err.message)
        setLoading(false)
      })
  }, [selectedDate])

  return (
    <div className="app">
      <header className="header">
        <h1><span className="logo-icon">⚾</span> Strikes &amp; Downs</h1>
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
            <main className="games-grid">
              {games.map((game) => (
                <GameCard key={game.game_id} game={game} />
              ))}
            </main>
          )}
        </>
      )}
    </div>
  )
}

function offsetDate(dateStr: string, days: number): string {
  const d = new Date(dateStr + 'T12:00:00')
  d.setDate(d.getDate() + days)
  return d.toISOString().slice(0, 10)
}

export default App
