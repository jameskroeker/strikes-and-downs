import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'

const API_BASE = import.meta.env.VITE_API_URL

type Sport = {
  key: string
  emoji: string
  name: string
  blurb: string
  primary: { label: string; to: string }
  secondary?: { label: string; to: string }
  note?: string
}

function SportCard({ s }: { s: Sport }) {
  const [hover, setHover] = useState(false)
  return (
    <div
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        background: '#161b27', border: `1px solid ${hover ? '#3a9e6a' : '#2a2f3e'}`, borderRadius: '12px',
        padding: '24px', display: 'flex', flexDirection: 'column', gap: '12px', transition: 'border-color 0.15s',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
        <span style={{ fontSize: '28px', lineHeight: 1 }}>{s.emoji}</span>
        <h2 style={{ margin: 0, fontSize: '1.5rem', fontWeight: 800, color: '#f0f4ff' }}>{s.name}</h2>
      </div>
      <p style={{ margin: 0, color: '#94a3b8', fontSize: '14px', lineHeight: 1.5, flex: 1 }}>{s.blurb}</p>
      {s.note && <p style={{ margin: 0, color: '#64748b', fontSize: '12px' }}>{s.note}</p>}
      <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap', marginTop: '4px' }}>
        <Link to={s.primary.to} style={{
          background: '#1e40af', color: '#fff', textDecoration: 'none', padding: '10px 18px',
          borderRadius: '8px', fontSize: '14px', fontWeight: 600,
        }}>{s.primary.label}</Link>
        {s.secondary && (
          <Link to={s.secondary.to} style={{
            border: '1px solid #2a2f3e', color: '#94a3b8', textDecoration: 'none', padding: '10px 18px',
            borderRadius: '8px', fontSize: '14px',
          }}>{s.secondary.label}</Link>
        )}
      </div>
    </div>
  )
}

export function Home() {
  const [nflThrough, setNflThrough] = useState<string | null>(null)

  useEffect(() => {
    fetch(`${API_BASE}/api/nfl/meta`)
      .then(r => r.json())
      .then(m => m?.data_through && setNflThrough(`${m.data_through.season} ${m.data_through.week.replace('W', 'Week ')}`))
      .catch(() => {})
  }, [])

  const sports: Sport[] = [
    {
      key: 'mlb', emoji: '⚾', name: 'MLB',
      blurb: "Today's games with team context and situational signals, plus a query builder for how teams have performed in any situation.",
      primary: { label: "Today's games →", to: '/mlb' },
      secondary: { label: 'Query builder', to: '/query' },
    },
    {
      key: 'nfl', emoji: '🏈', name: 'NFL',
      blurb: 'Query builder for spreads, totals and situations: rest, road trips, last week\'s result, early-season records and more. Every result shows its sample size and how it held up season by season.',
      primary: { label: 'NFL query builder →', to: '/nfl' },
      note: nflThrough ? `2022–present, data through ${nflThrough}` : '2022–present',
    },
  ]

  return (
    <div className="app">
      <header className="header" style={{ padding: 0, margin: 0 }}>
        <img src="/logo.png" alt="Strikes + Downs" style={{ width: '67%', maxWidth: '300px', display: 'block', margin: '0 auto' }} />
        <p className="subtitle" style={{ fontSize: '0.95rem', color: '#94a3b8', marginTop: '8px' }}>
          Historical situational research for bettors. Build your own conviction.
        </p>
      </header>
      <main style={{
        display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '16px',
        maxWidth: '880px', margin: '28px auto 0',
      }}>
        {sports.map(s => <SportCard key={s.key} s={s} />)}
      </main>
    </div>
  )
}
