import { useNavigate } from 'react-router-dom'

export function Methodology() {
  const navigate = useNavigate()

  return (
    <div className="app">
      <header className="header" style={{ padding: 0, margin: 0 }}>
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

      <main style={{ padding: '0 20px 48px', maxWidth: '720px', margin: '0 auto' }}>

        <h2 style={{ color: '#e2e8f0', fontSize: '22px', fontWeight: 'bold', marginBottom: '8px' }}>How It Works</h2>
        <p style={{ color: '#64748b', fontSize: '14px', marginBottom: '32px', lineHeight: 1.6 }}>
          Strikes and Downs surfaces historical betting patterns — not predictions. The goal is to help you build conviction before a game, not tell you what to do.
        </p>

        <Section title="The Signal Engine">
          <p>Every day, the signal engine scores each game by asking: do both teams' historical patterns agree on an outcome? When they do — strongly — that's a T1 signal.</p>
          <p>A T1 signal fires when the consensus score reaches 1.0 or higher. Games where one team is a heavy favorite are excluded — the juice isn't worth it.</p>
          <p>T1 record since April 18, 2026: <span style={{ color: '#4caf50', fontWeight: 'bold' }}>28-9 (76%)</span>. Sharp bettors hit 55-57% long term. We're tracking well above that — but sample size is still small.</p>
        </Section>

        <Section title="Win% Buckets">
          <BucketTable rows={[
            ['Elite', '59%+', 'One of the better teams in baseball'],
            ['Good', '53–58%', 'A winning team'],
            ['Average', '47–52%', 'A .500 team'],
            ['Poor', '41–46%', 'A losing team'],
            ['Bad', '<40%', 'One of the worst teams in baseball'],
          ]} />
          <p style={{ marginTop: '12px' }}>Win% buckets are unreliable before game 20 of the season — early season signals carry more uncertainty.</p>
        </Section>

        <Section title="Odds Buckets">
          <BucketTable rows={[
            ['Heavy Favorite', '-400 to -250', 'Excluded from signals'],
            ['Strong Favorite', '-250 to -200', ''],
            ['Favorite', '-200 to -154', ''],
            ['Mild Favorite', '-154 to -133', ''],
            ['Slight Favorite', '-133 to even', ''],
            ['Pick', 'Even to +110', ''],
            ['Slight Underdog', '+110 to +130', ''],
            ['Underdog', '+130 to +150', ''],
            ['Clear Underdog', '+150 to +225', ''],
            ['Big Underdog', '+225+', ''],
          ]} />
        </Section>

        <Section title="Situation Cards">
          <p>Each game detail page shows historical situations for both teams. A situation is a specific combination of conditions — for example: "road team as a slight favorite against a bad opponent."</p>
          <p>The win bar shows historical win rate. The tick mark shows where the odds-implied probability sits. When the two diverge significantly, you'll see a value gap badge.</p>
          <p>Each card shows sample size (n). Low n means less confidence — use it as context, not gospel.</p>
        </Section>

        <Section title="Data Sources">
          <p>Game and odds data via API-Sports. Primary odds source: Pinnacle (the sharpest book in the world). Fallback: Marathon. Historical data covers 2022–2025 regular season.</p>
          <p>Odds are locked at 8PM ET each day — signals reflect pre-game lines, not live or closing lines.</p>
        </Section>

        <Section title="What This Is Not">
          <p>This is not a picks service. No one is telling you what to bet. The data is here to help you think — to confirm or challenge a hypothesis you already have.</p>
          <p>Past patterns don't guarantee future results. Treat every signal as one input among many, not a guaranteed edge.</p>
        </Section>

        <div style={{ color: '#374151', fontSize: '11px', textAlign: 'center', borderTop: '1px solid #1a1f2e', paddingTop: '16px', marginTop: '32px' }}>
          More detail coming soon · Questions? Methodology is always evolving
        </div>
      </main>
    </div>
  )
}

function Section({ title, children }: { title: string, children: React.ReactNode }) {
  return (
    <div style={{ marginBottom: '32px' }}>
      <h3 style={{ color: '#93c5fd', fontSize: '13px', fontWeight: 'bold', letterSpacing: '0.08em', marginBottom: '12px', textTransform: 'uppercase' }}>
        {title}
      </h3>
      <div style={{ color: '#94a3b8', fontSize: '14px', lineHeight: 1.7, display: 'flex', flexDirection: 'column', gap: '10px' }}>
        {children}
      </div>
    </div>
  )
}

function BucketTable({ rows }: { rows: string[][] }) {
  return (
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
      <tbody>
        {rows.map(([label, range, desc], i) => (
          <tr key={i} style={{ borderBottom: '1px solid #1a1f2e' }}>
            <td style={{ padding: '8px 12px 8px 0', color: '#e2e8f0', fontWeight: 'bold', whiteSpace: 'nowrap' }}>{label}</td>
            <td style={{ padding: '8px 12px', color: '#64748b', whiteSpace: 'nowrap' }}>{range}</td>
            <td style={{ padding: '8px 0', color: '#94a3b8' }}>{desc}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
