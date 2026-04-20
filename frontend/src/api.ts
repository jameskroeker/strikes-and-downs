import type { GamesResponse } from './types'

const API_BASE = `${import.meta.env.VITE_API_URL}/api`

export async function fetchTodayGames(): Promise<GamesResponse> {
  const res = await fetch(`${API_BASE}/games/today`)
  if (!res.ok) throw new Error(`Failed to fetch games: ${res.statusText}`)
  return res.json()
}

export async function fetchGamesForDate(date: string): Promise<GamesResponse> {
  const res = await fetch(`${API_BASE}/games/${date}`)
  if (!res.ok) throw new Error(`No game data for ${date}`)
  return res.json()
}

export async function fetchSignalsForDate(date: string): Promise<Record<string, any>> {
  const res = await fetch(`${API_BASE}/signals/${date}`)
  if (!res.ok) return {}
  const data = await res.json()
  // Return a map of game_id -> signal for easy lookup
  const map: Record<string, any> = {}
  for (const g of data.signals || []) {
    if (g.tier > 0) map[g.game_id] = { ...g.signal, tier: g.tier, signal_team: g.signal_team }
  }
  return map
}
