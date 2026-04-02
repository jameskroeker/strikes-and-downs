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
