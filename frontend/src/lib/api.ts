// src/lib/api.ts
// API client to connect frontend with backend_api.py

const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "http://localhost:5002").replace(/\/$/, "");

function buildApiUrl(endpoint: string): string {
  if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
    return endpoint;
  }
  const normalizedEndpoint = endpoint.startsWith("/") ? endpoint : `/${endpoint}`;
  return `${API_BASE}${normalizedEndpoint}`;
}

export interface Flight {
  flight_id: number;
  airline: string;
  departure_city: string;
  departure_airport: string;
  arrival_city: string;
  arrival_airport: string;
  lat: number;
  lon: number;
  dest_lat: number;
  dest_lon: number;
  current_altitude_m: number;
  current_speed_km_h: number;
  speed: number;
  direction: number;
  distance_travelled_km: number;
  flight_status: string;
  scheduled_departure_time: string;
  actual_departure_time: string;
  scheduled_arrival_time: string;
  actual_landed_time: string | null;
  current_location: { latitude: number; longitude: number };
}

export interface PgFlight {
  id: number;
  flight_id: number;
  airline: string;
  flight_status: string;
  departure_city: string;
  arrival_city: string;
  delayed_status: string;
  inserted_at: string;
}

export interface DelayItem {
  delayed_status: string;
  count: number;
}

export interface AirlineItem {
  airline: string;
  flight_count?: number;
  total?: number;
  late?: number;
  late_pct?: number;
}

export interface PipelineStatus {
  mongodb: { status: string; records?: number; error?: string };
  postgresql: { status: string; records?: number; error?: string };
}

async function fetchAPI<T>(endpoint: string): Promise<T | null> {
  try {
    const res = await fetch(buildApiUrl(endpoint));
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    console.warn(`API error [${endpoint}]:`, err);
    return null;
  }
}

// ============================================================
// API FUNCTIONS
// ============================================================

export async function getLiveFlights() {
  return fetchAPI<{
    active_flights: Flight[];
    landed_flights: Flight[];
    active_count: number;
    landed_count: number;
    timestamp: string;
  }>("/api/live-flights");
}

export async function getMongoStats() {
  return fetchAPI<{
    total_records: number;
    active_flights: number;
    landed_flights: number;
    unique_airlines: number;
    airlines: string[];
  }>("/api/mongo-stats");
}

export async function getPgFlights() {
  return fetchAPI<{ flights: PgFlight[]; count: number }>("/api/pg-flights");
}

export async function getDelayDistribution() {
  return fetchAPI<{ distribution: DelayItem[] }>("/api/delay-distribution");
}

export async function getTopAirlines() {
  return fetchAPI<{ airlines: AirlineItem[] }>("/api/top-airlines");
}

export async function getDelayRatio() {
  return fetchAPI<{ airlines: AirlineItem[] }>("/api/delay-ratio");
}

export async function getPipelineStatus() {
  return fetchAPI<{ pipeline: PipelineStatus; timestamp: string }>(
    "/api/pipeline-status"
  );
}
