export interface RankedZone {
  rank: number;
  zoneId: number;
  zone: string;
  borough: string;
  trips: number;
}

export interface TopZones {
  windowStart: string;
  windowEnd: string;
  windowMinutes: number;
  k: number;
  watermark: string | null;
  zones: RankedZone[];
}

export interface ZoneSeries {
  zoneId: number;
  zone: string;
  borough: string;
  windowStart: string;
  windowEnd: string;
  buckets: { bucketStart: string; trips: number }[];
}

export interface Stats {
  watermark: string | null;
  tripsApplied: number;
  duplicatesSkipped: number;
  malformedSkipped: number;
  lateRejected: number;
}

async function get<T>(path: string): Promise<{ body: T; cache: string | null }> {
  const res = await fetch(path);
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`);
  }
  return { body: (await res.json()) as T, cache: res.headers.get('X-Cache') };
}

export const api = {
  topZones: (k: number, windowMinutes: number) =>
    get<TopZones>(`/api/v1/zones/top?k=${k}&windowMinutes=${windowMinutes}`),
  zoneSeries: (zoneId: number, windowMinutes: number) =>
    get<ZoneSeries>(`/api/v1/zones/${zoneId}/demand?windowMinutes=${windowMinutes}`),
  stats: () => get<Stats>('/api/v1/stats'),
};
