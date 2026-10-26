import { useCallback, useEffect, useState } from 'react';
import { api, type Stats, type TopZones, type ZoneSeries } from './api';

const WINDOWS = [
  { minutes: 15, label: '15 min' },
  { minutes: 60, label: '1 hour' },
  { minutes: 180, label: '3 hours' },
  { minutes: 1440, label: '24 hours' },
];
const POLL_MS = 5000;

const nyTime = (iso: string | null | undefined) =>
  iso
    ? new Date(iso).toLocaleString('en-US', {
        timeZone: 'America/New_York',
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
      })
    : '—';

export default function App() {
  const [windowMinutes, setWindowMinutes] = useState(60);
  const [top, setTop] = useState<TopZones | null>(null);
  const [cache, setCache] = useState<string | null>(null);
  const [stats, setStats] = useState<Stats | null>(null);
  const [selected, setSelected] = useState<number | null>(null);
  const [series, setSeries] = useState<ZoneSeries | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [t, s] = await Promise.all([api.topZones(10, windowMinutes), api.stats()]);
      setTop(t.body);
      setCache(t.cache);
      setStats(s.body);
      setError(null);
      const zone = selected ?? t.body.zones[0]?.zoneId ?? null;
      if (zone !== null) {
        setSeries((await api.zoneSeries(zone, Math.max(windowMinutes, 180))).body);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [windowMinutes, selected]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, POLL_MS);
    return () => clearInterval(id);
  }, [refresh]);

  const max = Math.max(1, ...(top?.zones.map((z) => z.trips) ?? [1]));
  const activeZone = selected ?? top?.zones[0]?.zoneId ?? null;

  return (
    <main>
      <header>
        <div>
          <h1>NYC Taxi Live Demand</h1>
          <p className="muted">
            Trips replayed from TLC records through Kafka. Stream time: <strong>{nyTime(stats?.watermark)}</strong> (New York)
          </p>
        </div>
        <dl className="stats">
          <div>
            <dt>Trips counted</dt>
            <dd>{stats ? stats.tripsApplied.toLocaleString() : '—'}</dd>
          </div>
          <div>
            <dt>Duplicates skipped</dt>
            <dd>{stats ? stats.duplicatesSkipped.toLocaleString() : '—'}</dd>
          </div>
        </dl>
      </header>

      {error && <p className="error">API unavailable: {error}</p>}

      <section className="panel">
        <div className="panel-head">
          <h2>Busiest pickup zones</h2>
          <div className="segmented" role="tablist" aria-label="Window">
            {WINDOWS.map((w) => (
              <button
                key={w.minutes}
                role="tab"
                aria-selected={w.minutes === windowMinutes}
                onClick={() => setWindowMinutes(w.minutes)}
              >
                {w.label}
              </button>
            ))}
          </div>
        </div>
        <p className="muted small">
          {nyTime(top?.windowStart)} – {nyTime(top?.windowEnd)}
          {cache && <span className={`badge ${cache === 'HIT' ? 'hit' : 'miss'}`}>cache {cache.toLowerCase()}</span>}
        </p>
        <ol className="ranking">
          {top?.zones.map((z) => (
            <li key={z.zoneId}>
              <button
                className={z.zoneId === activeZone ? 'row active' : 'row'}
                onClick={() => setSelected(z.zoneId)}
              >
                <span className="rank">{z.rank}</span>
                <span className="name">
                  {z.zone}
                  <span className="muted small"> · {z.borough}</span>
                </span>
                <span className="bar">
                  <span style={{ width: `${(z.trips / max) * 100}%` }} />
                </span>
                <span className="count">{z.trips.toLocaleString()}</span>
              </button>
            </li>
          ))}
          {top && top.zones.length === 0 && <p className="muted">No trips in this window yet. Start the replayer.</p>}
        </ol>
      </section>

      {series && (
        <section className="panel">
          <h2>
            {series.zone} <span className="muted small">trips per 5 minutes</span>
          </h2>
          <Sparkline series={series} />
        </section>
      )}
    </main>
  );
}

function Sparkline({ series }: { series: ZoneSeries }) {
  const width = 720;
  const height = 120;
  const start = new Date(series.windowStart).getTime();
  const end = new Date(series.windowEnd).getTime();
  const step = 5 * 60 * 1000;
  const byTime = new Map(series.buckets.map((b) => [new Date(b.bucketStart).getTime(), b.trips]));
  const points: number[] = [];
  for (let t = start; t < end; t += step) {
    points.push(byTime.get(t) ?? 0);
  }
  const max = Math.max(1, ...points);
  const barWidth = width / Math.max(points.length, 1);

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="sparkline" role="img" aria-label={`${series.zone} demand`}>
      {points.map((v, i) => {
        const h = (v / max) * (height - 4);
        return (
          <rect key={i} x={i * barWidth + 1} y={height - h} width={Math.max(barWidth - 2, 1)} height={h} rx={1.5}>
            <title>{`${nyTime(new Date(start + i * step).toISOString())}: ${v} trips`}</title>
          </rect>
        );
      })}
    </svg>
  );
}
