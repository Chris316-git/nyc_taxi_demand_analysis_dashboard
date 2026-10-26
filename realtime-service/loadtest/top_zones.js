// k6 load test for the read path.
//   k6 run loadtest/top_zones.js                     # MODE=live: dashboards polling the current window (cache hits)
//   k6 run -e MODE=miss loadtest/top_zones.js        # random historical windows: almost every request misses
//                                                    # the cache and aggregates in Postgres
//   k6 run -e BASE_URL=http://host:8080 -e RATE=300 loadtest/top_zones.js
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const WINDOWS = [15, 60, 180, 1440];
const MODE = __ENV.MODE || 'live';
// Replayed month: January 2025 (UTC). Used to pick random historical "asOf" times in miss mode.
const MONTH_START = Date.parse('2025-01-02T00:00:00Z');
const MONTH_SPAN_MS = 29 * 24 * 3600 * 1000;

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 200),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<250'],
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const window = WINDOWS[Math.floor(Math.random() * WINDOWS.length)];
  let url = `${BASE}/api/v1/zones/top?k=10&windowMinutes=${window}`;
  if (MODE === 'miss') {
    const asOf = new Date(MONTH_START + Math.floor(Math.random() * MONTH_SPAN_MS)).toISOString();
    url += `&asOf=${asOf}`;
  }
  const res = http.get(url, { tags: { window: String(window) } });
  check(res, {
    'status 200': (r) => r.status === 200,
    'cache header': (r) => r.headers['X-Cache'] !== undefined,
    'has zones': (r) => r.json('zones') !== undefined,
  });
}
