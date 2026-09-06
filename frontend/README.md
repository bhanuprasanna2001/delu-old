# DELU website

React + TypeScript, scaffolded with `npx create-vite@latest`, with Tailwind's
Vite plugin, Oxlint, and Recharts. Node.js 24 or newer is required.

```bash
npm ci
npm run dev
```

Vite runs at <http://127.0.0.1:5173> and proxies `/api` to the existing FastAPI
server at <http://127.0.0.1:8000>. See the repository README for backend setup.
The frontend uses real API data; there is no demo-data fallback.

```bash
npm run lint
npm run build
npx playwright install chromium
npm test
```

The build writes to `../app/static`. FastAPI serves those assets and the API
from one authenticated Databricks app. Build before starting FastAPI or
uploading the Databricks bundle. Generated assets and dependencies are ignored
by Git; the bundle explicitly includes the built assets.

The Playwright suite runs desktop and mobile flows with synthetic HTTP fixtures
in `tests/` only. It covers date gaps, historical observations, quarter-hour
inspection, settlement refresh, downloads, input pagination, and error states.

Dates and the detailed view are linkable using `?date=YYYY-MM-DD&view=detail`.
The browser Back button preserves navigation. Data polls every minute while a
forecast or its settlement metrics are pending and revalidates on window focus.

Forecast intervals show their recorded coverage target. Metrics use stored
settlement results, not a clock-based guess. Historical observations never
receive invented model forecasts; runs published after their delivery day are
marked as retrospective. The fundamentals charts compare forecasts from the previous day
with actual values from two days earlier and explicitly identify their different delivery days.
