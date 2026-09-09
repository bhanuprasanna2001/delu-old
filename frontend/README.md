# DELU website

React, Vite, and TypeScript, styled with Tailwind CSS. Charts use Recharts;
Oxlint checks the code. Requires Node.js 24 or newer.

- [Set up and run locally](../README.md#run-locally)
- [Run frontend checks](../CONTRIBUTING.md#website)
- [Deploy on Render](../README.md#deploy-on-render)

Vite serves development traffic on port 5173 and forwards `/api` to FastAPI on
port 8000. Production builds write to `app/static/` at the repository root.
FastAPI serves the built website and API together on Render or Databricks Apps.

Share a selected day with `?date=YYYY-MM-DD&view=detail`. Forecasts and evaluation
metrics refresh every minute, including settled days updated by a backfill.
Publication timestamps remain visible; only model-quality alerts are shown,
once, in the performance section. The website uses real API data;
synthetic data is limited to the browser tests.

`/sources` is a separate attribution page linked from the overview, daily inputs,
downloads, and detail footer. It loads independently of the data API and supports
direct navigation and refresh through FastAPI. Keep its provider details in
`src/Sources.tsx` aligned with [DATA_SOURCES.md](../DATA_SOURCES.md). Weather tables
and charts also retain their direct Open-Meteo credit.

Browser tests cover desktop and mobile navigation, downloads, pending-data
refresh, updated settled evaluations, legacy cutoff metadata, and access to the
sources page during an API outage. Run `npm test` from this directory.
