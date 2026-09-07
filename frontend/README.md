# DELU website

React, Vite, and TypeScript, styled with Tailwind CSS. Charts use Recharts;
Oxlint checks the code. Requires Node.js 24 or newer.

- [Set up and run locally](../README.md#run-locally)
- [Run frontend checks](../CONTRIBUTING.md#website)
- [Deploy on Render](../README.md#deploy-on-render)

Vite serves development traffic on port 5173 and forwards `/api` to FastAPI on
port 8000. Production builds write to `app/static/` at the repository root.
FastAPI serves the built website and API together on Render or Databricks Apps.

Share a selected day with `?date=YYYY-MM-DD&view=detail`. Pending forecasts and
settlement metrics refresh every minute. The website uses real API data;
synthetic data is limited to the browser tests.
