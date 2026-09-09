<h1 align="center"><i>delu</i></h1>

<p align="center">
  Electricity price forecasts for Germany and Luxembourg.<br>
  Explore each day’s prices, prediction intervals, and market data.
</p>

<p align="center">
  <a href="#the-project">Overview</a> ·
  <a href="https://delu.bhanuprasanna.com/">Live site</a> ·
  <a href="MODEL_DATA.md">Model &amp; data</a> ·
  <a href="DATA_SOURCES.md">Data sources</a> ·
  <a href="EXPERIMENTS.md">Experiments</a> ·
  <a href="#run-locally">Run locally</a> ·
  <a href="#deploy-on-render">Deploy</a> ·
  <a href="CONTRIBUTING.md">Contribute</a>
</p>

<p align="center">
  <a href="https://github.com/bhanuprasanna2001/delu/actions/workflows/ci.yml">
    <img src="https://github.com/bhanuprasanna2001/delu/actions/workflows/ci.yml/badge.svg" alt="Build and checks">
  </a>
  <a href="https://delu.bhanuprasanna.com/">
    <img src="https://img.shields.io/badge/live-delu.bhanuprasanna.com-547dab" alt="Live site">
  </a>
</p>

## The project

DELU forecasts electricity prices at 15-minute intervals for the Germany–Luxembourg
market. It pairs model predictions with uncertainty intervals, then compares them
with published Single Day-Ahead Coupling (SDAC) prices when results are available.

- **Explore a day.** Navigate dates and hover over the chart to inspect each price.
- **Look closer.** Open the detailed view for forecast inputs, settlement metrics,
  and load and generation charts.
- **Follow the results.** See actual prices and model performance as they arrive.
- **Take the data with you.** Download the production model and Gold dataset, the
  cleaned data used by the forecasting pipeline.
- **Trace the sources.** Open the dedicated [attribution page](https://delu.bhanuprasanna.com/sources)
  for providers, usage terms, and the transformations behind the displayed data.

## How it works

```mermaid
flowchart LR
    D["Databricks<br>Data · training · scheduled forecasts"]
    A["FastAPI<br>Read-only API"]
    W["React<br>Interactive website"]
    D --> A --> W
```

Databricks prepares the data, trains the model, and stores forecasts. FastAPI reads
those results; React displays them in the browser. On Render, the website and API
run together in one service, with credentials kept on the server.

| Scheduled job | Frequency | What it does |
| :--- | :--- | :--- |
| Data pipeline | Every 30 minutes | Fetches missing data, builds complete days, publishes missing forecasts, and evaluates available actual prices. |
| Retraining | Monthly, day 3 at 06:00 Europe/Berlin | Evaluates a candidate model and promotes it if the quality checks pass. |

Missing data stays pending. Each run retries the missing source responses across
history, including gaps lasting several days. Existing forecasts keep their
original values and publication timestamps. A delayed forecast is evaluated in
the same way once its actual prices are available.

If a scheduled run arrives while the pipeline is busy, Databricks queues it.
One run writes the shared tables at a time. The next run checks the remaining
gaps, so a delayed API response does not require a separate recovery workflow.
The website refreshes forecasts and evaluation results every minute, including
settled days whose rolling metrics change after a backfill. Publication time is
shown as recorded; monitoring messages describe model quality only.

The model starts with the early EXAA auction price and learns a correction using
boosted trees. Calibrated prediction intervals target 90% coverage. Results appear
after the scheduled jobs complete and the required data is available.

See [Model and data](MODEL_DATA.md) for exact Bronze-to-Gold lineage and the
training, prediction, evaluation, and backfill behavior. See
[Experiments](EXPERIMENTS.md) for offline evidence and reproducible comparisons.

**Reading the charts:** historical dates may contain observations without a stored
model forecast. Load and generation inputs compare forecasts from the previous day
with actual values from two days earlier; they represent different delivery days.

## Run locally

You need **Python 3.12+**, **Node.js 24+**, and **[uv](https://docs.astral.sh/uv/)**.
Live data also requires access to the existing Databricks tables and model exports.
See [Databricks access](CONTRIBUTING.md#databricks-access) for the required permissions.

### 1. Get the project

```bash
git clone https://github.com/bhanuprasanna2001/delu.git
cd delu
uv sync --locked --all-groups
npm --prefix frontend ci
```

### 2. Add your connection details

```bash
cp .env.render.example .env.render
```

Open `.env.render` and fill in `DATABRICKS_CLIENT_ID` and
`DATABRICKS_CLIENT_SECRET`. The DELU workspace, warehouse, and resource names are
already filled in. Use your own values if connecting to another configured
workspace. This file is ignored by Git.

### 3. Start the API

```bash
uv run --env-file .env.render uvicorn app.delu_app.api:app --reload --host 127.0.0.1 --port 8000
```

API documentation is available at <http://127.0.0.1:8000/docs>.

### 4. Start the website

In a second terminal, from the repository root:

```bash
npm --prefix frontend run dev
```

Open **<http://127.0.0.1:5173>**. Vite forwards API requests to port 8000.
The first data request can take longer while the Databricks SQL warehouse starts;
the website displays a loading message while you wait.

## Deploy on Render

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/bhanuprasanna2001/delu)

1. Click **Deploy to Render** and connect the GitHub repository.
2. Enter `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`. The remaining values
   are provided by [render.yaml](render.yaml).
3. Review the service plan in the blueprint, then click **Deploy**. Open the public
   `onrender.com` URL once the service is live.

Visitors do not need a Databricks account. Data, training, and scheduled jobs stay
in Databricks. Later commits deploy after GitHub checks pass.

The blueprint currently selects the Free plan in Frankfurt. For
connection permissions, deployment checks, and the authenticated Databricks Apps
option, see the [contributing guide](CONTRIBUTING.md#deployment-reference).

## Contributing

Start with [CONTRIBUTING.md](CONTRIBUTING.md) for the project map, checks to run,
and the pull request workflow. Browser tests use fixtures and can run without
Databricks credentials.

Built with React, Vite, TypeScript, Tailwind CSS, FastAPI, and Databricks.
Market data comes from [ENTSO-E](https://transparency.entsoe.eu/), weather data
from [Open-Meteo](https://open-meteo.com/), and holiday data from
[OpenHolidays](https://www.openholidaysapi.org/).
Provider credits, licences, and data transformations are documented in
[DATA_SOURCES.md](DATA_SOURCES.md) and on the website's
[Data sources & attribution page](https://delu.bhanuprasanna.com/sources).
