# Contributing to DELU

Contributions can improve the website, API, data pipeline, models, tests, or docs.
Keep each change focused and explain how you verified it.

[Local setup](README.md#run-locally) · [Project map](#2-find-the-right-place) ·
[Checks](#3-verify-your-change) · [Deployment reference](#deployment-reference)

## 1. Set up your workspace

Fork the repository if you do not have write access, then follow the
[local setup](README.md#run-locally), using your fork’s clone URL when applicable.
Create a branch for your change:

```bash
git switch -c your-change-name
```

For tests and frontend builds, only the dependency installation is needed.
Live browsing requires a Databricks connection; the Python and browser tests use
local data or mocks and do not require credentials.

## 2. Find the right place

| Location | Responsibility |
| :--- | :--- |
| [frontend/src/](frontend/src/) | React views, charts, styles, and API data handling |
| [DATA_SOURCES.md](DATA_SOURCES.md) and [frontend/src/Sources.tsx](frontend/src/Sources.tsx) | Provider attribution, usage terms, and data transformations |
| [app/delu_app/](app/delu_app/) | FastAPI routes and read-only Databricks access |
| [src/delu/pipeline/](src/delu/pipeline/) | Raw ingestion (Bronze), cleanup (Silver), and model inputs (Gold) |
| [src/delu/ml/](src/delu/ml/) | Training, prediction, evaluation, and monitoring |
| [tests/](tests/) and [frontend/tests/](frontend/tests/) | Python and browser tests |
| [resources/](resources/) and [databricks.yml](databricks.yml) | Databricks jobs, app, volume, and deployment targets |
| [Dockerfile](Dockerfile) and [render.yaml](render.yaml) | Public website deployment |

Follow the existing patterns and use the installed tools. For bug fixes, reproduce
the reported behavior before changing it, then check the same flow after the fix.

Keep these project rules intact:

- Forecast inputs must use only information available at prediction time.
- Evaluate models in time order. Never present training-set predictions as
  historical production forecasts.
- Show actual prices and metrics only when the stored results exist. Label
  actual publication timestamps and the delivery dates of load and generation inputs clearly.
- Check keyboard access, chart tooltips, mobile layouts, and loading/error states
  when changing the UI.
- Keep credentials, datasets, and model artifacts out of commits. Let the package
  manager update lockfiles; do not hand-edit generated output.
- Update both attribution references when a source or transformation changes.
  Keep a linked Open-Meteo credit beside displayed weather data.

## 3. Verify your change

Run the checks for the area you changed from the repository root. For documentation
changes, check the links, command examples, and Markdown preview.

### Python and API

```bash
uv run ruff format --check src tests app
uv run ruff check src tests app
uv run ty check
uv run pytest
```

Add a focused regression test when fixing behavior. Use `uv run ruff format` on
changed Python files if formatting needs adjustment.

### Website

```bash
npm --prefix frontend run lint
npm --prefix frontend run build
npm --prefix frontend exec -- playwright install chromium
npm --prefix frontend test
```

Playwright starts Vite and checks desktop and mobile flows using synthetic API
responses. On Linux, install browser system dependencies with
`npm --prefix frontend exec -- playwright install --with-deps chromium` if needed.
The production build writes to `app/static/`; do not commit that generated folder.

### Container or deployment changes

```bash
docker build -t delu-web .
docker run --rm --env-file .env.render -p 10000:10000 delu-web
```

Use the credentials configured during local setup. Open <http://localhost:10000>
and check both the website and the data connection described below.

GitHub CI checks Python formatting, lint, types, tests, and wheel packaging. It
also builds the Docker image, which checks and builds the frontend. Browser tests
currently run locally.

## 4. Open a pull request

Push your branch and open a pull request against `main`. Include:

- The problem and what users will see after the change.
- The checks you ran and their results.
- Screenshots for visual changes, or reproduction steps for a bug fix.
- Any required configuration or data changes.

Keep unrelated cleanup separate. For a bug report, include the selected delivery
date, steps to reproduce, expected behavior, and the actual result. Remove secrets
and private data from logs or screenshots before sharing them.

## Databricks access

The public app uses OAuth credentials for a **Databricks service principal**.
That principal must be assigned to the workspace and have:

| Resource | Permission |
| :--- | :--- |
| SQL warehouse `32972e04a08b3a3f` | `CAN_USE` |
| Catalog `delu` | `USE CATALOG` |
| Schema `delu.gold` | `USE SCHEMA` |
| Tables `model_input`, `forecasts`, `forecast_runs`, `forecast_metrics` in `delu.gold` | `SELECT` |
| Volume `delu.gold.model_exports` | `READ VOLUME` |

The workspace URL is `https://dbc-a4fcce00-b5bf.cloud.databricks.com`.
[.env.render.example](.env.render.example) contains all connection and resource
settings. A separate Render principal does not inherit the grants configured for
Databricks Apps. Keep OAuth credentials on the API server, never in frontend code
or `VITE_*` variables.

## Deployment reference

These instructions operate on an existing DELU workspace with its tables and model
exports populated. Deploying the website alone does not create the data pipeline.

<details>
<summary><strong>Render: verify a deployment</strong></summary>

Follow the three steps in the [README](README.md#deploy-on-render). For a private
repository, grant Render’s GitHub app access to it. When deploying a fork, connect
that fork as a Blueprint and use its `render.yaml`.

After deployment, open these paths on the assigned public URL:

| Path | What it checks |
| :--- | :--- |
| `/healthz` | Web process is running; no Databricks query. Render uses this check. |
| `/api/dates?limit=1` | Warehouse access and the latest available date. |
| `/api/model` | Access to the exported model metadata. |
| `/docs` | Interactive reference for every API endpoint. |
| `/sources` | Directly accessible source attribution page; no warehouse query. |

Then open a day in the website and try the Gold CSV and model ZIP downloads.
`/api/health` provides forecast and settlement freshness through a database query.

If the page loads but data fails, check Render’s logs, OAuth credentials, and the
permissions above. A sleeping warehouse can delay the first data request even
when the web service is healthy.

The Docker image serves the built React app and FastAPI together, using Render’s
`PORT`. It runs as an unprivileged user and excludes local credentials, datasets,
and dependencies from its build context.

</details>

<details>
<summary><strong>Databricks: update the authenticated app and jobs</strong></summary>

The existing app is
[delu-api-prod](https://delu-api-prod-7474652950254462.aws.databricksapps.com).
Visitors must sign in and have access to the app. Use Render for anonymous access.

The bundle currently uses the CLI profile `bhanu prasanna`. Configure your own
profile in `databricks.yml` when working in another workspace. Both `dev` and `prod`
currently share the `delu` catalog and workspace; dev is not an isolated data sandbox.

**1. Build and validate from the repository root.**

```bash
npm --prefix frontend ci
npm --prefix frontend run build
databricks bundle validate --strict -t dev
databricks bundle validate --strict -t prod
```

**2. Deploy and start each environment included in the release.**

For development:

```bash
databricks bundle deploy -t dev
databricks bundle run -t dev api
databricks bundle summary -t dev
```

For a production release:

```bash
databricks bundle deploy -t prod
databricks bundle run -t prod api
databricks bundle summary -t prod
```

A bundle deployment updates its configured jobs and resources as well as the app
source. Keep dev schedules paused to avoid duplicate processing. Dev uses
`model_exports_dev`; production uses `model_exports` and active schedules.
Deploy both targets when changing shared job definitions so dev also replaces
obsolete jobs. Deploying the bundle does not run the data or training jobs.
Verify the deployed job's queue is enabled, maximum concurrent runs is one,
and schedule is paused in dev and active in prod. Check each app with
`databricks apps get delu-api-dev` or `databricks apps get delu-api-prod`, using
the configured profile, and confirm its `app_status.state` is `RUNNING`.

The ingestion jobs require the ENTSO-E API key. If its secret scope has not been
configured, create it once and enter the key when prompted:

```bash
databricks secrets create-scope delu --profile "bhanu prasanna"
databricks secrets put-secret delu entsoe-api-key --profile "bhanu prasanna"
```

With labeled Gold history available, `databricks bundle run -t prod monthly_training`
trains and evaluates the initial model. A candidate must pass promotion checks to
become `@prod` before scheduled forecasting can use it.

The `data_pipeline` job runs at 02:30, 10:30, 11:30, and 13:30 Europe/Berlin in
cost-efficient `STANDARD` performance mode. It fetches missing source responses,
rebuilds complete Silver and Gold data, fills missing forecasts, and evaluates
available actual prices. Missing or temporarily unavailable data stays pending for
the next run; it has no expiry. To backfill a specific inclusive delivery range:

```bash
databricks bundle run -t prod --params start=2026-09-01,end=2026-09-05 data_pipeline
```

The same job fetches lagged inputs for the requested range. Without explicit
bounds, ingestion checks the full source history, prediction fills gaps from the
first published forecast (or the model's registration date on first use), and
evaluation checks all stored forecasts. Published forecasts retain their original
values and timestamps; newly backfilled predictions record their actual creation
time and the model version used.

An overlapping trigger is queued behind the active run. Keep native queueing
enabled and the single-run limit in the bundle: Silver and Gold rebuild shared
tables, so parallel runs would compete over the same output. A queued run checks
current data when it starts. If the platform expires a queued run after its
48-hour limit, the source gaps are still picked up by subsequent scheduled runs.
Use the production job for shared-catalog backfills, rather than starting a
parallel dev job. The Free Edition account limit of five concurrent tasks is
separate from this job's concurrency setting; see [processing and retries](MODEL_DATA.md#processing-and-retries).

Inspect a successful run's individual tasks and logs as well as its overall
status. A successful run can still be waiting for unpublished source data.
Validate availability with `/api/dates` and `/api/health`, then inspect a forecast
and its evaluation on the website. Pending results refresh every five minutes and
settled evaluations every 30 minutes, so backfilled rolling metrics can update
without reopening the page. Historical
`late` metrics are recomputed by evaluation; the UI shows quality alerts once
and does not display retired cutoff labels.

</details>

<details>
<summary><strong>Model development and evaluation</strong></summary>

The pipeline normalizes each day to 96 quarters. The model learns the difference
between SDAC and EXAA, then calibrates its quantile intervals on held-out data.
Monthly training uses data only through the previous month-end, evaluates in time
order, and compares the candidate with EXAA and the current production model.

To train locally, obtain an authorized Gold snapshot at
`data/gold/model_input.parquet`, then run:

```bash
uv run delu-train --data-path data/gold/model_input.parquet --through 2026-08-31
```

Replace the example cutoff with the previous full month-end for your evaluation.
The local model is saved to `artifacts/sdac_cqr`; both the snapshot and artifacts
are ignored by Git. Production versions are registered as `delu.ml.sdac_cqr`, with
`@candidate` and `@prod` aliases.

Settlement provides the feedback: released prices are joined to stored forecasts
to calculate MAE, RMSE, bias, interval coverage, interval width, and interval score.
Rolling error and coverage checks are recorded alongside the metrics without
blocking data processing. Unexpected code, configuration, and authentication
failures still fail the job and trigger native Databricks notifications. Settled data becomes history for subsequent training.

Method references:
[electricity price forecasting and temporal evaluation](https://doi.org/10.1016/j.apenergy.2021.116983)
and [conformalized quantile regression](https://papers.neurips.cc/paper_files/paper/2019/file/5103c3584b063c431bd1268e9b5e76fb-Paper.pdf).

</details>
