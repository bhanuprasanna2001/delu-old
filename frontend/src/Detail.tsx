import { ChevronLeft, ChevronRight, Clock3, Database, PackageCheck } from 'lucide-react'
import { useState } from 'react'
import useSWR from 'swr'
import Chart, { colors } from './Chart'
import { fetchData, forecastIsLate, formatDate, formatNumber, formatTimestamp, modelSchema, percent, timeRange, weatherSchema } from './data'
import type { DateSummary, Feature, Features, Forecast, Metrics, PricePoint, Weather, WeatherQuarter } from './data'
import { DataError, DownloadButton, Loading, SectionHeading } from './ui'

type Props = {
  day: DateSummary
  forecast?: Forecast
  points: PricePoint[]
  features?: Features
  featuresError: unknown
  retryFeatures: () => void
}

const metricDefinitions: { key: keyof Pick<Metrics, 'mae' | 'rmse' | 'bias' | 'picp' | 'mpiw' | 'interval_score'>; label: string; note: string; percent?: boolean }[] = [
  { key: 'mae', label: 'Mean absolute error', note: 'Average distance from the settled price. Lower is better.' },
  { key: 'rmse', label: 'Root mean square error', note: 'Price error with more weight on larger misses. Lower is better.' },
  { key: 'picp', label: 'Interval coverage', note: 'Share of actual prices inside the prediction interval.', percent: true },
  { key: 'mpiw', label: 'Mean interval width', note: 'Average width between the lower and upper bounds.' },
  { key: 'bias', label: 'Forecast bias', note: 'Mean forecast minus actual. Positive means overprediction.' },
  { key: 'interval_score', label: 'Interval score', note: 'Balances interval width and missed prices. Lower is better.' },
]

export default function Detail({ day, forecast, points, features, featuresError, retryFeatures }: Props) {
  const model = useSWR('/api/model', url => fetchData(url, modelSchema))
  const weather = useSWR(`/api/weather/${day.delivery_date}`, url => fetchData(url, weatherSchema))
  const metrics = forecast?.metrics
  return <div className="detail-sections">
    <section aria-label="Forecast performance">
      <SectionHeading number="01" title="Forecast performance"><span className="section-context">{metrics ? `Evaluated ${formatTimestamp(metrics.evaluated_at)}` : 'Settlement from 15:00 · Europe/Berlin'}</span></SectionHeading>
      {forecastIsLate(forecast ?? day) ? <p className="section-description">This forecast was published outside the day-ahead cutoff. Its metrics are shown for research and excluded from on-time monitoring.</p> : null}
      {metrics ? <>
        <div className="metrics-grid">{metricDefinitions.map(item => <div className="metric" key={item.key} title={item.note}>
          <span className="metric-label">{item.label}</span><div className="metric-value">{item.percent ? percent(metrics[item.key]) : formatNumber(metrics[item.key])}</div>
          <span className="metric-unit">{item.percent ? `${percent(forecast.nominal_coverage).replace('.0%', '%')} target coverage` : 'EUR / MWh'}</span>
        </div>)}</div>
        <details className="metric-details"><summary>Definitions & rolling performance</summary><div className="mt-5 grid gap-x-12 gap-y-4 md:grid-cols-2 xl:grid-cols-3">{metricDefinitions.map(item => <p key={item.key}><strong>{item.label}.</strong> {item.note}</p>)}</div>
          <div className="mt-5 border-t border-line pt-4">EXAA baseline MAE: <strong>{formatNumber(metrics.baseline_exaa_mae)} EUR/MWh</strong> · 7-day rolling MAE: <strong>{formatNumber(metrics.rolling_7d_mae)} EUR/MWh</strong> · 28-day rolling coverage: <strong>{percent(metrics.rolling_28d_picp)}</strong></div>
          <p className="mt-2">Monitoring: {metrics.monitoring_status}{metrics.monitoring_reasons.length ? ` · ${metrics.monitoring_reasons.join(' · ')}` : ''}</p>
        </details>
        {metrics.monitoring_reasons.length ? <p aria-live="polite" className="inline-note text-warning">Model monitoring: {metrics.monitoring_reasons.join(' · ')}</p> : null}
      </> : <div className="settlement-notice"><Clock3 size={20} className="shrink-0 text-muted" /><div><h3>{!day.has_forecast ? 'An observation, before forecast history began.' : day.settled ? 'Prices are settled. Evaluation is pending.' : 'A forecast now. A complete picture after settlement.'}</h3><p>{!day.has_forecast ? 'Market prices and Gold inputs are available for this day. No production forecast was stored, so model performance is not calculated.' : 'The 11:30 job publishes the forecast. The settlement job starts at 15:00 Berlin time; actual prices and metrics appear when the data is available and evaluation finishes.'}</p></div></div>}
    </section>

    <section aria-label="Daily data">
      <SectionHeading number="02" title={day.has_forecast ? 'Behind the forecast' : 'Market data & inputs'}><span className="section-context">{features ? '96 quarters · Gold dataset' : 'Gold dataset'}</span></SectionHeading>
      <p className="section-description">{day.has_forecast ? 'The prices and cutoff-safe inputs used for this delivery day.' : 'Historical market prices and the inputs available for this delivery day.'} Units are shown in the table headings.</p>
      {featuresError && !features ? <DataError error={featuresError} retry={retryFeatures} /> : !features ? <Loading>Loading the daily inputs</Loading> : <InputTable features={features} points={points} weather={weather.data} />}
    </section>

    <section aria-label="Model and downloads">
      <SectionHeading number="03" title="The model & the data"><span className="section-context">Open for a closer look</span></SectionHeading>
      <div className="download-grid">
        <div className="download-panel">
          <div className="flex items-start justify-between gap-4"><span className="download-icon"><PackageCheck size={20} strokeWidth={1.4} /></span><span className="small-tag">Production model{model.data ? ` · v${model.data.version}` : ''}</span></div>
          <h3>Built on the market. Calibrated for uncertainty.</h3>
          <p>EXAA-anchored gradient boosting with conformal prediction intervals.</p>
          {model.error && !model.data ? <DataError error={model.error} retry={() => void model.mutate()} /> : !model.data ? <Loading>Loading model details</Loading> : <>
            <dl className="model-facts"><div><dt>Trained through</dt><dd>{formatDate(model.data.training_through, 'short')}</dd></div><div><dt>Target coverage</dt><dd>{percent(model.data.target_coverage)}</dd></div><div><dt>Holdout MAE</dt><dd>{formatNumber(model.data.test_metrics.mae)} EUR/MWh</dd></div><div><dt>Holdout coverage</dt><dd>{model.data.test_metrics.picp == null ? 'Not available' : percent(model.data.test_metrics.picp)}</dd></div></dl>
            {forecast && forecast.model_version !== model.data.version ? <p className="mb-4 text-xs">This day's forecast used v{forecast.model_version}. The download is the current production model, v{model.data.version}.</p> : null}
            <details className="metric-details mt-4"><summary>All holdout metrics</summary><dl className="model-facts">{Object.entries(model.data.test_metrics).map(([key, value]) => <div key={key}><dt>{key.replaceAll('_', ' ')}</dt><dd>{key.includes('picp') || key.includes('coverage') ? percent(value) : formatNumber(value)}</dd></div>)}</dl></details>
            <div className="download-panel-footer"><DownloadButton href="/api/downloads/model.zip" filename={`delu-model-v${model.data.version}.zip`}>Download model <span className="file-type">ZIP</span></DownloadButton><span className="text-[10px] text-muted">Published {formatTimestamp(model.data.published_at)}</span></div>

          </>}
        </div>
        <div className="download-panel dataset-panel">
          <div className="flex items-start justify-between gap-4"><span className="download-icon"><Database size={20} strokeWidth={1.4} /></span><span className="small-tag">Gold dataset</span></div>
          <h3>The full picture, down to every quarter hour.</h3>
          <p>Download the complete Gold dataset, including market prices, load, renewable generation, weather, and calendar features.</p>
          <dl className="model-facts"><div><dt>Resolution</dt><dd>15 minutes</dd></div><div><dt>Daily grid</dt><dd>96 normalized quarters</dd></div><div><dt>Source</dt><dd>ENTSO-E · OpenHolidays · <a href="https://open-meteo.com/" target="_blank" rel="noreferrer">Open-Meteo</a></dd></div><div><dt>Format</dt><dd>CSV, with column headers</dd></div></dl>
          <div className="download-panel-footer"><DownloadButton href="/api/downloads/gold.csv" filename="delu-gold.csv">Download dataset <span className="file-type">CSV</span></DownloadButton><span className="text-[10px] text-muted">All available dates</span></div>
        </div>
      </div>
    </section>

    <section aria-label="Load and generation plots">
      <SectionHeading number="04" title="The fundamentals"><span className="section-context">{formatDate(day.delivery_date, 'short')} · Model inputs</span></SectionHeading>
      <p className="section-description">The forecast inputs come from the day before the selected date; measured values come from two days earlier. They show the information available to the model, rather than forecast accuracy for a single day.</p>
      {featuresError && !features ? <DataError error={featuresError} retry={retryFeatures} /> : !features ? <Loading>Loading fundamentals</Loading> : <Fundamentals features={features} />}
    </section>

    <section aria-label="Weather plots">
      <SectionHeading number="05" title="Weather outlook"><span className="section-context">{weather.data ? `${weather.data.location_count}-point grid mean · run ${formatDate(weather.data.model_run_date, 'short')}` : 'Open-Meteo · ECMWF IFS'}</span></SectionHeading>
      <p className="section-description">Cutoff-safe forecasts for the delivery day from the previous 00Z model run. Temperature-only gaps use ECMWF IFS 0.25°; known midnight solar gaps are zero.</p>
      {weather.error && !weather.data ? <DataError error={weather.error} retry={() => void weather.mutate()} /> : !weather.data ? <Loading>Loading weather outlook</Loading> : <WeatherPlots weather={weather.data} />}
    </section>
  </div>
}

type Column = { label: string; key: keyof Feature | keyof PricePoint | keyof WeatherQuarter; kind?: 'boolean' | 'text' }
const tables: Record<string, Column[]> = {
  'Market prices': [
    { label: 'DELU forecast', key: 'forecast' }, { label: 'Actual SDAC', key: 'actual' },
    { label: 'Prediction interval', key: 'interval' }, { label: 'EXAA DE-LU', key: 'price_de_lu_exaa_eur_per_mwh' },
    { label: 'EXAA AT', key: 'price_at_exaa_eur_per_mwh' },
    { label: 'SDAC · previous day', key: 'price_de_lu_sdac_lag_1d_eur_per_mwh' },
    { label: 'SDAC · two days earlier', key: 'price_de_lu_sdac_lag_2d_eur_per_mwh' },
    { label: 'SDAC · one week earlier', key: 'price_de_lu_sdac_lag_7d_eur_per_mwh' },
  ],
  Load: [
    { label: 'Forecast · previous day', key: 'load_day_ahead_forecast_mw' }, { label: 'Actual · two days earlier', key: 'load_actual_d_minus_2_mw' },
    { label: 'Residual forecast · previous day', key: 'residual_load_day_ahead_forecast_mw' }, { label: 'Residual actual · two days earlier', key: 'residual_load_actual_d_minus_2_mw' },
  ],
  Generation: [
    { label: 'Solar forecast · previous day', key: 'solar_day_ahead_forecast_mw' }, { label: 'Solar actual · two days earlier', key: 'solar_actual_d_minus_2_mw' },
    { label: 'Onshore forecast · previous day', key: 'wind_onshore_day_ahead_forecast_mw' }, { label: 'Onshore actual · two days earlier', key: 'wind_onshore_actual_d_minus_2_mw' },
    { label: 'Offshore forecast · previous day', key: 'wind_offshore_day_ahead_forecast_mw' }, { label: 'Offshore actual · two days earlier', key: 'wind_offshore_actual_d_minus_2_mw' },
  ],
  Weather: [
    { label: 'Temperature at 2 m (°C)', key: 'temperature_2m_c' },
    { label: 'Wind speed at 100 m (m/s)', key: 'wind_speed_100m_m_s' },
    { label: 'Solar radiation (W/m²)', key: 'shortwave_radiation_w_m2' },
    { label: 'Cloud cover (%)', key: 'cloud_cover_pct' },
  ],
  Calendar: [
    { label: 'Day of week (Mon = 0)', key: 'day_of_week' }, { label: 'Month', key: 'month' }, { label: 'Season', key: 'season', kind: 'text' },
    { label: 'Weekend', key: 'is_weekend', kind: 'boolean' }, { label: 'DE nationwide holiday', key: 'is_holiday_de_nationwide', kind: 'boolean' }, { label: 'LU holiday', key: 'is_holiday_lu', kind: 'boolean' },
  ],
}

function InputTable({ features, points, weather }: { features: Features; points: PricePoint[]; weather?: Weather }) {
  const [tab, setTab] = useState('Market prices')
  const [page, setPage] = useState(0)
  const columns = tables[tab]
  const prices = new Map(points.map(point => [point.quarter, point]))
  const weatherRows = new Map(weather?.quarters.map(row => [row.quarter_of_day, row]))
  const rows = features.rows.toSorted((a, b) => a.quarter_of_day - b.quarter_of_day).slice(page * 8, (page + 1) * 8)
  return <div className="table-panel">
    <div className="table-tabs" aria-label="Input data categories">{Object.keys(tables).map(name => <button type="button" key={name} aria-pressed={name === tab} onClick={() => { setTab(name); setPage(0) }}>{name}</button>)}</div>
    {/* oxlint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- A scrollable table needs keyboard focus. */}
    <section className="table-scroll" tabIndex={0} aria-label={`${tab} data, scroll for more columns`}>
      <table><caption className="sr-only">{tab} for {features.delivery_date}. Units are shown in the column headings.</caption><thead><tr><th scope="col">Delivery time</th>{columns.map(column => <th scope="col" key={column.key}>{column.label}</th>)}</tr></thead><tbody>{rows.map(feature => {
        const row = { ...feature, ...prices.get(feature.quarter_of_day), ...weatherRows.get(feature.quarter_of_day) }
        return <tr key={feature.quarter_of_day}><th scope="row">{timeRange(feature.quarter_of_day)}</th>{columns.map(column => {
          const value = row[column.key]
          return <td key={column.key} className={column.key === 'forecast' ? 'text-forecast' : ''}>{value == null ? <span className="text-muted">Pending / unavailable</span> : Array.isArray(value) ? `${formatNumber(value[0])} to ${formatNumber(value[1])}` : typeof value === 'boolean' ? value ? 'Yes' : 'No' : typeof value === 'number' ? tab === 'Calendar' ? value : formatNumber(value) : value}</td>
        })}</tr>
      })}</tbody></table>
    </section>
    <div className="table-footer"><span>Quarters <strong>{page * 8 + 1}-{Math.min((page + 1) * 8, 96)}</strong> of 96 <span className="hidden sm:inline">· Europe/Berlin</span></span><div className="flex items-center gap-3"><span className="tabular-nums">{page + 1} / 12</span><button type="button" className="icon-button" aria-label="Previous table page" disabled={page === 0} onClick={() => setPage(page - 1)}><ChevronLeft size={15} /></button><button type="button" className="icon-button" aria-label="Next table page" disabled={page === 11} onClick={() => setPage(page + 1)}><ChevronRight size={15} /></button></div></div>
  </div>
}

const inputSeries = [
  { key: 'prior', label: 'Actual from two days earlier', color: colors.prior },
  { key: 'forecast', label: 'Forecast from previous day', color: colors.forecast, style: 'dashed' as const },
]
const generationKeys = {
  'Wind + solar': ['solar', 'wind_onshore', 'wind_offshore'],
  Solar: ['solar'], 'Onshore wind': ['wind_onshore'], 'Offshore wind': ['wind_offshore'],
} as const

function Fundamentals({ features }: { features: Features }) {
  const [generation, setGeneration] = useState<keyof typeof generationKeys>('Wind + solar')
  const rows = features.rows.toSorted((a, b) => a.quarter_of_day - b.quarter_of_day)
  const load = rows.map(row => ({ quarter: row.quarter_of_day, forecast: row.load_day_ahead_forecast_mw / 1000, prior: row.load_actual_d_minus_2_mw / 1000 }))
  const renewable = rows.map(row => ({
    quarter: row.quarter_of_day,
    forecast: generationKeys[generation].reduce((sum, key) => sum + row[`${key}_day_ahead_forecast_mw`], 0) / 1000,
    prior: generationKeys[generation].reduce((sum, key) => sum + row[`${key}_actual_d_minus_2_mw`], 0) / 1000,
  }))
  return <div className="fundamentals-grid"><div className="fundamental-panel"><div className="fundamental-heading"><h3>Electricity load</h3><span className="text-[11px] text-muted">Germany</span></div><Chart points={load} series={inputSeries} unit="GW" label="Electricity load model inputs" /></div><div className="fundamental-panel"><div className="fundamental-heading"><h3>Renewable generation</h3><select aria-label="Generation source" value={generation} onChange={event => setGeneration(event.target.value as keyof typeof generationKeys)}>{Object.keys(generationKeys).map(key => <option key={key}>{key}</option>)}</select></div><Chart points={renewable} series={inputSeries} unit="GW" label={`${generation} generation model inputs`} /></div></div>
}

type WeatherMetric = keyof Omit<WeatherQuarter, 'quarter_of_day'>
const weatherMetrics: { key: WeatherMetric; title: string; unit: string; yDomain?: [number | 'auto', number | 'auto'] }[] = [
  { key: 'temperature_2m_c', title: 'Air temperature', unit: '°C' },
  { key: 'wind_speed_100m_m_s', title: 'Wind speed at 100 m', unit: 'm/s', yDomain: [0, 'auto'] },
  { key: 'shortwave_radiation_w_m2', title: 'Solar radiation', unit: 'W/m²', yDomain: [0, 'auto'] },
  { key: 'cloud_cover_pct', title: 'Cloud cover', unit: '%', yDomain: [0, 100] },
]
const weatherSeries = [{ key: 'mean', label: 'Grid mean', color: colors.forecast, style: 'dashed' as const }]

function WeatherPlots({ weather }: { weather: Weather }) {
  const rows = weather.quarters.toSorted((a, b) => a.quarter_of_day - b.quarter_of_day)
  return <div className="fundamentals-grid">{weatherMetrics.map(metric => {
    const values = rows.map(row => row[metric.key])
    const points = rows.map(row => ({ quarter: row.quarter_of_day, mean: row[metric.key] }))
    return <div className="fundamental-panel" key={metric.key}>
      <div className="fundamental-heading"><h3>{metric.title}</h3><span className="text-[11px] tabular-nums text-muted">{formatNumber(Math.min(...values))} to {formatNumber(Math.max(...values))} {metric.unit}</span></div>
      <Chart points={points} series={weatherSeries} unit={metric.unit} yDomain={metric.yDomain} label={`${metric.title}, ${weather.location_count}-point grid mean`} />
    </div>
  })}</div>
}
