import { expect, test } from '@playwright/test'
import type { Page } from '@playwright/test'

// Deliberately synthetic, isolated HTTP fixtures. The website never serves demo data.
const dates = [
  { delivery_date: '2026-09-05', model_version: '1', predicted_at: '2026-09-04T13:36:00Z', settled: true, has_forecast: true, has_actual: true, has_metrics: true },
  { delivery_date: '2026-09-03', model_version: null, predicted_at: null, settled: true, has_forecast: false, has_actual: true, has_metrics: false },
]
const metrics = {
  mae: 10, rmse: 12, bias: 1, picp: 0.9, mpiw: 40, interval_score: 50,
  baseline_exaa_mae: 11, baseline_7d_mae: 20, rolling_7d_mae: 9.5,
  rolling_7d_baseline_exaa_mae: 10.5, rolling_28d_picp: 0.89,
  monitoring_status: 'ok', monitoring_reasons: [] as string[], evaluated_at: '2026-09-05T13:05:00Z',
}
const quarters = Array.from({ length: 96 }, (_, q) => ({
  quarter_of_day: q,
  predicted_price_eur_per_mwh: 30 + Math.sin(q / 12) * 60,
  lower_price_eur_per_mwh: 10 + Math.sin(q / 12) * 60,
  upper_price_eur_per_mwh: 50 + Math.sin(q / 12) * 60,
  actual_price_eur_per_mwh: 32 + Math.sin(q / 12) * 60,
}))
const features = Array.from({ length: 96 }, (_, q) => ({
  quarter_of_day: q, price_de_lu_exaa_eur_per_mwh: 28 + Math.sin(q / 12) * 60, price_at_exaa_eur_per_mwh: 40,
  price_de_lu_sdac_lag_1d_eur_per_mwh: 50, price_de_lu_sdac_lag_2d_eur_per_mwh: 45, price_de_lu_sdac_lag_7d_eur_per_mwh: 55,
  load_day_ahead_forecast_mw: 40000 + q * 80, load_actual_d_minus_2_mw: 42000 + q * 80,
  solar_day_ahead_forecast_mw: 1000 + q * 100, solar_actual_d_minus_2_mw: 800 + q * 80,
  wind_onshore_day_ahead_forecast_mw: 10000, wind_onshore_actual_d_minus_2_mw: 8000,
  wind_offshore_day_ahead_forecast_mw: 3000, wind_offshore_actual_d_minus_2_mw: 3500,
  residual_load_day_ahead_forecast_mw: 25000, residual_load_actual_d_minus_2_mw: 27000,
  day_of_week: 5, month: 9, season: 'autumn', is_weekend: true, is_holiday_de_nationwide: false, is_holiday_lu: false,
}))
const weather = Array.from({ length: 96 }, (_, q) => ({
  quarter_of_day: q,
  temperature_2m_c: 18 + Math.sin((q - 24) / 96 * Math.PI * 2) * 7,
  wind_speed_100m_m_s: 5 + Math.sin(q / 96 * Math.PI * 4),
  shortwave_radiation_w_m2: Math.max(0, Math.sin((q - 24) / 48 * Math.PI) * 700),
  cloud_cover_pct: 55 + Math.sin(q / 96 * Math.PI * 2) * 30,
}))

async function mockApi(page: Page, mode: { settled?: boolean; featuresError?: boolean; datesError?: boolean; empty?: boolean; metrics?: typeof metrics } = {}) {
  await page.route('**/api/**', async route => {
    const url = new URL(route.request().url())
    if (url.pathname.includes('/downloads/')) return route.fulfill({
      status: 200, contentType: url.pathname.endsWith('.csv') ? 'text/csv' : 'application/zip',
      headers: { 'content-disposition': 'attachment; filename="test-download"' }, body: 'test-fixture-download',
    })
    if (url.pathname === '/api/dates') {
      if (mode.datesError) return route.fulfill({ status: 503, json: { detail: 'Offline' } })
      return route.fulfill({ json: mode.empty ? [] : dates.map(day => day.has_forecast && mode.settled === false ? { ...day, settled: false, has_actual: false, has_metrics: false } : day) })
    }
    if (url.pathname === '/api/model') return route.fulfill({ json: {
      model_name: 'delu.ml.sdac_cqr', model_family: 'hist_gradient_boosting_cqr', version: '1',
      training_through: '2026-08-31', published_at: '2026-09-03T06:00:00Z', target_coverage: 0.9,
      point_shrinkage: 0.61, test_metrics: { mae: 8.57, picp: 0.91 }, baseline_exaa_mae: 8.81,
    } })
    if (url.pathname.startsWith('/api/weather/')) return route.fulfill({ json: {
      delivery_date: url.pathname.split('/')[3], model_run_date: '2026-09-04',
      model: 'ecmwf_ifs', location_count: 25, quarters: weather,
    } })
    const day = url.pathname.split('/')[3]
    if (url.pathname.endsWith('/features')) {
      if (mode.featuresError) return route.fulfill({ status: 503, json: { detail: 'Features unavailable' } })
      // Reverse order to verify joining by quarter, not array position.
      return route.fulfill({ json: { delivery_date: day, rows: features.toReversed() } })
    }
    if (url.pathname.startsWith('/api/observations/')) return route.fulfill({ json: {
      delivery_date: day, quarters: quarters.map(row => ({ quarter_of_day: row.quarter_of_day, actual_price_eur_per_mwh: row.actual_price_eur_per_mwh, price_de_lu_exaa_eur_per_mwh: 50, price_at_exaa_eur_per_mwh: 55 })),
    } })
    return route.fulfill({ json: {
      delivery_date: day, model_version: '1', predicted_at: '2026-09-04T13:36:00Z', nominal_coverage: 0.9,
      settled: mode.settled !== false, metrics: mode.settled === false ? null : mode.metrics ?? metrics,
      quarters: mode.settled === false ? quarters.map(row => ({ ...row, actual_price_eur_per_mwh: null })) : quarters,
    } })
  })
}

test('overview, precise tooltip, detail layout, inputs, generation selector and downloads', async ({ page, isMobile }) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await mockApi(page)
  await page.goto('/')
  await expect(page.getByRole('heading', { name: 'DELU', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Next day' })).toBeDisabled()
  const forecastLegend = page.getByRole('button', { name: 'DELU forecast', exact: true })
  await expect(forecastLegend).toBeVisible()
  await forecastLegend.click()
  await expect(forecastLegend).toHaveAttribute('aria-pressed', 'false')
  await forecastLegend.click()
  const plot = page.locator('.recharts-surface').first()
  if (!isMobile) {
    await plot.focus()
    const tooltip = page.locator('.chart-tooltip')
    await expect(tooltip).toBeVisible()
    await expect(tooltip).toContainText('00:00 - 00:15')
    await expect(tooltip).toContainText('30.00')
    await expect(tooltip).toContainText('10.00 to 50.00')
    await page.keyboard.press('ArrowRight')
    await expect(tooltip).toContainText('00:15 - 00:30')
  }
  await page.getByRole('button', { name: 'Open detailed view', exact: true }).click()
  await expect(page).toHaveURL(/view=detail/)
  await expect(page.getByRole('heading', { name: 'Forecast performance', exact: true })).toBeVisible()
  await expect(page.getByText('Published 04 Sept, 15:36 CEST', { exact: true })).toBeVisible()
  await expect(page.getByText(/cutoff|on-time monitoring|11:30|15:00/)).toHaveCount(0)
  await expect(page.getByText('90.0%', { exact: true }).first()).toBeVisible()
  if (!isMobile) {
    const bounds = await page.locator('.metrics-grid').boundingBox()
    const chart = await page.locator('.price-panel-detail').boundingBox()
    expect(chart!.width).toBeCloseTo(bounds!.width, 0)
    expect(chart!.x).toBeCloseTo(bounds!.x, 0)
  }
  await expect(page.getByRole('cell', { name: '28.00', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Next table page' }).click()
  await expect(page.getByRole('rowheader', { name: '02:00 - 02:15', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Weather', exact: true }).click()
  await expect(page.getByRole('columnheader', { name: 'Temperature at 2 m (°C)', exact: true })).toBeVisible()
  await expect(page.getByRole('cell', { name: '11.00', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Load', exact: true }).click()
  await expect(page.getByRole('columnheader', { name: 'Forecast · previous day', exact: true })).toBeVisible()
  await expect(page.getByRole('rowheader', { name: '00:00 - 00:15', exact: true })).toBeVisible()
  await page.getByLabel('Generation source').selectOption('Solar')
  await expect(page.getByLabel('Solar generation model inputs')).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Weather outlook', exact: true })).toBeVisible()
  await expect(page.getByText('25-point grid mean · run 04 Sept 2026', { exact: true })).toBeVisible()
  await expect(page.getByLabel('Air temperature, 25-point grid mean')).toBeVisible()
  await expect(page.getByLabel('Cloud cover, 25-point grid mean')).toBeVisible()
  const modelDownload = page.waitForEvent('download')
  await page.getByRole('button', { name: /Download model/ }).click()
  expect((await modelDownload).suggestedFilename()).toBe('delu-model-v1.zip')
  const goldDownload = page.waitForEvent('download')
  await page.getByRole('button', { name: /Download dataset/ }).click()
  expect((await goldDownload).suggestedFilename()).toBe('delu-gold.csv')
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
  await page.getByRole('button', { name: 'Back to overview' }).click()
  await expect(page.getByRole('heading', { name: 'DELU', exact: true })).toBeVisible()
  await page.goBack()
  await expect(page.getByRole('heading', { name: 'Forecast performance', exact: true })).toBeVisible()
  expect(errors).toEqual([])
})

test('date arrows expose missing dates and historical observations never acquire forecasts', async ({ page }) => {
  await mockApi(page)
  await page.goto('/')
  await page.getByRole('button', { name: 'Previous day' }).click()
  await expect(page.getByLabel('Delivery date')).toHaveValue('2026-09-04')
  await expect(page.getByText('Forecasts and results will appear as the data becomes available.')).toBeVisible()
  await page.getByRole('button', { name: 'Previous day' }).click()
  await expect(page.getByLabel('Delivery date')).toHaveValue('2026-09-03')
  await expect(page.getByText('Historical observation', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'DELU forecast', exact: true })).toHaveCount(0)
  await expect(page.getByRole('button', { name: 'Previous day' })).toBeDisabled()
  await page.getByRole('button', { name: 'Explore this day' }).click()
  await expect(page.getByText('No forecast has been published for this day.')).toBeVisible()
  await expect(page.getByText('Mean absolute error', { exact: true })).toHaveCount(0)
  await page.getByLabel('Delivery date').fill('2026-09-04')
  await expect(page.getByText('Forecasts and results will appear as the data becomes available.')).toBeVisible()
  await page.getByRole('button', { name: 'Go to latest available day' }).click()
  await expect(page.getByLabel('Delivery date')).toHaveValue('2026-09-05')
})

test('forecast-only data refreshes into settlement without fabricating truth', async ({ page }) => {
  const mode = { settled: false }
  await mockApi(page, mode)
  await page.clock.install()
  await page.goto('/?date=2026-09-05&view=detail')
  await expect(page.getByText('Forecast only', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Actual SDAC', exact: true })).toHaveCount(0)
  await expect(page.getByText('Waiting for actual prices.')).toBeVisible()
  mode.settled = true
  await page.clock.runFor(5 * 60_000 + 1_000)
  await expect(page.getByText('Settled', { exact: true })).toBeVisible()
  await expect(page.getByText('Mean absolute error', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Actual SDAC', exact: true })).toBeVisible()
})

test('partial inputs preserve the price forecast and errors can be retried', async ({ page }) => {
  const mode = { featuresError: true, datesError: true }
  await mockApi(page, mode)
  await page.goto('/')
  await expect(page.getByRole('alert')).toBeVisible()
  mode.datesError = false
  await page.getByRole('button', { name: 'Try again' }).click()
  await expect(page.getByRole('button', { name: 'DELU forecast', exact: true })).toBeVisible()
  await expect(page.getByText('EXAA inputs are unavailable.', { exact: false })).toBeVisible()
  mode.featuresError = false
  await page.getByRole('button', { name: 'Retry inputs' }).click()
  await expect(page.getByRole('button', { name: 'EXAA DE-LU', exact: true })).toBeVisible()
})

test('waiting data refreshes into a published forecast automatically', async ({ page }) => {
  const mode = { empty: true }
  await mockApi(page, mode)
  await page.clock.install()
  await page.goto('/')
  await expect(page.getByText('Forecasts and results will appear as the data becomes available.')).toBeVisible()
  await expect(page.getByRole('button', { name: 'DELU forecast', exact: true })).toHaveCount(0)
  mode.empty = false
  await page.clock.runFor(15 * 60_000 + 1_000)
  await expect(page.getByRole('button', { name: 'DELU forecast', exact: true })).toBeVisible()
  await expect(page.getByText('Waiting for data', { exact: true })).toHaveCount(0)
})

test('invalid date links fall back to the latest available date', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await mockApi(page)
  await page.goto('/?date=invalid')
  await expect(page.getByRole('button', { name: 'DELU forecast', exact: true })).toBeVisible()
  await expect(page.getByLabel('Delivery date')).toHaveValue('2026-09-05')
  expect(errors).toEqual([])
})

test('settled evaluations refresh and show each quality alert once', async ({ page }) => {
  const mode = { metrics: { ...metrics } }
  await mockApi(page, mode)
  await page.clock.install()
  await page.goto('/?date=2026-09-05&view=detail')
  await expect(page.getByText('Evaluated 05 Sept, 15:05 CEST', { exact: true })).toBeVisible()
  mode.metrics = { ...metrics, monitoring_status: 'alert', monitoring_reasons: ['seven-day model MAE is worse than the EXAA baseline'], evaluated_at: '2026-09-06T06:00:00Z' }
  await page.clock.runFor(31 * 60_000)
  await expect(page.getByText('Evaluated 06 Sept, 08:00 CEST', { exact: true })).toBeVisible()
  await page.getByText('Definitions & rolling performance', { exact: true }).click()
  await expect(page.getByText(/seven-day model MAE is worse than the EXAA baseline/)).toHaveCount(1)
})

test('legacy cutoff metadata never becomes a public warning', async ({ page }) => {
  await mockApi(page, { metrics: { ...metrics, monitoring_status: 'late', monitoring_reasons: ['forecast created outside the D-1 15:00 production cutoff', 'excluded from on-time monitoring'] } })
  await page.goto('/?date=2026-09-05&view=detail')
  await expect(page.getByText('Mean absolute error', { exact: true })).toBeVisible()
  await page.getByText('Definitions & rolling performance', { exact: true }).click()
  await expect(page.getByText('Published 04 Sept, 15:36 CEST', { exact: true })).toBeVisible()
  await expect(page.getByText(/cutoff|on-time monitoring|Monitoring: late/)).toHaveCount(0)
})

test('sources have a direct page and remain accessible when market data is unavailable', async ({ page }) => {
  const requests: string[] = []
  page.on('request', request => { if (new URL(request.url()).pathname.startsWith('/api/')) requests.push(request.url()) })
  await mockApi(page, { datesError: true })
  await page.goto('/sources')
  await expect(page).toHaveTitle('Data sources & attribution | DELU')
  await expect(page.getByRole('heading', { name: 'Data sources & attribution', exact: true })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'ENTSO-E', exact: true })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Open-Meteo & ECMWF', exact: true })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'OpenHolidays', exact: true })).toBeVisible()
  await expect(page.getByRole('link', { name: 'Open-Meteo licence', exact: true })).toHaveAttribute('href', 'https://open-meteo.com/en/license')
  await expect(page.getByRole('link', { name: 'Open Database Licence (ODbL)', exact: true })).toHaveAttribute('href', 'https://opendatacommons.org/licenses/odbl/1-0/')
  await expect(page.getByRole('link', { name: 'Terms & free-reuse list', exact: true })).toHaveAttribute('href', /transparencyplatform.zendesk.com/)
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
  await page.reload()
  await expect(page.getByRole('heading', { name: 'Data sources & attribution', exact: true })).toBeVisible()
  expect(requests).toEqual([])
  await page.getByRole('link', { name: 'Back to forecasts', exact: true }).first().click()
  await expect(page.getByRole('alert')).toBeVisible()
  await page.getByRole('link', { name: 'Data sources', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'Data sources & attribution', exact: true })).toBeVisible()
})
