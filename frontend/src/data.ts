import { z } from 'zod'

const number = z.number().finite()
export const dateSchema = z.iso.date()
const quarter = z.number().int().min(0).max(95)
const publicationStatus = z.enum(['on_time', 'late', 'backfill'])
const grid = <T extends z.ZodType<{ quarter_of_day: number }>>(schema: T) =>
  z.array(schema).length(96).refine(rows => new Set(rows.map(row => row.quarter_of_day)).size === 96, 'Incomplete quarter-hour grid')

export const datesSchema = z.array(z.object({
  delivery_date: dateSchema,
  model_version: z.string().nullable(),
  predicted_at: z.string().nullable(),
  settled: z.boolean(),
  has_forecast: z.boolean().default(true),
  has_actual: z.boolean().optional(),
  has_metrics: z.boolean().optional(),
  publication_status: publicationStatus.nullable(),
})).transform(days => days.toSorted((a, b) => b.delivery_date.localeCompare(a.delivery_date)))

const metricsSchema = z.object({
  mae: number, rmse: number, bias: number, picp: number, mpiw: number,
  interval_score: number, baseline_exaa_mae: number, baseline_7d_mae: number,
  rolling_7d_mae: number.nullable(), rolling_7d_baseline_exaa_mae: number.nullable(), rolling_28d_picp: number.nullable(),
  monitoring_status: z.string(), monitoring_reasons: z.array(z.string()), evaluated_at: z.string(),
})

export const forecastSchema = z.object({
  delivery_date: dateSchema, model_version: z.string(), predicted_at: z.string(),
  publication_status: publicationStatus,
  nominal_coverage: number.min(0).max(1), settled: z.boolean(), metrics: metricsSchema.nullable(),
  quarters: grid(z.object({
    quarter_of_day: quarter,
    predicted_price_eur_per_mwh: number,
    lower_price_eur_per_mwh: number,
    upper_price_eur_per_mwh: number,
    actual_price_eur_per_mwh: number.nullable(),
  }).refine(row => row.lower_price_eur_per_mwh <= row.upper_price_eur_per_mwh, 'Invalid prediction interval')),
})

export const observationsSchema = z.object({
  delivery_date: dateSchema,
  quarters: grid(z.object({
    quarter_of_day: quarter, actual_price_eur_per_mwh: number.nullable(),
    price_de_lu_exaa_eur_per_mwh: number, price_at_exaa_eur_per_mwh: number,
  })),
})

export const featuresSchema = z.object({
  delivery_date: dateSchema,
  rows: grid(z.object({
    quarter_of_day: quarter,
    price_de_lu_exaa_eur_per_mwh: number, price_at_exaa_eur_per_mwh: number,
    price_de_lu_sdac_lag_1d_eur_per_mwh: number,
    price_de_lu_sdac_lag_2d_eur_per_mwh: number,
    price_de_lu_sdac_lag_7d_eur_per_mwh: number,
    load_day_ahead_forecast_mw: number.nullable(), load_actual_d_minus_2_mw: number,
    solar_day_ahead_forecast_mw: number.nullable(), solar_actual_d_minus_2_mw: number,
    wind_onshore_day_ahead_forecast_mw: number.nullable(), wind_onshore_actual_d_minus_2_mw: number,
    wind_offshore_day_ahead_forecast_mw: number.nullable(), wind_offshore_actual_d_minus_2_mw: number,
    residual_load_day_ahead_forecast_mw: number.nullable(), residual_load_actual_d_minus_2_mw: number,
    day_of_week: z.number().int(), month: z.number().int(), season: z.string(),
    is_weekend: z.boolean(), is_holiday_de_nationwide: z.boolean(), is_holiday_lu: z.boolean(),
  })),
})

export const weatherSchema = z.object({
  delivery_date: dateSchema, model_run_date: dateSchema,
  model: z.literal('ecmwf_ifs'), location_count: z.number().int().positive(),
  quarters: grid(z.object({
    quarter_of_day: quarter,
    temperature_2m_c: number, wind_speed_100m_m_s: number,
    shortwave_radiation_w_m2: number, cloud_cover_pct: number,
  })),
})

export const modelSchema = z.object({
  model_name: z.string(), model_family: z.string(), version: z.string(),
  training_through: dateSchema, published_at: z.string(), target_coverage: number,
  point_shrinkage: number, feature_data_version: z.number().int().positive().nullish(),
  test_metrics: z.record(z.string(), number), baseline_exaa_mae: number,
  reference_metrics: z.record(z.string(), number).nullish(),
  mae_gain_interval: z.array(number).length(3).nullish(),
  empirical_mae_gain_interval: z.array(number).length(3).nullish(),
})

export type DateSummary = z.infer<typeof datesSchema>[number]
export type Forecast = z.infer<typeof forecastSchema>
export type Observations = z.infer<typeof observationsSchema>
export type Features = z.infer<typeof featuresSchema>
export type Feature = Features['rows'][number]
export type Weather = z.infer<typeof weatherSchema>
export type WeatherQuarter = Weather['quarters'][number]
export type Metrics = z.infer<typeof metricsSchema>
export type Model = z.infer<typeof modelSchema>

export type PricePoint = {
  quarter: number
  actual: number | null
  forecast: number | null
  interval: [number, number] | null
  exaa: number | null
  austria: number | null
  actualSpread: number | null
  forecastSpread: number | null
  intervalSpread: [number, number] | null
}

export function pricePoints(day: Forecast | Observations, features?: Features): PricePoint[] {
  const inputs = new Map(features?.rows.map(row => [row.quarter_of_day, row]))
  return day.quarters.map(row => {
    const feature = inputs.get(row.quarter_of_day)
    const actual = row.actual_price_eur_per_mwh
    const forecast = 'predicted_price_eur_per_mwh' in row ? row.predicted_price_eur_per_mwh : null
    const interval = 'lower_price_eur_per_mwh' in row ? [row.lower_price_eur_per_mwh, row.upper_price_eur_per_mwh] as [number, number] : null
    const exaa = 'price_de_lu_exaa_eur_per_mwh' in row ? row.price_de_lu_exaa_eur_per_mwh : feature?.price_de_lu_exaa_eur_per_mwh ?? null
    return {
      quarter: row.quarter_of_day,
      actual,
      forecast,
      interval,
      exaa,
      austria: 'price_at_exaa_eur_per_mwh' in row ? row.price_at_exaa_eur_per_mwh : feature?.price_at_exaa_eur_per_mwh ?? null,
      actualSpread: actual == null || exaa == null ? null : actual - exaa,
      forecastSpread: forecast == null || exaa == null ? null : forecast - exaa,
      intervalSpread: interval == null || exaa == null ? null : [interval[0] - exaa, interval[1] - exaa] as [number, number],
    }
  }).sort((a, b) => a.quarter - b.quarter)
}

export async function fetchData<T>(url: string, schema: z.ZodType<T>): Promise<T> {
  const response = await fetch(url, { signal: AbortSignal.timeout(60_000), headers: { Accept: 'application/json' } })
  if (response.status === 401 || response.status === 403 || response.redirected) {
    throw new Error('Your session has expired. Reload the page to sign in again.')
  }
  if (!response.ok) throw new Error(response.status === 404 ? 'No data has been published for this date yet.' : 'Market data is temporarily unavailable. Please try again.')
  if (!response.headers.get('content-type')?.includes('application/json')) throw new Error('The data connection is unavailable. Reload the page to reconnect.')
  const result = schema.safeParse(await response.json())
  if (!result.success) throw new Error('This dataset is incomplete or has an unexpected format. Please try again later.')
  return result.data
}

const decimals = new Intl.NumberFormat('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
export const formatNumber = (value: number | null | undefined) => value == null ? 'Not available' : decimals.format(value)
export const percent = (value: number) => `${(value * 100).toFixed(1)}%`
export const timeOfQuarter = (value: number) => `${String(Math.floor(value / 4)).padStart(2, '0')}:${String((value % 4) * 15).padStart(2, '0')}`
export const timeRange = (value: number) => `${timeOfQuarter(value)} - ${timeOfQuarter(value + 1)}`
export const formatDate = (value: string, style: 'long' | 'short' = 'long') => new Intl.DateTimeFormat('en-GB', {
  day: '2-digit', month: style, year: 'numeric', timeZone: 'UTC',
}).format(new Date(`${value}T12:00:00Z`))
export const formatTimestamp = (value: string) => new Intl.DateTimeFormat('en-GB', {
  day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Berlin', timeZoneName: 'short',
}).format(new Date(value))
export const todayBerlin = () => new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/Berlin', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date())

export function dayStatus(day: DateSummary) {
  if (!day.has_forecast) return { label: 'Historical observation', tone: 'neutral' }
  return day.settled ? { label: 'Settled', tone: 'settled' } : { label: 'Forecast only', tone: 'pending' }
}

export function publicationLabel(status: DateSummary['publication_status']) {
  if (status === 'on_time') return 'On-time forecast'
  if (status === 'late') return 'Late forecast'
  if (status === 'backfill') return 'Backfilled forecast'
  return 'Publication time unavailable'
}
