import { ArrowDownRight, ArrowLeft, ArrowUpRight } from 'lucide-react'
import { lazy, Suspense, useEffect, useMemo, useState } from 'react'
import useSWR from 'swr'
import Chart, { colors } from './Chart'
import { dateSchema, datesSchema, fetchData, featuresSchema, forecastSchema, formatDate, formatTimestamp, observationsSchema, percent, pricePoints, publishedAfterDelivery, todayBerlin } from './data'
import type { DateSummary } from './data'
import { DataError, DatePicker, GitHubLink, Loading, Status } from './ui'

const Detail = lazy(() => import('./Detail'))
const readLocation = () => {
  const params = new URLSearchParams(window.location.search)
  const date = dateSchema.safeParse(params.get('date'))
  return { date: date.success ? date.data : '', detail: params.get('view') === 'detail' }
}

export default function App() {
  const [location, setLocation] = useState(readLocation)
  const dates = useSWR('/api/dates?limit=2000', url => fetchData(url, datesSchema), { refreshInterval: 60_000 })
  const defaultDate = dates.data?.find(day => day.has_forecast)?.delivery_date ?? dates.data?.[0]?.delivery_date
  const selectedDate = location.date || defaultDate
  const selectedDay = dates.data?.find(day => day.delivery_date === selectedDate)

  useEffect(() => {
    const pop = () => setLocation(readLocation())
    window.addEventListener('popstate', pop)
    return () => window.removeEventListener('popstate', pop)
  }, [])

  function navigate(date: string, detail = location.detail) {
    const url = new URL(window.location.href)
    url.searchParams.set('date', date)
    if (detail) url.searchParams.set('view', 'detail')
    else url.searchParams.delete('view')
    window.history.pushState(null, '', url)
    setLocation({ date, detail })
    if (detail !== location.detail) window.scrollTo({ top: 0, behavior: 'instant' })
  }

  const picker = selectedDate && dates.data ? <DatePicker days={dates.data} date={selectedDate} onChange={date => navigate(date)} /> : null
  const error = dates.error && !dates.data
  const content = error ? <DataError error={dates.error} retry={() => void dates.mutate()} />
    : !dates.data ? <Loading />
      : !selectedDay ? <div className="data-state"><span className="eyebrow">No published data</span><p>{dates.data.length ? 'There is no complete dataset for this date.' : 'The first forecast will appear after the scheduled 11:30 run.'}</p>{defaultDate ? <button type="button" className="secondary-button" onClick={() => navigate(defaultDate)}>Go to latest available day</button> : null}</div>
        : <DayView key={selectedDay.delivery_date} day={selectedDay} detail={location.detail} onExpand={() => navigate(selectedDay.delivery_date, true)} />

  if (location.detail) return <main className="detail-page">
    <header className="detail-header">
      <div className="flex items-center gap-5"><button type="button" className="icon-button back-button" aria-label="Back to overview" onClick={() => navigate(selectedDate ?? '', false)}><ArrowLeft size={17} /></button><a href="/" className="wordmark wordmark-small" onClick={event => { event.preventDefault(); navigate(selectedDate ?? '', false) }}>DELU</a><span className="header-divider" /><span className="hidden text-xs text-muted sm:block">The day ahead, in detail.</span></div>
      <div className="flex items-center gap-4"><GitHubLink /><span className="market-label">DE <span className="text-line">/</span> LU</span></div>
    </header>
    <div className="detail-heading"><div><div className="eyebrow mb-2">The daily perspective</div><h1>{selectedDate ? formatDate(selectedDate) : 'Market overview'}</h1></div>{picker}</div>
    {content}
    <footer className="site-footer"><span className="flex items-center gap-2"><span className="wordmark wordmark-tiny">DELU</span><span>Germany & Luxembourg</span></span><span>Source: ENTSO-E · <a href="https://open-meteo.com/" target="_blank" rel="noreferrer">Open-Meteo</a> · All times Europe/Berlin</span></footer>
  </main>

  return <main className="overview-page">
    <GitHubLink className="overview-github" />
    <div className="overview-content">
      <header className="overview-brand"><h1 className="wordmark">DELU</h1><p>Clarity for the day ahead.</p></header>
      <div className="overview-date">{picker}</div>
      {content}
    </div>
  </main>
}

function DayView({ day, detail, onExpand }: { day: DateSummary; detail: boolean; onExpand: () => void }) {
  const date = day.delivery_date
  const forecast = useSWR(day.has_forecast ? `/api/forecasts/${date}` : null, url => fetchData(url, forecastSchema), {
    refreshInterval: data => data?.settled && data.metrics ? 0 : 60_000,
  })
  const observations = useSWR(!day.has_forecast ? `/api/observations/${date}` : null, url => fetchData(url, observationsSchema))
  const features = useSWR(`/api/forecasts/${date}/features`, url => fetchData(url, featuresSchema))
  const result = day.has_forecast ? forecast : observations
  const points = useMemo(() => result.data ? pricePoints(result.data, features.data) : [], [result.data, features.data])
  const actualDay = forecast.data ? { ...day, settled: forecast.data.settled } : day
  const stale = date < todayBerlin()
  const series = [
    { key: 'interval', label: `${percent(forecast.data?.nominal_coverage ?? 0.9).replace('.0%', '%')} prediction interval`, color: colors.forecast, style: 'band' as const },
    { key: 'exaa', label: 'EXAA DE-LU', color: colors.exaa, style: 'dashed' as const },
    { key: 'austria', label: 'EXAA AT', color: colors.austria, style: 'dashed' as const, hidden: true },
    { key: 'actual', label: 'Actual SDAC', color: colors.actual },
    { key: 'forecast', label: 'DELU forecast', color: colors.forecast },
  ]
  const chart = <section className={`price-panel ${detail ? 'price-panel-detail' : 'price-panel-overview'}`} aria-label="Day-ahead electricity prices">
    <div className="price-panel-heading">
      <div><div className="eyebrow mb-1.5">Germany & Luxembourg</div><h2>Day-ahead electricity price</h2></div>
      <div className="flex items-center gap-3"><Status day={actualDay} />{!detail ? <button type="button" className="icon-button expand-button" aria-label="Open detailed view" onClick={onExpand} onPointerEnter={() => void import('./Detail')} onFocus={() => void import('./Detail')}><ArrowUpRight size={17} /></button> : null}</div>
    </div>
    {result.error && !result.data ? <DataError error={result.error} retry={() => void result.mutate()} /> : !result.data ? <Loading /> : <Chart points={points} series={series} unit="EUR / MWh" label={`Electricity prices for ${date}`} onInspect={!detail ? onExpand : undefined} />}
    <div className="price-panel-footer">
      <span>{!day.has_forecast ? 'Observed prices · No stored model forecast' : forecast.data ? `Published ${formatTimestamp(forecast.data.predicted_at)}` : 'Forecast 11:30 · Settlement from 15:00'}</span>
      {detail ? <span>Europe/Berlin</span> : <button type="button" className="explore-button" onClick={onExpand} onPointerEnter={() => void import('./Detail')}>Explore this day <ArrowDownRight size={13} /></button>}
    </div>
    {publishedAfterDelivery(forecast.data ?? day) ? <p className="inline-note">Published after the delivery day. This is a retrospective run.</p> : null}
    {features.error && !features.data && day.has_forecast ? <div className="inline-note" aria-live="polite">EXAA inputs are unavailable. <button onClick={() => void features.mutate()} className="underline underline-offset-2">Retry inputs</button></div> : null}
    {result.error && result.data ? <div className="inline-note" aria-live="polite">Showing the last loaded data. Refresh failed. <button onClick={() => void result.mutate()} className="underline underline-offset-2">Retry</button></div> : null}
  </section>

  if (!detail) return <>{chart}{stale ? <p className="overview-note">Viewing {formatDate(date, 'short')} · Forecasts at 11:30, settlement from 15:00 Berlin time</p> : <p className="overview-note">Forecasts at 11:30 · Settlement from 15:00 · Europe/Berlin</p>}</>
  return <>
    <div className="detail-plot-grid">{chart}<div aria-hidden="true" /></div>
    <Suspense fallback={<Loading>Loading the daily details</Loading>}>
      <Detail day={actualDay} forecast={forecast.data} points={points} features={features.data} featuresError={features.error} retryFeatures={() => void features.mutate()} />
    </Suspense>
  </>
}
