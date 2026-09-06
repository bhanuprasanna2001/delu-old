import { CalendarDays, ChevronLeft, ChevronRight, Download, LoaderCircle, RefreshCw } from 'lucide-react'
import { useState } from 'react'
import type { ReactNode } from 'react'
import { dayStatus, formatDate } from './data'
import type { DateSummary } from './data'

export function DatePicker({ days, date, onChange }: { days: DateSummary[]; date: string; onChange: (value: string) => void }) {
  const previous = days.find(day => day.delivery_date < date)?.delivery_date
  const next = days.findLast(day => day.delivery_date > date)?.delivery_date
  return <div className="date-picker">
    <button type="button" className="icon-button" aria-label="Previous available day" disabled={!previous} onClick={() => previous && onChange(previous)}><ChevronLeft size={15} /></button>
    <label className="date-input-label">
      <CalendarDays size={14} className="text-muted" />
      <span>{formatDate(date)}</span>
      <input aria-label="Delivery date" type="date" value={date} min={days.at(-1)?.delivery_date} max={days[0]?.delivery_date} onChange={event => event.target.value && onChange(event.target.value)} />
    </label>
    <button type="button" className="icon-button" aria-label="Next available day" disabled={!next} onClick={() => next && onChange(next)}><ChevronRight size={15} /></button>
  </div>
}

export function Status({ day }: { day: DateSummary }) {
  const status = dayStatus(day)
  return <span className={`status status-${status.tone}`}><span className="size-1.5 rounded-full bg-current" />{status.label}</span>
}

export function Loading({ children = 'Bringing the day into focus' }: { children?: ReactNode }) {
  return <output className="data-state"><LoaderCircle size={20} className="animate-spin text-muted" /><p className="font-medium text-ink">{children}</p><p className="max-w-xs text-xs leading-relaxed">We’re fetching the latest available data. The first visit may take a little longer.</p></output>
}

export function DataError({ error, retry }: { error: unknown; retry: () => void }) {
  return <div className="data-state" role="alert">
    <span className="eyebrow">Connection interrupted</span>
    <p className="max-w-sm leading-relaxed">{error instanceof Error ? error.message : 'Data is temporarily unavailable.'}</p>
    <button type="button" className="secondary-button" onClick={retry}><RefreshCw size={13} />Try again</button>
  </div>
}

export function SectionHeading({ number, title, children }: { number: string; title: string; children?: ReactNode }) {
  return <div className="section-heading"><div className="flex items-center gap-3"><span className="section-number">{number}</span><h2>{title}</h2></div>{children}</div>
}

export function DownloadButton({ href, filename, children, disabled = false }: { href: string; filename: string; children: ReactNode; disabled?: boolean }) {
  const [state, setState] = useState<'idle' | 'loading' | 'error'>('idle')
  async function download() {
    setState('loading')
    try {
      const response = await fetch(href, { signal: AbortSignal.timeout(120_000) })
      if (!response.ok || response.redirected || !response.headers.get('content-disposition')?.includes('attachment')) throw new Error('Download unavailable')
      const blob = await response.blob()
      const url = URL.createObjectURL(blob)
      const anchor = document.createElement('a')
      anchor.href = url
      anchor.download = filename
      anchor.click()
      setTimeout(() => URL.revokeObjectURL(url), 60_000)
      setState('idle')
    } catch {
      setState('error')
    }
  }
  return <div className="download-control">
    <button type="button" className="secondary-button" disabled={disabled || state === 'loading'} onClick={() => void download()}>
      {state === 'loading' ? <LoaderCircle size={14} className="animate-spin" /> : <Download size={14} />}
      {state === 'loading' ? 'Preparing download' : children}
    </button>
    {state === 'error' ? <span className="mt-2 block text-xs text-warning" role="alert">Download unavailable. Please try again.</span> : null}
  </div>
}
