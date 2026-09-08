import { CalendarDays, ChevronLeft, ChevronRight, Download, LoaderCircle, RefreshCw } from 'lucide-react'
import { useState } from 'react'
import type { ReactNode } from 'react'
import { dayStatus, formatDate } from './data'
import type { DateSummary } from './data'

export function DatePicker({ days, date, onChange }: { days: DateSummary[]; date: string; onChange: (value: string) => void }) {
  const move = (offset: number) => {
    const value = new Date(`${date}T00:00:00Z`)
    value.setUTCDate(value.getUTCDate() + offset)
    return value.toISOString().slice(0, 10)
  }
  const minimum = days.at(-1)?.delivery_date
  const maximum = days[0]?.delivery_date
  const previous = minimum && date > minimum ? move(-1) : undefined
  const next = maximum && date < maximum ? move(1) : undefined
  return <div className="date-picker">
    <button type="button" className="icon-button" aria-label="Previous day" disabled={!previous} onClick={() => previous && onChange(previous)}><ChevronLeft size={15} /></button>
    <label className="date-input-label">
      <CalendarDays size={14} className="text-muted" />
      <span>{formatDate(date)}</span>
      <input aria-label="Delivery date" type="date" value={date} min={days.at(-1)?.delivery_date} max={days[0]?.delivery_date} onChange={event => event.target.value && onChange(event.target.value)} />
    </label>
    <button type="button" className="icon-button" aria-label="Next day" disabled={!next} onClick={() => next && onChange(next)}><ChevronRight size={15} /></button>
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

export function GitHubLink({ className }: { className?: string }) {
  return (
    <a href="https://github.com/bhanuprasanna2001/delu" target="_blank" rel="noreferrer" aria-label="View the source on GitHub" className={`github-link${className ? ` ${className}` : ''}`}>
      <svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor" aria-hidden="true">
        <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8Z" />
      </svg>
    </a>
  )
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
