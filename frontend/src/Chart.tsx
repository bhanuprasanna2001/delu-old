import { useId, useState } from 'react'
import { Area, CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { formatNumber, timeOfQuarter, timeRange } from './data'

export const colors = {
  forecast: 'var(--color-forecast)', actual: 'var(--color-ink)',
  exaa: 'var(--color-exaa)', austria: 'var(--color-austria)', prior: 'var(--color-prior)',
}
export type Series = { key: string; label: string; color: string; style?: 'band' | 'dashed'; hidden?: boolean }
export type ChartPoint = { quarter: number; [key: string]: number | [number, number] | null }

type Props = {
  points: ChartPoint[]
  series: Series[]
  unit: string
  label: string
  yDomain?: [number | 'auto', number | 'auto']
  onInspect?: () => void
}

export default function Chart({ points, series, unit, label, yDomain, onInspect }: Props) {
  const pattern = useId().replace(/:/g, '')
  const [hidden, setHidden] = useState(() => new Set(series.filter(item => item.hidden).map(item => item.key)))
  const visible = series.filter(item => !hidden.has(item.key))
  const available = series.filter(item => points.some(point => point[item.key] != null))

  function toggle(key: string) {
    setHidden(current => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else if (available.filter(item => !next.has(item.key)).length > 1) next.add(key)
      return next
    })
  }

  return (
    <div className="chart" aria-label={label}>
      <div className="flex items-center justify-between px-1 pb-3 text-[10px] tracking-wide text-muted">
        <span>{unit}</span><span>15-minute intervals</span>
      </div>
      <div className="chart-canvas">
        <ResponsiveContainer width="100%" height="100%" minWidth={0}>
          <ComposedChart data={points} margin={{ top: 9, right: 15, left: -15, bottom: 0 }} accessibilityLayer onClick={onInspect}>
            <defs>
              <pattern id={pattern} patternUnits="userSpaceOnUse" width="5" height="5" patternTransform="rotate(45)">
                <rect width="5" height="5" fill="var(--color-band)" />
                <line x1="0" y1="0" x2="0" y2="5" stroke="var(--color-forecast)" strokeOpacity="0.12" strokeWidth="1" />
              </pattern>
            </defs>
            <CartesianGrid vertical={false} stroke="var(--color-line)" strokeDasharray="2 5" />
            <XAxis dataKey="quarter" type="number" domain={[0, 95]} ticks={[0, 16, 32, 48, 64, 80, 95]} tickFormatter={timeOfQuarter} tickLine={false} axisLine={false} tickMargin={15} minTickGap={30} tick={{ fontSize: 10, fill: 'var(--color-muted)' }} height={36} />
            <YAxis domain={yDomain ?? (unit === 'GW' ? [0, 'auto'] : ['auto', 'auto'])} tickCount={5} tickLine={false} axisLine={false} tickMargin={9} tick={{ fontSize: 10, fill: 'var(--color-muted)' }} tickFormatter={value => Number(value).toLocaleString('en-GB', { maximumFractionDigits: 0 })} width={55} />
            <ReferenceLine y={0} stroke="var(--color-line)" />
            <Tooltip
              isAnimationActive={false}
              cursor={{ stroke: 'var(--color-muted)', strokeDasharray: '3 4', strokeWidth: 1 }}
              offset={18}
              wrapperStyle={{ zIndex: 10, outline: 'none', pointerEvents: 'none' }}
              content={({ active, label: quarter }) => {
                const point = points.find(row => row.quarter === Number(quarter))
                if (!active || !point) return null
                return (
                  <div className="chart-tooltip">
                    <div className="mb-3 flex items-center justify-between gap-7 border-b border-line pb-2.5">
                      <strong className="font-medium tabular-nums">{timeRange(point.quarter)}</strong>
                      <span className="text-[10px] text-muted">Berlin time</span>
                    </div>
                    {visible.map(item => {
                      const value = point[item.key]
                      if (value == null) return null
                      return <div key={item.key} className="my-2 flex items-center justify-between gap-6 text-xs">
                        <span className="flex items-center gap-2 text-muted"><span className="size-1.5 rounded-full" style={{ background: item.color }} />{item.label}</span>
                        <span className="font-medium tabular-nums">{Array.isArray(value) ? `${formatNumber(value[0])} to ${formatNumber(value[1])}` : formatNumber(value)}</span>
                      </div>
                    })}
                    <div className="mt-3 text-right text-[10px] text-muted">{unit}</div>
                  </div>
                )
              }}
            />
            {series.map(item => item.style === 'band' ? (
              <Area key={item.key} dataKey={item.key} name={item.label} type="linear" hide={hidden.has(item.key)} stroke="none" fill={`url(#${pattern})`} fillOpacity={1} isAnimationActive={false} activeDot={false} connectNulls={false} />
            ) : (
              <Line key={item.key} dataKey={item.key} name={item.label} type="linear" hide={hidden.has(item.key)} stroke={item.color} strokeWidth={item.key === 'forecast' ? 2.1 : 1.6} strokeDasharray={item.style === 'dashed' ? '5 4' : undefined} dot={false} activeDot={{ r: 4, stroke: 'var(--color-paper)', strokeWidth: 2 }} isAnimationActive={false} connectNulls={false} />
            ))}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
      <div className="chart-legend" aria-label="Chart series">
        {available.map(item => <button type="button" key={item.key} aria-pressed={!hidden.has(item.key)} onClick={() => toggle(item.key)} className={`legend-item ${hidden.has(item.key) ? 'legend-hidden' : ''}`}>
          <span className={`legend-mark ${item.style === 'band' ? 'legend-band' : ''}`} style={{ color: item.color, borderStyle: item.style === 'dashed' ? 'dashed' : 'solid' }} />
          {item.label}
        </button>)}
      </div>
      <p className="sr-only">Focus the plot and use the arrow keys to inspect each quarter-hour. Use the series buttons to show or hide a line.</p>
    </div>
  )
}
