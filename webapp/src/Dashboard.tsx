import { useCallback, useEffect, useState } from 'react'
import { ArrowUpRight, Database, RefreshCw } from 'lucide-react'
import { getDashboard } from './api'
import type { DashboardGroup, DashboardPoint, DashboardSnapshot } from './types'
import './dashboard.css'

type Period = '7' | '30' | '90' | 'all'
const periods: { value: Period; label: string }[] = [
  { value: '7', label: '7 days' }, { value: '30', label: '30 days' },
  { value: '90', label: '90 days' }, { value: 'all', label: 'All' },
]
const money = (value: number) => new Intl.NumberFormat('en-AU', { style: 'currency', currency: 'AUD', maximumFractionDigits: 0 }).format(value)
const number = (value: number) => new Intl.NumberFormat('en-AU').format(value)
const shortDate = (value: string) => new Date(`${value}T00:00:00Z`).toLocaleDateString('en-AU', { day: 'numeric', month: 'short', timeZone: 'UTC' })

function RevenueTrend({ daily }: { daily: DashboardPoint[] }) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null)
  const width = 520, height = 238, left = 62, right = 12, top = 16, bottom = 36
  const plotWidth = width - left - right, plotHeight = height - top - bottom
  const max = Math.max(1, ...daily.map(point => point.revenueAud))
  const points = daily.map((point, index) => ({
    ...point,
    x: left + (daily.length === 1 ? plotWidth / 2 : index * plotWidth / (daily.length - 1)),
    y: top + plotHeight * (1 - point.revenueAud / max),
  }))
  const line = points.map((point, index) => `${index ? 'L' : 'M'} ${point.x.toFixed(1)} ${point.y.toFixed(1)}`).join(' ')
  const fill = points.length ? `${line} L ${points.at(-1)!.x.toFixed(1)} ${top + plotHeight} L ${points[0].x.toFixed(1)} ${top + plotHeight} Z` : ''
  const ticks = [0, 0.5, 1]
  const active = activeIndex === null ? null : points[activeIndex]

  return <section className="dashboard-panel dashboard-trend" aria-labelledby="revenue-trend-title">
    <div className="dashboard-panel-head"><div><h3 id="revenue-trend-title">Revenue over time</h3><p>Completed sales by UTC date</p></div><span className="dashboard-panel-unit">AUD / DAY</span></div>
    <div className="dashboard-chart-wrap">
    <svg viewBox={`0 0 ${width} ${height}`} role="img" tabIndex={0} aria-label={`Daily completed revenue from ${shortDate(daily[0].date)} to ${shortDate(daily.at(-1)!.date)}. Use left and right arrow keys to explore dates.`} preserveAspectRatio="none" onPointerMove={event => {
      const box = event.currentTarget.getBoundingClientRect()
      const x = (event.clientX - box.left) / box.width * width
      setActiveIndex(Math.max(0, Math.min(points.length - 1, Math.round((x - left) / plotWidth * (points.length - 1)))))
    }} onPointerLeave={() => setActiveIndex(null)} onFocus={() => setActiveIndex(index => index ?? points.length - 1)} onBlur={() => setActiveIndex(null)} onKeyDown={event => {
      if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return
      event.preventDefault()
      setActiveIndex(index => Math.max(0, Math.min(points.length - 1, (index ?? points.length - 1) + (event.key === 'ArrowRight' ? 1 : -1))))
    }}>
      {ticks.map(tick => {
        const y = top + plotHeight * (1 - tick)
        return <g key={tick}><line x1={left} x2={width - right} y1={y} y2={y} className="dashboard-gridline"/><text x={left - 10} y={y + 4} textAnchor="end" className="dashboard-axis-label">{money(max * tick)}</text></g>
      })}
      {fill && <path d={fill} className="dashboard-area"/>}
      {line && <path d={line} className="dashboard-line"/>}
      {points.map((point, index) => <circle key={point.date} cx={point.x} cy={point.y} r={index === activeIndex ? 5 : 3.5} className={index === activeIndex ? 'dashboard-point active' : 'dashboard-point'}/>)}
      {[points[0], points[Math.floor((points.length - 1) / 2)], points.at(-1)].filter((point, index, array) => point && array.findIndex(item => item?.date === point.date) === index).map(point =>
        <text key={point!.date} x={point!.x} y={height - 10} textAnchor={point!.x === points[0].x ? 'start' : point!.x === points.at(-1)!.x ? 'end' : 'middle'} className="dashboard-axis-label">{shortDate(point!.date)}</text>
      )}
    </svg>
    {active && <div className={`dashboard-tooltip ${active.y < 94 ? 'below' : ''}`} role="tooltip" style={{ left: `${Math.max(22, Math.min(78, active.x / width * 100))}%`, top: `${active.y / height * 100}%` }}><span className="dashboard-tooltip-label">{shortDate(active.date)} · UTC</span><strong className="dashboard-tooltip-value">{money(active.revenueAud)}</strong><div className="dashboard-tooltip-row"><span>Transactions</span><b>{number(active.completedTransactions)}</b></div><div className="dashboard-tooltip-row"><span>Customers</span><b>{number(active.purchasingCustomers)}</b></div></div>}
    </div>
    <details className="dashboard-data-details"><summary>View daily values</summary><div className="dashboard-table-wrap"><table><thead><tr><th>Date (UTC)</th><th>Revenue</th><th>Transactions</th></tr></thead><tbody>{daily.map(point => <tr key={point.date}><td>{point.date}</td><td>{money(point.revenueAud)}</td><td>{number(point.completedTransactions)}</td></tr>)}</tbody></table></div></details>
  </section>
}

function Breakdown({ title, caption, rows, total, tone }: { title: string; caption: string; rows: DashboardGroup[]; total: number; tone: 'type' | 'region' }) {
  return <section className="dashboard-panel dashboard-breakdown" aria-label={title}>
    <div className="dashboard-panel-head"><div><h3>{title}</h3><p>{caption}</p></div></div>
    <div className="dashboard-bars">{rows.map((row, index) => {
      const share = total ? row.revenueAud / total * 100 : 0
      return <div className="dashboard-bar-row" key={row.name}>
        <div className="dashboard-bar-label"><strong>{row.name}</strong><span>{money(row.revenueAud)}</span></div>
        <div className="dashboard-bar-track" role="meter" aria-label={`${row.name} share of revenue`} aria-valuenow={Math.round(share)} aria-valuemin={0} aria-valuemax={100}><span className={`${tone} tone-${index % 4}`} style={{ width: `${share}%` }}/></div>
        <div className="dashboard-bar-meta"><span>{share.toFixed(1)}% of revenue</span><span>{number(row.purchasingCustomers)} customers</span></div>
      </div>
    })}</div>
  </section>
}

export default function Dashboard() {
  const [period, setPeriod] = useState<Period>('30')
  const [revision, setRevision] = useState(0)
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(true)
  const refresh = useCallback(() => setRevision(value => value + 1), [])

  useEffect(() => {
    const controller = new AbortController()
    setLoading(true)
    setError('')
    getDashboard(period, controller.signal)
      .then(setSnapshot)
      .catch(problem => { if (!controller.signal.aborted) setError(problem instanceof Error ? problem.message : String(problem)) })
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [period, revision])

  const data = snapshot?.available ? snapshot : null
  return <div className="dashboard-view">
    <div className="dashboard-toolbar"><div className="dashboard-source"><Database size={14}/><strong>DUCKDB</strong><span>customer_revenue_daily</span></div><div className="dashboard-controls"><div className="dashboard-periods" aria-label="Revenue period">{periods.map(item => <button key={item.value} className={period === item.value ? 'active' : ''} aria-pressed={period === item.value} onClick={() => setPeriod(item.value)}>{item.label}</button>)}</div><button className="dashboard-refresh" onClick={refresh} disabled={loading} aria-label="Refresh dashboard" title="Refresh dashboard"><RefreshCw size={15}/></button></div></div>
    {error ? <div className="dashboard-message"><h3>Could not read the revenue mart</h3><p>{error}</p><button onClick={refresh}>Try again</button></div> : loading ? <div className="dashboard-message" role="status"><h3>Reading DuckDB…</h3><p>Loading the latest customer revenue snapshot.</p></div> : !data ? <div className="dashboard-message"><h3>Dashboard is waiting for data</h3><p>{snapshot?.available === false ? snapshot.message : 'Run the customer_revenue_daily DAG, then refresh this view.'}</p><button onClick={refresh}>Refresh dashboard</button></div> : <>
      <div className="dashboard-context"><span>{shortDate(data.firstDate)} — {shortDate(data.latestDate)} · UTC</span><span>Current mart column: <strong>{data.customerTypeColumn || 'unavailable'}</strong></span></div>
      <div className="dashboard-metrics"><div><span>COMPLETED REVENUE</span><strong>{money(data.summary.revenueAud)}</strong><small>AUD across selected dates</small></div><div><span>COMPLETED TRANSACTIONS</span><strong>{number(data.summary.completedTransactions)}</strong><small>Failed sales excluded</small></div><div><span>PURCHASING CUSTOMERS</span><strong>{number(data.summary.purchasingCustomers)}</strong><small>Distinct customers</small></div><div><span>CUSTOMER DAYS</span><strong>{number(data.summary.customerDays)}</strong><small>Rows in the revenue mart</small></div></div>
      <div className="dashboard-main-grid"><RevenueTrend daily={data.daily}/><Breakdown title={data.customerTypeColumn === 'customer_type' ? 'Revenue by customer type' : 'Revenue by customer segment'} caption="Share of completed revenue" rows={data.customerTypes} total={data.summary.revenueAud} tone="type"/></div>
      <div className="dashboard-region-section"><Breakdown title="Revenue by region" caption="Where purchasing customers are based" rows={data.regions} total={data.summary.revenueAud} tone="region"/></div>
      <div className="dashboard-provenance"><ArrowUpRight size={14}/><span>Read directly from the local DuckDB mart. Refresh after a successful DAG run to see rebuilt results.</span></div>
    </>}
  </div>
}
