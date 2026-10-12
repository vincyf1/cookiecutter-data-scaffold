import type { Config, DagSnapshot, DashboardSnapshot } from './types'

async function json<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(path, options)
  if (!(response.headers.get('content-type') || '').includes('json')) {
    throw new Error('API server returned a page instead of data. Restart the webapp server to load the dashboard route.')
  }
  const body = await response.json()
  if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`)
  return body as T
}

export const getConfig = () => json<Config>('/api/config')
export const getDag = () => json<DagSnapshot>('/api/dag')
export const getDashboard = (period: '7' | '30' | '90' | 'all', signal?: AbortSignal) => json<DashboardSnapshot>(`/api/dashboard?period=${period}`, { signal })
export const getOpenAIHealth = () => json<{ status: 'unconfigured' | 'ready' | 'error' | 'model_error' }>('/api/health/openai')
export const callTool = (name: string, args: Record<string, string>) => json<unknown>('/api/tool', {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, args }),
})
export type DelegatedResult = { answer: string; evidence: { name: string; args: Record<string, string>; result: unknown }[]; model: string; reasoningEffort: string }
export type DelegateUpdate =
  | { type: 'step'; name: string; args: Record<string, string> }
  | { type: 'evidence'; name: string; args: Record<string, string>; result: unknown }

// The server streams NDJSON: step/evidence updates while Terra works, then one result or error line.
export async function delegateInvestigation(request: string, onUpdate: (update: DelegateUpdate) => void = () => {}): Promise<DelegatedResult> {
  const response = await fetch('/api/delegate', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ request }),
  })
  if (!response.ok || !response.body) throw new Error(`Delegation request failed (${response.status}). Restart the webapp server if it was just updated.`)
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader()
  let buffer = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (value) buffer += value
    let newline
    while ((newline = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, newline).trim()
      buffer = buffer.slice(newline + 1)
      if (!line) continue
      const message = JSON.parse(line)
      if (message.type === 'result') return message.result as DelegatedResult
      if (message.type === 'error') throw new Error(message.error)
      onUpdate(message as DelegateUpdate)
    }
    if (done) throw new Error('The delegated workflow ended without a result.')
  }
}
export const rerunTask = (task_id: string) => json<unknown>('/api/rerun', {
  method: 'POST', headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ dag_id: 'customer_revenue_daily', task_id, confirm: true }),
})
