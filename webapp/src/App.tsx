import { useCallback, useEffect, useRef, useState } from 'react'
import { createClient } from '@supabase/supabase-js'
import { Activity, ArrowDownRight, ArrowRight, AudioLines, Check, CircleAlert, Database, GitBranch, Mic, MicOff, Play, Radio, RefreshCw, Send, Terminal, X } from 'lucide-react'
import { describeStep } from './progress'
import { callTool, delegateInvestigation, getConfig, getDag, getOpenAIHealth, rerunTask } from './api'
import { VoiceSession, type VoiceEvent } from './voice'
import Dashboard from './Dashboard'
import EvidenceCode from './EvidenceCode'
import { fileLanguage } from './evidence-format'
import type { Activity as ActivityItem, Config, DagSnapshot, Evidence } from './types'

const DAG = 'customer_revenue_daily'
const stages = ['Detect', 'Diagnose', 'Plan', 'Act', 'Validate']
const names: Record<string, string> = {
  get_dag_status: 'DAG status', get_task_logs: 'Task logs', get_lineage: 'Model lineage',
  get_airflow_runs: 'Airflow runs', get_supabase_schema: 'Supabase schema',
  get_supabase_source_summary: 'Supabase source',
  get_recent_schema_changes: 'Schema history', get_recent_git_changes: 'Recent changes',
  read_dbt_model: 'dbt model', run_dbt_tests: 'dbt tests', propose_patch: 'Proposed patch',
  rerun_airflow_task: 'Task rerun', trigger_airflow_dag: 'Fresh DAG run',
  list_airflow_files: 'Downstream files', read_airflow_file: 'Pipeline source',
  edit_airflow_file: 'Staged patch', validate_airflow_deployment: 'Deployment validation',
  deploy_airflow_changes: 'Airflow deployment', get_airflow_deployment_status: 'Deployment status',
  discard_airflow_changes: 'Discarded staged edits',
}
const time = () => new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
const label = (name: string) => names[name] || name.replaceAll('_', ' ')
const id = () => crypto.randomUUID()

function phaseFor(evidence: Evidence[]) {
  if (evidence.some(item => ['rerun_airflow_task', 'trigger_airflow_dag'].includes(item.name) && !(item.result as Record<string, unknown>)?.error)) return 4
  if (evidence.some(item => item.name === 'deploy_airflow_changes' && (item.result as Record<string, unknown>)?.deployed === true)) return 4
  if (evidence.some(item => ['propose_patch', 'edit_airflow_file', 'validate_airflow_deployment'].includes(item.name) && !(item.result as Record<string, unknown>)?.error)) return 3
  if (evidence.some(item => ['get_task_logs', 'get_lineage', 'get_recent_schema_changes', 'get_supabase_schema', 'get_supabase_source_summary', 'read_dbt_model'].includes(item.name))) return 2
  if (evidence.length) return 1
  return 0
}

function renderResult(item: Evidence) {
  const value = item.result as Record<string, any>
  if (!value || typeof value !== 'object') return <EvidenceCode value={item.result} language="text"/>
  if (value.error) return <EvidenceCode value={value.error} language="log" title="Tool error"/>
  if (item.name === 'get_dag_status') {
    const run = value.run || value.latest_run
    const tasks = value.tasks || []
    return <div className="result-stack"><div className="result-pair"><span>Latest run</span><strong>{run?.state || 'No run'}</strong></div><div className="result-pair"><span>Run ID</span><code>{run?.dag_run_id || run?.run_id || '—'}</code></div>{tasks.map((task: any) => <div className="result-pair" key={task.task_id}><span>{task.task_id}</span><strong className={`state-${task.state}`}>{task.state}</strong></div>)}</div>
  }
  if (item.name === 'get_airflow_runs') return <div className="result-stack">{(value.runs || []).map((run: any) => <div className="result-pair" key={run.run_id}><span>{run.run_id}</span><strong className={`state-${run.state}`}>{run.state}</strong></div>)}</div>
  if (item.name === 'get_supabase_schema' || item.name === 'get_supabase_source_summary') return <div className="result-stack"><div className="result-pair"><span>Live source</span><strong>{value.table}</strong></div><div className="result-pair"><span>Observed</span><strong>{value.observed_at}</strong></div>{value.row_count !== undefined && <div className="result-pair"><span>Rows</span><strong>{value.row_count}</strong></div>}{value.category_column && <div className="result-pair"><span>Customer field</span><strong>{value.category_column}</strong></div>}{(value.columns || []).map((column: any) => <div className="result-pair" key={column.name}><span>{column.name}</span><code>{column.type}{column.nullable ? '' : ' · required'}</code></div>)}{(value.categories || []).map((category: any) => <div className="result-pair" key={category.value}><span>{category.value}</span><strong>{category.count} customers</strong></div>)}{(value.statuses || []).map((status: any) => <div className="result-pair" key={status.status}><span>{status.status}</span><strong>{status.count} transactions</strong></div>)}</div>
  if (item.name === 'get_task_logs') return <EvidenceCode value={value.logs || value.stdout || 'No log lines returned.'} language="log" title="Airflow task log"/>
  if (item.name === 'get_lineage') return <div className="lineage-result"><div><span>UPSTREAM MODELS</span><strong>{value.upstream_models?.join(', ') || 'None'}</strong></div><ArrowDownRight size={20}/><div><span>MODEL</span><strong>{value.model}</strong></div><ArrowDownRight size={20}/><div><span>DOWNSTREAM MODELS</span><strong>{value.downstream_models?.join(', ') || 'None'}</strong></div>{value.upstream_sources?.length > 0 && <small>Sources: {value.upstream_sources.join(', ')}</small>}</div>
  if (item.name === 'get_recent_schema_changes') return <div className="result-stack"><div className="result-pair"><span>Current columns</span><strong>{value.current_columns?.join(', ') || 'Unavailable'}</strong></div><div className="result-pair"><span>Migration files</span><strong>{value.migrations?.length || 0}</strong></div>{value.migrations?.map((migration: any) => <details key={migration.file}><summary>{migration.file}</summary><EvidenceCode value={migration.sql} language="sql" title="Migration SQL"/></details>)}{value.note && <p className="result-note">{value.note}</p>}</div>
  if (item.name === 'read_airflow_file') return <><p className="result-note">{value.path} · {value.staged ? 'Staged content' : 'Live content'}</p><EvidenceCode value={value.content} language={fileLanguage(value.path || '')} title={fileLanguage(value.path || '').toUpperCase()}/></>
  if (item.name === 'edit_airflow_file') return <><p className="result-note">Staged edit. Deployment is required to publish it.</p><EvidenceCode value={value.diff || 'No changes remain in this file.'} language="diff" title="Staged diff"/></>
  if (item.name === 'list_airflow_files') return <div className="result-stack">{(value.files || []).map((path: string) => <div className="result-pair" key={path}><code>{path}</code><strong>{value.staged_files?.includes(path) ? 'Staged' : 'Live'}</strong></div>)}</div>
  if (item.name === 'validate_airflow_deployment') return <div className="result-stack"><div className="result-pair"><span>DAG imports</span><strong>{value.valid ? 'Passed' : 'Failed'}</strong></div><div className="result-pair"><span>dbt parse</span><strong>{value.dbt_parse}</strong></div><p>{value.note}</p></div>
  if (['deploy_airflow_changes', 'get_airflow_deployment_status'].includes(item.name)) return <div className="result-stack"><div className="result-pair"><span>Deployment</span><strong>{value.deployed ? 'Published' : value.last_deployment ? 'Previously published' : 'No deployment recorded'}</strong></div><div className="result-pair"><span>Docker mounted files</span><strong>{value.runtime_files_match ? 'Match' : 'Mismatch'}</strong></div>{value.deployment_files_unchanged !== undefined && <div className="result-pair"><span>Recorded deployment unchanged</span><strong>{value.deployment_files_unchanged ? 'Yes' : 'No'}</strong></div>}{(value.files || value.staged_files || []).map((path: string) => <code key={path}>{path}</code>)}<p>{value.message}</p>{value.serialized_dag_error && <p className="result-error">{value.serialized_dag_error}</p>}</div>
  if (item.name === 'read_dbt_model') return <EvidenceCode value={value.sql} language="sql" title="dbt model SQL"/>
  if (item.name === 'propose_patch') return <><p className="result-note">Proposal only. No files were changed.</p><EvidenceCode value={value.patch} language="diff" title="Proposed diff"/></>
  if (['rerun_airflow_task', 'trigger_airflow_dag'].includes(item.name)) return <div className="result-stack"><div className="result-pair"><span>Action</span><strong>{value.action}</strong></div><div className="result-pair"><span>Run ID</span><code>{value.run_id}</code></div>{value.task_id && <div className="result-pair"><span>Task</span><strong>{value.task_id}</strong></div>}<p>{value.message}</p></div>
  if (item.name === 'run_dbt_tests') return <><div className="result-pair"><span>Exit code</span><strong>{value.exit_code ?? 'Unavailable'}</strong></div><EvidenceCode value={[value.stdout, value.stderr].filter(Boolean).join('\n') || value.error || value} language="log" title="dbt test output"/></>
  return <EvidenceCode value={value} language="json" title="Tool result JSON"/>
}

function App() {
  const [config, setConfig] = useState<Config | null>(null)
  const [dag, setDag] = useState<DagSnapshot | null>(null)
  const [dagError, setDagError] = useState('')
  const [broadcastStatus, setBroadcastStatus] = useState('Not configured')
  const [voiceStatus, setVoiceStatus] = useState('Voice off')
  const [voicePhase, setVoicePhase] = useState<'listening' | 'hearing' | 'speaking'>('listening')
  const [openaiHealth, setOpenaiHealth] = useState('checking')
  const [evidence, setEvidence] = useState<Evidence[]>([])
  const [activity, setActivity] = useState<ActivityItem[]>([{ id: id(), text: 'Dashboard ready. Connect voice or inspect the pipeline.', kind: 'system', at: time() }])
  const [resultView, setResultView] = useState<'readable' | 'json'>('readable')
  const [selected, setSelected] = useState<string | null>(null)
  const [leftView, setLeftView] = useState<'investigation' | 'dashboard'>(() => new URLSearchParams(window.location.search).get('view') === 'dashboard' ? 'dashboard' : 'investigation')
  const [prompt, setPrompt] = useState('')
  const [busy, setBusy] = useState(false)
  const [rerunning, setRerunning] = useState(false)
  const voice = useRef<VoiceSession | null>(null)
  const activityScroll = useRef<HTMLDivElement | null>(null)
  const followActivity = useRef(true)

  const log = useCallback((text: string, kind: ActivityItem['kind'] = 'system') => {
    setActivity(current => [...current.slice(-59), { id: id(), text, kind, at: time() }])
  }, [])
  const addEvidence = useCallback((name: string, args: Record<string, string>, result: unknown, preview = false) => {
    const entry: Evidence = { id: id(), name, args, result, at: time(), preview }
    setEvidence(current => [entry, ...current])
    setSelected(entry.id)
  }, [])
  const refreshDag = useCallback(async () => {
    try { setDag(await getDag()); setDagError('') }
    catch (error) { setDagError(error instanceof Error ? error.message : String(error)) }
  }, [])

  useEffect(() => {
    getConfig().then(setConfig).catch(error => log(error.message, 'error'))
    refreshDag()
    const timer = window.setInterval(refreshDag, 8000)
    return () => window.clearInterval(timer)
  }, [log, refreshDag])

  useEffect(() => {
    const check = () => getOpenAIHealth().then(result => setOpenaiHealth(result.status)).catch(() => setOpenaiHealth('error'))
    check()
    const timer = window.setInterval(check, 30000)
    return () => window.clearInterval(timer)
  }, [])

  useEffect(() => {
    if (!config?.supabaseUrl || !config.supabaseKey) return
    const client = createClient(config.supabaseUrl, config.supabaseKey)
    const channel = client.channel(config.broadcastTopic)
      .on('broadcast', { event: 'platform-event' }, ({ payload }) => {
        const message = payload?.message || payload?.status || payload?.type || 'Platform event received'
        log(String(message), 'system')
        if (payload?.tool_name && payload?.result) addEvidence(payload.tool_name, payload.args || {}, payload.result)
        refreshDag()
      })
      .subscribe(status => setBroadcastStatus(status === 'SUBSCRIBED' ? 'Connected' : status))
    return () => { client.removeChannel(channel) }
  }, [config, log, addEvidence, refreshDag])

  useEffect(() => {
    const container = activityScroll.current
    if (container && followActivity.current) container.scrollTop = container.scrollHeight
  }, [activity])
  useEffect(() => () => voice.current?.stop(), [])

  const runTool = useCallback(async (name: string, args: Record<string, string>) => {
    log(`Running ${label(name)}…`, 'tool')
    try {
      const result = await callTool(name, args)
      addEvidence(name, args, result)
      log(`${label(name)} returned evidence.`, 'tool')
      if (name === 'get_dag_status') refreshDag()
    } catch (error) {
      const result = { error: error instanceof Error ? error.message : String(error) }
      addEvidence(name, args, result)
      log(`${label(name)}: ${result.error}`, 'error')
    }
  }, [addEvidence, log, refreshDag])

  const runDelegation = useCallback(async (request: string, delegationId: string | null) => {
    log('Delegating workflow to gpt-5.6-terra · medium reasoning…', 'tool')
    voice.current?.watch(delegationId)
    try {
      const delegated = await delegateInvestigation(request, update => {
        if (update.type === 'evidence') return addEvidence(update.name, update.args, update.result)
        const step = describeStep(update.name, update.args)
        log(step, 'tool')
        voice.current?.progress(delegationId, step)
      })
      log(`Terra completed the workflow${delegated.evidence.length ? ` with ${delegated.evidence.length} tool result${delegated.evidence.length === 1 ? '' : 's'}` : ''}.`, 'tool')
      voice.current?.commentary(delegationId, delegated.answer)
      refreshDag()
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      log(`Delegation failed: ${message}`, 'error')
      voice.current?.commentary(delegationId, `The investigation could not complete: ${message}`)
    }
  }, [addEvidence, log, refreshDag])

  const onVoiceEvent = useCallback((event: VoiceEvent) => {
    if (event.type === 'status') setVoiceStatus(event.status)
    if (event.type === 'phase') setVoicePhase(event.phase)
    if (event.type === 'activity') log(event.text, event.kind)
    if (event.type === 'delegate') runDelegation(event.request, event.delegationId)
    if (event.type === 'ended') { voice.current = null; setVoicePhase('listening') }
  }, [log, runDelegation])

  async function toggleVoice() {
    if (voice.current) { voice.current.stop(); voice.current = null; setVoiceStatus('Voice off'); setVoicePhase('listening'); log('Voice session ended.'); return }
    const session = new VoiceSession(event => { if (voice.current === session) onVoiceEvent(event) })
    voice.current = session
    try { await session.start(); if (voice.current === session) log('Voice connection initialized. Ask the agent directly when you want it to investigate.', 'system') }
    catch { if (voice.current === session) { voice.current = null; setVoiceStatus('Voice off') } }
  }

  async function submitPrompt(event: React.FormEvent) {
    event.preventDefault()
    const value = prompt.trim()
    if (!value || busy) return
    setPrompt('')
    log(value, 'user')
    if (voice.current) { setBusy(true); try { await runDelegation(value, null) } finally { setBusy(false) } return }
    setBusy(true)
    try {
      const question = value.toLowerCase()
      if (question.includes('log') || question.includes('fail')) await runTool('get_task_logs', { dag_id: DAG, task_id: 'ingest_supabase' })
      else if (question.includes('lineage')) await runTool('get_lineage', { model: 'customer_revenue_daily' })
      else if (question.includes('schema') || question.includes('column')) await runTool('get_recent_schema_changes', { dataset: 'customers' })
      else await runTool('get_dag_status', { dag_id: DAG })
      log('Connect voice for conversational diagnosis. The requested evidence is shown on the left.', 'system')
    } finally { setBusy(false) }
  }

  function previewIncident() {
    addEvidence('get_task_logs', { dag_id: DAG, task_id: 'ingest_supabase' }, { logs: 'psycopg.errors.UndefinedColumn: column "customer_segment" does not exist\n\nThe source customers table now exposes customer_type. Ingestion still requests customer_segment.' }, true)
    log('Rehearsal preview: ingest_supabase failed after the customer_segment → customer_type rename.', 'system')
  }

  async function rerun() {
    const taskId = dag?.tasks.find(task => task.state === 'failed')?.task_id
    if (!taskId) return
    setRerunning(true)
    log(`Rerunning ${taskId}…`, 'tool')
    try {
      const result = await rerunTask(taskId)
      addEvidence('rerun_airflow_task', { dag_id: DAG, task_id: taskId }, result)
      log('Rerun request submitted to Airflow.', 'tool')
      refreshDag()
    } catch (error) { log(error instanceof Error ? error.message : String(error), 'error') }
    finally { setRerunning(false) }
  }

  const current = evidence.find(item => item.id === selected) || evidence[0]
  const phase = phaseFor(evidence)
  const failed = dag?.tasks.some(task => task.state === 'failed') || dag?.run?.state === 'failed'
  const voiceOn = voiceStatus === 'Listening for requests'
  const voiceActive = voiceStatus !== 'Voice off'
  const voiceDisplayPhase = voiceOn ? voicePhase : 'connecting'
  const voicePhaseLabel = voiceDisplayPhase === 'hearing' ? 'HEARING YOU' : voiceDisplayPhase === 'speaking' ? 'AGENT RESPONDING' : voiceDisplayPhase === 'connecting' ? 'ESTABLISHING SIGNAL' : 'LISTENING'
  const airflowState = dagError ? 'OFFLINE' : dag ? 'LIVE' : 'CHECKING'
  const supabaseState = broadcastStatus === 'Connected' ? 'LIVE' : !config?.supabaseKey ? 'UNSET' : ['CHANNEL_ERROR', 'TIMED_OUT', 'CLOSED'].includes(broadcastStatus) ? 'OFFLINE' : 'CONNECTING'
  const openaiState = config?.voiceConfigError ? 'MODEL ERROR' : voiceOn ? 'ACTIVE' : !config?.voiceAvailable ? 'UNSET' : openaiHealth === 'ready' ? 'READY' : openaiHealth === 'error' ? 'OFFLINE' : 'CHECKING'

  return <div className="app-shell">
    <header className="masthead">
      <div className="brand-mark"><Database size={19} strokeWidth={2.3}/><span className="mark-dot"/></div>
      <div className="brand-copy"><h1>Schema Therapy <span>Agentic Data Platform</span></h1><p>LIVE DATA OPERATIONS CONSOLE</p></div>
      <div className="header-right" aria-label="Service status">
        <span className={`service-badge ${airflowState === 'LIVE' ? 'healthy' : airflowState === 'OFFLINE' ? 'down' : 'pending'}`}><i/>AIRFLOW <strong>{airflowState}</strong></span>
        <span className={`service-badge ${supabaseState === 'LIVE' ? 'healthy' : supabaseState === 'UNSET' || supabaseState === 'OFFLINE' ? 'down' : 'pending'}`}><i/>SUPABASE <strong>{supabaseState}</strong></span>
        <span className={`service-badge ${openaiState === 'ACTIVE' || openaiState === 'READY' ? 'healthy' : openaiState === 'CHECKING' ? 'pending' : 'down'}`}><i/>OPENAI <strong>{openaiState}</strong></span>
      </div>
    </header>

    <main className={`workspace ${leftView === 'dashboard' ? 'dashboard-active' : ''}`}>
      <section className="evidence-pane" aria-label="Incident evidence">
        <div className="pane-topline"><div><span className="section-index">01 / DATA PLATFORM</span><h2>{leftView === 'dashboard' ? 'Revenue dashboard' : 'Investigation'}</h2></div><span className="pane-meta">{leftView === 'dashboard' ? 'LIVE MART VIEW' : evidence.length ? `${evidence.length} RESULT${evidence.length === 1 ? '' : 'S'}` : 'AWAITING SIGNAL'}</span></div>
        <nav className="left-view-switch" aria-label="Left panel view"><button className={leftView === 'investigation' ? 'active' : ''} aria-current={leftView === 'investigation' ? 'page' : undefined} onClick={() => setLeftView('investigation')}>Investigation{evidence.length > 0 && <span>{evidence.length}</span>}</button><button className={leftView === 'dashboard' ? 'active' : ''} aria-current={leftView === 'dashboard' ? 'page' : undefined} onClick={() => setLeftView('dashboard')}>Revenue dashboard</button></nav>
        {leftView === 'dashboard' ? <Dashboard/> : evidence.length === 0 ? <div className="empty-state"><div className="empty-orbit"><Activity size={35} strokeWidth={1.3}/></div><div className="empty-lines"><span/><span/><span/></div><h3>Nothing to investigate yet.</h3><p>Evidence from the agent’s tool calls will appear here as the incident unfolds.</p><div className="empty-actions"><button onClick={() => runTool('get_dag_status', { dag_id: DAG })} className="secondary-button"><RefreshCw size={15}/> Inspect pipeline</button><button onClick={previewIncident} className="text-button">Preview incident <ArrowRight size={15}/></button></div></div> : <div className="evidence-content"><div className="evidence-nav" role="tablist" aria-label="Evidence results">{evidence.map(item => <button role="tab" aria-selected={current?.id === item.id} className={current?.id === item.id ? 'evidence-tab active' : 'evidence-tab'} onClick={() => setSelected(item.id)} key={item.id}><span className="tab-icon">{item.name.includes('lineage') ? <GitBranch size={15}/> : item.name.includes('log') ? <Terminal size={15}/> : <Database size={15}/>}</span><span>{label(item.name)}</span>{item.preview && <em>PREVIEW</em>}</button>)}</div><article className="result-panel"><div className="result-header"><div><span className="result-kicker">{current.preview ? 'REHEARSAL DATA' : 'AGENT TOOL RESULT'}</span><h3>{label(current.name)}</h3></div><span className="result-time">{current.at}</span></div><div className="result-args">{Object.entries(current.args).map(([key, value]) => value.length > 100 || value.includes("\n") ? <details className="text-argument" key={key}><summary>{key} · {value.length} characters</summary><EvidenceCode value={value} language="text" title={key}/></details> : <span key={key}>{key} <b>{value}</b></span>)}</div><div className="result-view-switch" role="group" aria-label="Evidence format"><button aria-pressed={resultView === 'readable'} onClick={() => setResultView('readable')}>Readable</button><button aria-pressed={resultView === 'json'} onClick={() => setResultView('json')}>JSON</button></div><div className="result-body">{resultView === 'json' ? <EvidenceCode key={`${current.id}-json`} value={current.result} language="json" title="Tool result JSON"/> : <div key={current.id}>{renderResult(current)}</div>}</div></article></div>}
      </section>

      <aside className="agent-pane" aria-label="Agent status and activity">
        <div className="agent-head"><div className="agent-avatar"><AudioLines size={25}/></div><div><span className="section-index">02 / ACTIVE AGENT</span><h2>Schema Therapy Agent</h2></div><span className={`agent-indicator ${voiceOn ? 'connected' : ''}`}/></div>
        <div className="agent-status">{voiceActive && <div className={`voice-visual voice-visual--${voiceDisplayPhase}`}><div className="voice-visual-stage" aria-hidden="true"><span className="voice-orbit voice-orbit--outer"/><span className="voice-orbit voice-orbit--inner"/><div className="voice-wave">{Array.from({ length: 11 }, (_, index) => <span key={index} style={{ '--bar': index } as React.CSSProperties}/>)}</div></div><span className="voice-visual-label">{voicePhaseLabel}</span></div>}<div className="status-line"><span className={`status-pulse ${voiceOn ? 'active' : ''}`}/><strong>{voiceOn && voicePhase === 'hearing' ? 'Hearing you' : voiceOn && voicePhase === 'speaking' ? 'Agent responding' : voiceStatus}</strong><span className="status-hint">{voiceOn ? 'VOICE READY' : voiceActive ? 'VOICE CONNECTING' : 'READY WHEN YOU ARE'}</span></div><p>{voiceOn ? 'Present to your audience naturally. Ask the agent directly when you want it to inspect or steer the platform.' : voiceActive ? 'Setting up the voice connection. You can end the session at any time.' : 'Connect your microphone to speak with the agent during the demo.'}</p><button className={voice.current ? 'voice-button stop' : 'voice-button'} onClick={toggleVoice} disabled={!config?.voiceAvailable && !voice.current}>{voice.current ? <MicOff size={17}/> : <Mic size={17}/>} {voice.current ? 'End voice session' : 'Start voice session'} <ArrowRight size={16}/></button>{!config?.voiceAvailable && <small className="config-hint">{config?.voiceConfigError || 'Set OPENAI_API_KEY in webapp/.env to enable voice.'}</small>}<small className="delegate-hint">VOICE HOST · GPT-LIVE<br/>INVESTIGATION MODEL · {config?.delegateModel || 'gpt-5.6-terra'} / MEDIUM</small></div>
        <div className="pipeline-status"><div className="subsection-title"><span>PIPELINE HEALTH</span><button onClick={refreshDag} title="Refresh Airflow status" aria-label="Refresh Airflow status"><RefreshCw size={14}/></button></div><div className="pipeline-name"><span className={`pipeline-dot ${failed ? 'bad' : dagError ? 'muted' : ''}`}/><strong>{DAG}</strong><span>{dag?.run?.state || '—'}</span></div><div className="task-list">{(dag?.tasks || [{ task_id: 'ingest_supabase', state: 'none', try_number: 0 }, { task_id: 'transform_with_dbt', state: 'none', try_number: 0 }]).map(task => <div className="task-row" key={task.task_id}><span className={`task-symbol task-${task.state}`}>{task.state === 'success' ? <Check size={12}/> : task.state === 'failed' ? <X size={12}/> : <span/>}</span><span>{task.task_id}</span><em>{task.state === 'none' ? 'waiting' : task.state}</em></div>)}</div>{dagError && <div className="inline-error"><CircleAlert size={14}/>{dagError}</div>}</div>
        <div className="activity-feed"><div className="subsection-title"><span>AGENT ACTIVITY</span><span className="feed-live"><span/> LIVE FEED</span></div><div className="activity-scroll" ref={activityScroll} role="log" aria-live="polite" onScroll={event => { const node = event.currentTarget; followActivity.current = node.scrollHeight - node.scrollTop - node.clientHeight < 48 }}>{activity.map(item => <div className={`activity-item ${item.kind}`} key={item.id}><time>{item.at}</time><div className="activity-marker"/><p>{item.text}</p></div>)}</div></div>
        <form className="prompt-form" onSubmit={submitPrompt}><input aria-label="Ask the agent" placeholder="Ask the agent about the pipeline…" value={prompt} onChange={event => setPrompt(event.target.value)}/><button type="submit" disabled={!prompt.trim() || busy} aria-label="Send question"><Send size={17}/></button></form>
      </aside>
    </main>

    <footer className="workflow-footer"><div className="footer-title"><span className="footer-symbol"><Radio size={16}/></span><span>INCIDENT WORKFLOW</span><strong>Detect <ArrowRight size={13}/> Diagnose <ArrowRight size={13}/> Plan <ArrowRight size={13}/> Act <ArrowRight size={13}/> Validate</strong></div><div className="stage-track">{stages.map((stage, index) => <div className={`stage ${index < phase ? 'done' : index === phase ? 'current' : ''}`} key={stage}><span>{index < phase ? <Check size={12}/> : String(index + 1).padStart(2, '0')}</span><strong>{stage}</strong></div>)}</div>{failed && <div className="footer-right"><button className="rerun-button" onClick={rerun} disabled={rerunning}><Play size={12}/>{rerunning ? 'Rerunning…' : 'Rerun failed task'}</button></div>}</footer>
  </div>
}

export default App
