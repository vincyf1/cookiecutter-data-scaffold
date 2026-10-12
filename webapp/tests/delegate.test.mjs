import assert from 'node:assert/strict'
import test from 'node:test'
import { createDelegate } from '../server/delegate.mjs'

test('offers staged editing and deployment, preserving multiline edit arguments', async () => {
  const requests = []
  const calls = []
  const args = { path: 'src/schema_therapy/batch/dags/customer_revenue_daily.py', expected_sha256: 'a'.repeat(64), old_text: '    old()\n', new_text: '    new()\n' }
  const replies = [
    { output: [{ type: 'function_call', name: 'edit_airflow_file', arguments: JSON.stringify(args), call_id: 'edit' }] },
    { output: [{ type: 'message', content: [{ type: 'output_text', text: 'The DAG edit is staged, not deployed.' }] }] },
  ]
  const delegate = createDelegate({
    apiKey: 'test-key', executeTool: async (name, args) => { calls.push({ name, args }); return { staged: true, deployed: false } },
    fetchImpl: async (_url, options) => { requests.push(JSON.parse(options.body)); return { ok: true, json: async () => replies.shift() } },
  })
  const result = await delegate('Update the downstream DAG and deploy it.')
  assert.deepEqual(calls, [{ name: 'edit_airflow_file', args }])
  for (const name of ['list_airflow_files', 'read_airflow_file', 'validate_airflow_deployment', 'deploy_airflow_changes', 'get_airflow_deployment_status']) {
    assert.ok(requests[0].tools.some(tool => tool.name === name), name)
  }
  assert.match(requests[0].instructions, /staged/)
  assert.equal(result.evidence[0].result.deployed, false)
})

test('a workflow limit preserves evidence from actions that already succeeded', async () => {
  const delegate = createDelegate({
    apiKey: 'test-key', executeTool: async () => ({ staged: true }),
    fetchImpl: async () => ({ ok: true, json: async () => ({ output: [{ type: 'function_call', name: 'get_dag_status', arguments: '{"dag_id":"customer_revenue_daily"}', call_id: 'call' }] }) }),
  })
  const result = await delegate('Inspect the workflow')
  assert.equal(result.evidence.length, 24)
  assert.match(result.answer, /limit/)
  assert.match(result.answer, /status/)
})

test('delegates Airflow recovery and keeps source writes outside its tool set', async () => {
  const requests = []
  const executed = []
  const replies = [
    { output: [{ type: 'function_call', name: 'trigger_airflow_dag', arguments: '{"dag_id":"customer_revenue_daily"}', call_id: 'trigger' }] },
    { output: [{ type: 'message', content: [{ type: 'output_text', text: 'New run queued; validation is pending.' }] }] },
  ]
  const delegate = createDelegate({
    apiKey: 'test-key', executeTool: async (name, args) => { executed.push({ name, args }); return { run_id: 'new', state: 'queued' } },
    fetchImpl: async (_url, options) => { requests.push(JSON.parse(options.body)); return { ok: true, json: async () => replies.shift() } },
  })
  const result = await delegate('The downstream patch is deployed. Run the full DAG to validate.')
  assert.deepEqual(executed, [{ name: 'trigger_airflow_dag', args: { dag_id: 'customer_revenue_daily' } }])
  assert.ok(requests[0].tools.some(tool => tool.name === 'rerun_airflow_task'))
  assert.ok(!requests[0].tools.some(tool => /alter|write|update.*supabase/.test(tool.name)))
  assert.match(requests[0].instructions, /Source changes remain exclusively in presenter control/)
  assert.equal(result.evidence[0].result.state, 'queued')
})

test('uses Terra at medium effort and returns tool evidence', async () => {
  const requests = []
  const replies = [
    { output: [{ type: 'function_call', name: 'get_dag_status', arguments: '{"dag_id":"customer_revenue_daily"}', call_id: 'call_1' }] },
    { output: [{ type: 'message', content: [{ type: 'output_text', text: 'The latest DAG run failed at transform_with_dbt.' }] }] },
  ]
  const delegate = createDelegate({
    apiKey: 'test-key',
    executeTool: async () => ({ run: { state: 'failed' } }),
    fetchImpl: async (_url, options) => {
      requests.push(JSON.parse(options.body))
      return { ok: true, json: async () => replies.shift() }
    },
  })
  const result = await delegate('Why did revenue fail?')
  assert.equal(requests[0].model, 'gpt-5.6-terra')
  assert.equal(requests[0].reasoning.effort, 'medium')
  assert.equal(requests[1].input.at(-1).type, 'function_call_output')
  assert.equal(result.evidence[0].name, 'get_dag_status')
  assert.match(result.answer, /failed at transform_with_dbt/)
})

test('rejects unknown delegated tool calls without executing them', async () => {
  let executed = false
  const replies = [
    { output: [{ type: 'function_call', name: 'alter_supabase_source', arguments: '{}', call_id: 'call_2' }] },
    { output: [{ type: 'message', content: [{ type: 'output_text', text: 'Source changes are presenter-controlled.' }] }] },
  ]
  const delegate = createDelegate({
    apiKey: 'test-key',
    executeTool: async () => { executed = true },
    fetchImpl: async () => ({ ok: true, json: async () => replies.shift() }),
  })
  const result = await delegate('Rerun this')
  assert.equal(executed, false)
  assert.match(result.evidence[0].result.error, /Unknown delegated tool/)
})

test('offers and executes Airflow history and live Supabase tools', async () => {
  const calls = []
  const requests = []
  const replies = [
    { output: [
      { type: 'function_call', name: 'get_airflow_runs', arguments: '{"dag_id":"customer_revenue_daily"}', call_id: 'runs' },
      { type: 'function_call', name: 'get_supabase_schema', arguments: '{"dataset":"customers"}', call_id: 'schema' },
    ] },
    { output: [{ type: 'message', content: [{ type: 'output_text', text: 'The source has customer_type.' }] }] },
  ]
  const delegate = createDelegate({
    apiKey: 'test-key',
    executeTool: async (name, args) => { calls.push({ name, args }); return { ok: true } },
    fetchImpl: async (_url, options) => {
      requests.push(JSON.parse(options.body))
      return { ok: true, json: async () => replies.shift() }
    },
  })
  const result = await delegate('Compare Airflow and Supabase')
  assert.ok(requests[0].tools.some(tool => tool.name === 'get_airflow_runs'))
  assert.ok(requests[0].tools.some(tool => tool.name === 'get_supabase_schema'))
  assert.ok(requests[0].tools.some(tool => tool.name === 'get_supabase_source_summary'))
  assert.deepEqual(calls.map(call => call.name), ['get_airflow_runs', 'get_supabase_schema'])
  assert.equal(result.evidence.length, 2)
})

test('reports each step before it runs and its evidence after, even if the listener throws', async () => {
  const updates = []
  const replies = [
    { output: [{ type: 'function_call', name: 'get_task_logs', arguments: '{"dag_id":"customer_revenue_daily","task_id":"ingest_supabase"}', call_id: 'logs' }] },
    { output: [{ type: 'message', content: [{ type: 'output_text', text: 'Missing column.' }] }] },
  ]
  const delegate = createDelegate({
    apiKey: 'test-key',
    executeTool: async () => { updates.push('executed'); return { logs: 'column "customer_segment" does not exist' } },
    fetchImpl: async () => ({ ok: true, json: async () => replies.shift() }),
  })
  const result = await delegate('Why did it fail?', { onProgress: update => { updates.push(update.type); throw new Error('listener bug') } })
  assert.deepEqual(updates, ['step', 'executed', 'evidence'])
  assert.equal(result.answer, 'Missing column.')
})
