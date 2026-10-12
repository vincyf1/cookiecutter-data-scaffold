import assert from 'node:assert/strict'
import test from 'node:test'
import { createAirflowActions } from '../server/airflow_actions.mjs'

const dag_id = 'customer_revenue_daily'
function mockAirflow(replies) {
  const calls = []
  return { calls, action: createAirflowActions({ airflow: async (path, options) => {
    calls.push({ path, options }); return replies.shift()
  } }) }
}

test('triggers a fresh DAG with the Airflow v2 required logical_date field', async () => {
  const { action, calls } = mockAirflow([{ dag_runs: [] }, { dag_run_id: 'manual__new', state: 'queued' }])
  const result = await action('trigger_airflow_dag', { dag_id })
  assert.deepEqual(calls[1], { path: `/dags/${dag_id}/dagRuns`, options: { method: 'POST', body: { logical_date: null, conf: {} } } })
  assert.equal(result.run_id, 'manual__new')
  assert.equal(result.state, 'queued')
})

test('retry scopes clearing to the latest failed run and its blocked downstream tasks', async () => {
  const { action, calls } = mockAirflow([
    { dag_runs: [{ dag_run_id: 'manual__failure+01', state: 'failed' }] },
    { task_instances: [{ task_id: 'ingest_supabase', state: 'failed' }] },
    { task_instances: [{ task_id: 'ingest_supabase' }, { task_id: 'transform_with_dbt' }] },
  ])
  const result = await action('rerun_airflow_task', { dag_id, task_id: 'ingest_supabase' })
  assert.match(calls[1].path, /manual__failure%2B01/)
  assert.deepEqual(calls[2].options.body, {
    dag_run_id: 'manual__failure+01', task_ids: ['ingest_supabase'], dry_run: false,
    only_failed: false, reset_dag_runs: true, include_downstream: true,
    include_upstream: false, include_future: false, include_past: false, prevent_running_task: true,
  })
  assert.deepEqual(result.cleared_tasks, ['ingest_supabase', 'transform_with_dbt'])
})

test('rejects other DAGs, unknown tasks and extra arguments before calling Airflow', async () => {
  const { action, calls } = mockAirflow([])
  for (const [name, args] of [
    ['trigger_airflow_dag', { dag_id: 'other' }],
    ['rerun_airflow_task', { dag_id, task_id: 'other' }],
    ['trigger_airflow_dag', { dag_id, conf: {} }],
  ]) await assert.rejects(action(name, args), /Unknown Airflow/)
  assert.equal(calls.length, 0)
})

test('does not write while a run is active or when the selected task is not failed', async () => {
  for (const name of ['trigger_airflow_dag', 'rerun_airflow_task']) {
    const { action, calls } = mockAirflow([{ dag_runs: [{ dag_run_id: 'active', state: 'running' }] }])
    await assert.rejects(action(name, name === 'trigger_airflow_dag' ? { dag_id } : { dag_id, task_id: 'ingest_supabase' }), /running/)
    assert.equal(calls.length, 1)
  }
  const { action, calls } = mockAirflow([
    { dag_runs: [{ dag_run_id: 'failed', state: 'failed' }] },
    { task_instances: [{ task_id: 'transform_with_dbt', state: 'upstream_failed' }] },
  ])
  await assert.rejects(action('rerun_airflow_task', { dag_id, task_id: 'transform_with_dbt' }), /Only a failed task/)
  assert.equal(calls.length, 2)
})
