import assert from 'node:assert/strict'
import test from 'node:test'
import { createWorkspaceTools } from '../server/airflow_workspace_tools.mjs'

test('deployment refuses active runs without publishing files', async () => {
  let executed = false
  const execute = createWorkspaceTools({
    airflow: async () => ({ dag_runs: [{ dag_run_id: 'active', state: 'running' }] }),
    pythonTool: async () => { executed = true },
  })
  await assert.rejects(execute('deploy_airflow_changes', { dag_id: 'customer_revenue_daily', workspace_revision: 'a'.repeat(64) }), /active/)
  assert.equal(executed, false)
})

test('deploys a validated workspace and reports serialized DAG metadata', async () => {
  const paths = []
  const execute = createWorkspaceTools({
    airflow: async path => { paths.push(path); return path.includes('dagRuns') ? { dag_runs: [] } : { dag_id: 'customer_revenue_daily', last_parsed_time: 'today', fileloc: '/opt/airflow/dags/customer_revenue_daily.py' } },
    pythonTool: async () => ({ deployed: true }),
  })
  const result = await execute('deploy_airflow_changes', { dag_id: 'customer_revenue_daily', workspace_revision: 'a'.repeat(64) })
  assert.equal(result.deployed, true)
  assert.equal(result.serialized_dag.last_parsed_time, 'today')
  assert.match(paths[0], /state=running&state=queued/)
})

test('invalid DAG or unexpected arguments cannot reach the deployment bridge', async () => {
  const execute = createWorkspaceTools({ airflow: async () => assert.fail(), pythonTool: async () => assert.fail() })
  await assert.rejects(execute('deploy_airflow_changes', { dag_id: 'another', workspace_revision: 'a'.repeat(64) }), /Invalid/)
  await assert.rejects(execute('read_airflow_file', { path: '.env', command: 'anything' }), /Invalid/)
})
