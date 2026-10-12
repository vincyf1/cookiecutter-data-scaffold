const DAG = 'customer_revenue_daily'
const TASKS = ['ingest_supabase', 'transform_with_dbt']

export function createAirflowActions({ airflow }) {
  return async function executeAirflowAction(name, args) {
    const keys = name === 'trigger_airflow_dag' ? ['dag_id'] : ['dag_id', 'task_id']
    if (!['trigger_airflow_dag', 'rerun_airflow_task'].includes(name) ||
        !args || Object.keys(args).length !== keys.length || args.dag_id !== DAG ||
        (name === 'rerun_airflow_task' && !TASKS.includes(args.task_id))) {
      throw new Error('Unknown Airflow action, DAG, task, or arguments')
    }
    const runs = await airflow(`/dags/${DAG}/dagRuns?limit=1&order_by=-start_date`)
    const latest = runs.dag_runs?.[0]
    if (latest && ['queued', 'running'].includes(latest.state)) {
      throw new Error(`Run ${latest.dag_run_id} is ${latest.state}; check its status before starting another run.`)
    }
    if (name === 'trigger_airflow_dag') {
      const run = await airflow(`/dags/${DAG}/dagRuns`, {
        method: 'POST', body: { logical_date: null, conf: {} },
      })
      return { dag_id: DAG, run_id: run.dag_run_id, state: run.state, action: 'triggered', message: 'Fresh DAG run requested. Check task states and logs to verify completion.' }
    }
    if (!latest || latest.state !== 'failed') throw new Error('The latest DAG run must be failed before rerunning a task.')
    const instances = await airflow(`/dags/${DAG}/dagRuns/${encodeURIComponent(latest.dag_run_id)}/taskInstances`)
    const task = instances.task_instances?.find(item => item.task_id === args.task_id)
    if (task?.state !== 'failed') throw new Error('Only a failed task in the latest failed run can be rerun.')
    const result = await airflow(`/dags/${DAG}/clearTaskInstances`, {
      method: 'POST', body: {
        dag_run_id: latest.dag_run_id, task_ids: [args.task_id], dry_run: false,
        only_failed: false, reset_dag_runs: true, include_downstream: true,
        include_upstream: false, include_future: false, include_past: false,
        prevent_running_task: true,
      },
    })
    return { dag_id: DAG, task_id: args.task_id, run_id: latest.dag_run_id, action: 'cleared',
      message: 'Failed task and downstream tasks cleared for rerun. Completion is not yet verified.',
      cleared_tasks: (result.task_instances || []).map(item => item.task_id) }
  }
}
