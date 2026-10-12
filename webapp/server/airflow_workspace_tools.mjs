export const WORKSPACE_ARGS = {
  list_airflow_files: ['dag_id'],
  read_airflow_file: ['path'],
  edit_airflow_file: ['path', 'expected_sha256', 'old_text', 'new_text'],
  validate_airflow_deployment: ['dag_id'],
  deploy_airflow_changes: ['dag_id', 'workspace_revision'],
  discard_airflow_changes: ['dag_id', 'workspace_revision'],
  get_airflow_deployment_status: ['dag_id'],
}

export function validateWorkspaceArgs(name, args) {
  const keys = WORKSPACE_ARGS[name]
  if (!keys || !args || typeof args !== 'object' || Array.isArray(args) ||
      Object.keys(args).length !== keys.length || !keys.every(key => typeof args[key] === 'string') ||
      (keys.includes('dag_id') && args.dag_id !== 'customer_revenue_daily') ||
      (keys.includes('path') && (!args.path || args.path.length > 500 || !/^[A-Za-z0-9_./-]+$/.test(args.path))) ||
      (keys.includes('expected_sha256') && !/^[a-f0-9]{64}$/.test(args.expected_sha256)) ||
      (keys.includes('workspace_revision') && !/^[a-f0-9]{64}$/.test(args.workspace_revision)) ||
      (keys.includes('old_text') && (!args.old_text || Buffer.byteLength(args.old_text) > 60000 || Buffer.byteLength(args.new_text) > 60000))) {
    throw new Error(`Invalid arguments for ${name}`)
  }
  return args
}

export function createWorkspaceTools({ airflow, pythonTool }) {
  return async function executeWorkspaceTool(name, args) {
    validateWorkspaceArgs(name, args)
    if (name === 'deploy_airflow_changes') {
      const active = await airflow('/dags/customer_revenue_daily/dagRuns?state=running&state=queued&limit=1')
      if (active.dag_runs?.length) throw new Error(`Airflow run ${active.dag_runs[0].dag_run_id} is active; wait before deploying.`)
    }
    const result = await pythonTool(name, args)
    if (['deploy_airflow_changes', 'get_airflow_deployment_status'].includes(name)) {
      try {
        const dag = await airflow('/dags/customer_revenue_daily')
        result.serialized_dag = { dag_id: dag.dag_id, last_parsed_time: dag.last_parsed_time, fileloc: dag.fileloc, is_paused: dag.is_paused }
      } catch (error) {
        result.serialized_dag_error = error instanceof Error ? error.message : String(error)
      }
    }
    return result
  }
}
