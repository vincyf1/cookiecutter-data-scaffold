// Speakable descriptions of the delegate's tool calls. GPT-Live paraphrases these
// while Terra works, so each says what is being checked and why it matters, never a result.

const STEPS: Record<string, (args: Record<string, string>) => string> = {
  get_dag_status: () => 'Terra is checking the latest customer revenue DAG run and its task states, the quickest way to see where the pipeline stopped.',
  get_airflow_runs: () => 'Terra is comparing the recent Airflow runs to see when things went from green to red.',
  get_task_logs: args => `Terra is reading the ${args.task_id || 'task'} log, where the exact failing query or error shows up.`,
  get_supabase_schema: args => `Terra is reading the live Supabase ${args.dataset || 'source'} columns to see what the source actually provides right now.`,
  get_supabase_source_summary: args => `Terra is sampling safe row counts from Supabase ${args.dataset || 'source'} to confirm the data itself still looks healthy.`,
  get_lineage: args => `Terra is tracing dbt lineage for ${args.model || 'the model'} to map the blast radius downstream.`,
  get_recent_schema_changes: () => 'Terra is comparing the declared migrations with the columns DuckDB holds today.',
  get_recent_git_changes: () => 'Terra is checking recent code changes for anything that could have moved the contract.',
  read_dbt_model: args => `Terra is reading the SQL for ${args.model || 'a dbt model'} to see which columns it depends on.`,
  run_dbt_tests: args => `Terra is running the dbt tests for ${args.model || 'the model'}, the proof that the data is right, not just present.`,
  list_airflow_files: () => 'Terra is listing the downstream files it is allowed to change: ingestion, the DuckDB writer, dbt models, and tests.',
  read_airflow_file: args => `Terra is reading ${shortPath(args.path)} before touching it, so any edit is based on the real file.`,
  edit_airflow_file: args => `Terra is staging a small edit to ${shortPath(args.path)}. Nothing is live yet.`,
  validate_airflow_deployment: () => 'Terra is validating the staged changes in a disposable copy inside the Airflow image: imports, DAG wiring, and dbt parsing.',
  deploy_airflow_changes: () => 'Terra is deploying the validated revision to Airflow and checking the mounted files match.',
  discard_airflow_changes: () => 'Terra is discarding a stale staged patch so it can start again from the current files.',
  get_airflow_deployment_status: () => 'Terra is confirming the deployed files and the DAG Airflow actually loaded.',
  trigger_airflow_dag: () => 'Terra is starting a fresh DAG run from ingestion. Queued is a start; green tasks are the evidence.',
  rerun_airflow_task: args => `Terra is retrying ${args.task_id || 'the failed task'} and everything downstream of it.`,
}

function shortPath(path = '') {
  return path.split('/').slice(-2).join('/') || 'a pipeline file'
}

export function describeStep(name: string, args: Record<string, string> = {}): string {
  return STEPS[name]?.(args) ?? `Terra is running ${name.replaceAll('_', ' ')}.`
}
