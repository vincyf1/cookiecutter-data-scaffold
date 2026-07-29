CREATE TABLE IF NOT EXISTS agent_traces (
    trace_id VARCHAR PRIMARY KEY,
    dag_id VARCHAR,
    dag_run_id VARCHAR,
    task_id VARCHAR,
    try_number INTEGER,
    occurred_at TIMESTAMP,
    status VARCHAR,
    elements JSON
);
