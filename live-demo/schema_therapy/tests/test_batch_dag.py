from schema_therapy.batch.dags.example_dag import example_events_dag


def test_dag_has_no_import_errors():
    dag = example_events_dag()
    assert dag.dag_id == "example_events_dag"
    assert len(dag.tasks) == 2


def test_customer_revenue_dag_transforms_after_ingestion():
    from schema_therapy.batch.dags.customer_revenue_daily import customer_revenue_daily

    dag = customer_revenue_daily()
    assert dag.dag_id == "customer_revenue_daily"
    assert dag.schedule == "@daily"
    assert {task.task_id for task in dag.tasks} == {"ingest_supabase", "transform_with_dbt"}
    assert dag.get_task("transform_with_dbt").upstream_task_ids == {"ingest_supabase"}
