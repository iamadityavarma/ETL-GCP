from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,   # we'll control via EmailOperator for clarity
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="etl_pipeline_daily",
    default_args=default_args,
    description="ETL pipeline: extractor → loader → validator",
    schedule_interval="0 5 * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    PROJECT_ID = Variable.get("PROJECT_ID")
    REGION = Variable.get("REGION")
    ALERT_EMAILS = [e.strip() for e in Variable.get("ALERT_EMAILS").split(",")]  # ensure list

    run_extractor = CloudRunExecuteJobOperator(
        task_id="run_data_extractor",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="data-extractor",
        # gcp_conn_id="google_cloud_default",  # uncomment if you use a non-default conn
    )

    run_loader = CloudRunExecuteJobOperator(
        task_id="run_data_loader",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="data-loader",
    )

    run_validator = CloudRunExecuteJobOperator(
        task_id="run_data_validator",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="data-validator",
    )

    # Fires if ANY upstream task failed (extractor OR loader OR validator)
    failure_alert = EmailOperator(
        task_id="send_failure_email",
        to=ALERT_EMAILS,
        subject="ETL Pipeline Failed",
        html_content="""
            <h3> ETL Pipeline Execution Failed</h3>
            <p>Check task logs in Airflow for details.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Optional: success email only when ALL three succeed
    success_alert = EmailOperator(
        task_id="send_success_email",
        to=ALERT_EMAILS,
        subject="ETL Pipeline Succeeded",
        html_content="<h3>All ETL steps completed successfully.</h3>",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Order of core tasks stays sequential
    run_extractor >> run_loader >> run_validator

    # Connect all tasks to failure alert (ONE_FAILED will trigger if any fail)
    [run_extractor, run_loader, run_validator] >> failure_alert

    # Success alert only runs if validator succeeds (which means all succeeded)
    run_validator >> success_alert
