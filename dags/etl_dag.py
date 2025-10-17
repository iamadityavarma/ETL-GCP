from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
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
    ALERT_EMAILS = Variable.get("ALERT_EMAILS")

    run_extractor = CloudRunExecuteJobOperator(
        task_id="run_data_extractor",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="data-extractor",
        
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

    failure_alert = EmailOperator(
        task_id="send_failure_email",
        to=ALERT_EMAILS,
        subject="🚨 ETL Pipeline Failed",
        html_content="<h3>❌ ETL Pipeline Execution Failed</h3>",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    run_extractor >> run_loader >> run_validator >> failure_alert
