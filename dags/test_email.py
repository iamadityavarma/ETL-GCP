from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

with DAG(
    dag_id="test_email_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Get email from Airflow variable
    ALERT_EMAILS = [e.strip() for e in Variable.get("ALERT_EMAILS").split(",")]

    test_email = EmailOperator(
        task_id="send_test_email",
        to=ALERT_EMAILS,  # Now uses Airflow variable
        subject="Airflow Email Test",
        html_content="<h3>Success!</h3><p>Your Airflow email setup is working</p>",
        conn_id="smtp_default",  # must match the connection you created
    )
