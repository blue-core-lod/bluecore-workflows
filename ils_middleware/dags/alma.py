from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ils_middleware.tasks.amazon.alma_work_s3 import (
    send_work_to_alma_s3,
)
from ils_middleware.tasks.amazon.alma_instance_s3 import (
    send_instance_to_alma_s3,
)

from ils_middleware.tasks.sinopia.local_metadata import new_local_admin_metadata
from ils_middleware.tasks.sinopia.email import (
    notify_and_log,
    send_update_success_emails,
)
from ils_middleware.tasks.sinopia.login import sinopia_login
from ils_middleware.tasks.alma.post_bfwork import NewWorktoAlma
from ils_middleware.tasks.alma.post_bfinstance import NewInstancetoAlma
from ils_middleware.tasks.general import parse_messages, message_from_context


def task_failure_callback(ctx_dict) -> None:
    notify_and_log("Error executing task", ctx_dict)


def dag_failure_callback(ctx_dict) -> None:
    notify_and_log("Error executing DAG", ctx_dict)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provider": None,
    "on_failure_callback": task_failure_callback,
}

institutions = [
    "ucdavis",
    "penn",
]


for institution in institutions:
    with DAG(
        f"{institution}",
        default_args=default_args,
        description=f"{institution} Alma DAG",
        start_date=datetime(2021, 8, 24),
        tags=[f"{institution}-alma"],
        catchup=False,
        schedule=None,
        on_failure_callback=dag_failure_callback,
        max_active_runs=1,
    ) as dag:
        get_messages = PythonOperator(
            task_id="get-message-from-context", python_callable=message_from_context
        )

        process_message = PythonOperator(
            task_id="api-message-parse",
            python_callable=parse_messages,
            dag=dag,
            op_kwargs={"message": "{{ ti.xcom_pull('get-message-from-context') }}"},
        )

        with TaskGroup(group_id="process_alma", dag=dag) as alma_task_group:
            export_work_bf_xml = PythonOperator(
                task_id="bf_work_xml_to_s3",
                python_callable=send_work_to_alma_s3,
            )

            export_instance_bf_xml = PythonOperator(
                task_id="bf_instance_xml_to_s3",
                python_callable=send_instance_to_alma_s3,
            )

            alma_post_work = PythonOperator(
                task_id="post_work",
                python_callable=NewWorktoAlma,
            )

            alma_post_instance = PythonOperator(
                task_id="post_instance",
                python_callable=NewInstancetoAlma,
            )

            (
                export_work_bf_xml
                >> export_instance_bf_xml
                >> alma_post_work
                >> alma_post_instance
            )
        # EmptyOperator
        processed_sinopia = EmptyOperator(
            task_id="processed_sinopia", dag=dag, trigger_rule="none_failed"
        )

        with TaskGroup(group_id="update_sinopia") as sinopia_update_group:
            # Sinopia Login
            login_sinopia = PythonOperator(
                task_id="sinopia-login",
                python_callable=sinopia_login,
                op_kwargs={
                    "region": "us-west-2",
                    "sinopia_env": Variable.get("sinopia_env"),
                },
            )

            # Adds localAdminMetadata
            local_admin_metadata = PythonOperator(
                task_id="sinopia-new-metadata",
                python_callable=new_local_admin_metadata,
                op_kwargs={
                    "jwt": "{{ task_instance.xcom_pull(task_ids='update_sinopia.sinopia-login', key='return_value') }}",
                    "ils_tasks": {"ALMA": ["process_alma.post_new_alma"]},
                },
            )

            login_sinopia >> local_admin_metadata

        notify_sinopia_updated = PythonOperator(
            task_id="sinopia_update_success_notification",
            dag=dag,
            trigger_rule="none_failed",
            python_callable=send_update_success_emails,
        )

    processing_complete = EmptyOperator(task_id="processing_complete", dag=dag)
    messages_received = EmptyOperator(task_id="messages_received", dag=dag)

    get_messages >> messages_received >> process_message
    process_message >> alma_task_group >> processed_sinopia
    processed_sinopia >> sinopia_update_group >> notify_sinopia_updated
    notify_sinopia_updated >> processing_complete
