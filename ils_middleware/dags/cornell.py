"""DAG for Cornell University Libraries."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ils_middleware.tasks.folio.build import build_records
from ils_middleware.tasks.folio.graph import construct_graph
from ils_middleware.tasks.folio.map import FOLIO_FIELDS, map_to_folio
from ils_middleware.tasks.folio.new import post_folio_records
from ils_middleware.tasks.sinopia.local_metadata import new_local_admin_metadata
from ils_middleware.tasks.sinopia.login import sinopia_login
from ils_middleware.tasks.sinopia.email import (
    notify_and_log,
    send_update_success_emails,
)
from ils_middleware.tasks.general import (
    message_from_context,
    parse_messages
)
from airflow.providers.standard.operators.empty import EmptyOperator


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
}

with DAG(
    "cornell",
    default_args=default_args,
    description="Cornell FOLIO DAG",
    start_date=datetime(2021, 8, 14),
    tags=["folio"],
    schedule=None,
    catchup=False,
) as dag:
    get_messages = PythonOperator(
        task_id="get-message-from-context", python_callable=message_from_context
    )

    process_message = PythonOperator(
        task_id="sqs-message-parse",
        python_callable=parse_messages,
    )

    bf_graphs = PythonOperator(task_id="bf-graph", python_callable=construct_graph)

    with TaskGroup(group_id="folio_mapping") as folio_map_task_group:
        for folio_field in FOLIO_FIELDS:
            bf_to_folio = PythonOperator(
                task_id=f"{folio_field}_task",
                python_callable=map_to_folio,
                op_kwargs={
                    "folio_field": folio_field,
                    "task_groups_ids": [""],
                },
            )

    folio_records = PythonOperator(
        task_id="build-folio",
        python_callable=build_records,
        op_kwargs={
            "task_groups": ["folio_mapping"],
            "folio_connection_id": "cornell_folio",
            "task_groups_ids": ["folio_mapping"],
        },
    )

    new_folio_records = PythonOperator(
        task_id="new-or-upsert-folio-records",
        python_callable=post_folio_records,
        op_kwargs={
            "task_groups_ids": [""],
            "folio_connection_id": "cornell_folio",
        },
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
                "ils_tasks": {
                    "FOLIO": [
                        "new-or-upsert-folio-records",
                    ]
                },
            },
        )

        login_sinopia >> local_admin_metadata

    notify_sinopia_updated = PythonOperator(
        task_id="sinopia_update_success_notification",
        dag=dag,
        trigger_rule="none_failed",
        python_callable=send_update_success_emails,
    )

    # Empty Operators
    messages_received = EmptyOperator(task_id="messages_received", dag=dag)
    processing_complete = EmptyOperator(task_id="processing_complete", dag=dag)
    processed_sinopia = EmptyOperator(
        task_id="processed_sinopia", dag=dag, trigger_rule="none_failed"
    )


get_messages >> messages_received >> process_message
process_message >> bf_graphs >> folio_map_task_group
folio_map_task_group >> folio_records >> new_folio_records
new_folio_records >> processed_sinopia >> sinopia_update_group
sinopia_update_group >> notify_sinopia_updated
notify_sinopia_updated >> processing_complete
