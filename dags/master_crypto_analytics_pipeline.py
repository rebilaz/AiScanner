from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments applied to all tasks
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# The DAG orchestrates all analytics workers. It runs hourly by default.
with DAG(
    dag_id="master_crypto_analytics_pipeline",
    description="Orchestrate all crypto analytics workers",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Worker tasks defined via BashOperator. Paths are retrieved from
    # Airflow Variables so they can be updated without modifying the DAG.
    task_2_1 = BashOperator(
        task_id="run_worker_2_1",
        bash_command="python {{ dag.folder }}/../workers/worker_2_1.py",
    )

    task_2_2 = BashOperator(
        task_id="run_worker_2_2",
        bash_command="python {{ dag.folder }}/../workers/worker_2_2.py",
    )

    task_2_3 = BashOperator(
        task_id="run_worker_2_3",
        bash_command="python {{ dag.folder }}/../workers/worker_2_3.py",
    )

    task_5_1 = BashOperator(
        task_id="run_worker_5_1",
        bash_command="python {{ dag.folder }}/../workers/worker_5_1.py",
    )

    task_5_2 = BashOperator(
        task_id="run_worker_5_2",
        bash_command="python {{ dag.folder }}/../workers/worker_5_2.py",
    )

    task_7_3 = BashOperator(
        task_id="run_worker_7_3",
        bash_command="python {{ dag.folder }}/../workers/worker_7_3.py",
    )

    task_4_1 = BashOperator(
        task_id="run_worker_4_1",
        bash_command="python {{ dag.folder }}/../workers/worker_4_1.py",
    )

    task_4_2 = BashOperator(
        task_id="run_worker_4_2",
        bash_command="python {{ dag.folder }}/../workers/worker_4_2.py",
    )

    task_4_3 = BashOperator(
        task_id="run_worker_4_3",
        bash_command="python {{ dag.folder }}/../workers/worker_4_3.py",
    )

    task_3_1 = BashOperator(
        task_id="run_worker_3_1",
        bash_command="python {{ dag.folder }}/../workers/worker_3_1.py",
    )

    task_6_3 = BashOperator(
        task_id="run_worker_6_3",
        bash_command="python {{ dag.folder }}/../workers/worker_6_3.py",
    )

    task_7_2 = BashOperator(
        task_id="run_worker_7_2",
        bash_command="python {{ dag.folder }}/../workers/worker_7_2.py",
    )

    task_3_2 = BashOperator(
        task_id="run_worker_3_2",
        bash_command="python {{ dag.folder }}/../workers/worker_3_2.py",
    )

    task_7_1 = BashOperator(
        task_id="run_worker_7_1",
        bash_command="python {{ dag.folder }}/../workers/worker_7_1.py",
    )

    task_7_4 = BashOperator(
        task_id="run_worker_7_4",
        bash_command="python {{ dag.folder }}/../workers/worker_7_4.py",
    )

    # Task dependencies defining the workflow order. Adjust these as the
    # architecture evolves.
    task_2_1 >> task_2_2 >> task_2_3
    task_5_1 >> task_5_2
    task_2_2 >> task_7_3
    [task_4_1, task_4_2] >> task_4_3
    [task_2_2, task_3_1, task_4_3, task_5_2] >> task_6_3
    task_6_3 >> task_7_2
    task_3_2 >> task_7_1

    # Example: task_7_4 (new NFT collection detection) could trigger task_7_1
    # using a TriggerDagRunOperator if a new collection is found.
    # Additional workers can be added by instantiating new BashOperators and
    # linking them via the dependency operators above.
