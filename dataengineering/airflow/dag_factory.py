"""DAGFactor builder for dags with required metadata."""

from datetime import timedelta

from airflow.models import DAG, Variable
from decouple import config

from dataengineering.airflow.bigquery.utils import ServerEnv
from dataengineering.utils.notifications import task_fail_slack_alert


class DAGFactory:
    """Class that provides useful method to build an Airflow DAG"""

    @classmethod
    def create_dag(
        cls,
        dagname,
        description,
        tags,
        dagrun_timeout,
        start_date,
        schedule_interval,
        default_args=dict(),
    ):
        """
        params:
            dagname(str): the name of the dag
            default_args(dict): a dict with the specific keys you want to edit from the original DEFAULT_ARGS
            start_date(Date): the start date of the DAG('2020-12-29')
            cron(str): the cron expression or the schedule
        returns:
            DAG object
        """

        DEFAULT_ARGS = {
            "owner": "analytics",
            "start_date": start_date,
            "retries": 5,
            "retry_delay": timedelta(minutes=5),
            "catchup": False,
            "depends_on_past": False,
            "max_active_runs": 1,
            "concurrency": 5,
            "wait_for_downstream": True,
            "on_failure_callback": task_fail_slack_alert,
        }

        DEFAULT_ARGS.update(default_args)
        dagargs = {
            "default_args": DEFAULT_ARGS,
            "schedule_interval": schedule_interval,
            "dagrun_timeout": dagrun_timeout,
            "description": description,
            "tags": tags,
        }

        dag = DAG(dagname, **dagargs)
        return dag

    @classmethod
    def get_airflow_dag(
        cls,
        dagname,
        description,
        tags,
        start_date="2021-01-01",
        default_args={},
        schedule_interval="@daily",
        dagrun_timeout=None,
    ):
        """
            The actual method that has to be called by a DAG file to get the dag.
        returns:
            DAG object
        """
        server_env = config("SERVER_ENV", ServerEnv.LOCAL, cast=str)
        if server_env != ServerEnv.PRODUCTION:
            dataset_id = Variable.get("BIGQUERY_DESTINATION_DATASET")
            dagname = dagname + "_" + dataset_id

        dag = cls.create_dag(
            dagname,
            description=description,
            tags=tags,
            start_date=start_date,
            default_args=default_args,
            schedule_interval=schedule_interval,
            dagrun_timeout=dagrun_timeout,
        )
        return dag
