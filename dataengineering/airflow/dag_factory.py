"""DAGFactor builder for dags with required metadata."""


class DAGFactory:
    """Class that provides useful method to build an Airflow DAG"""

    @classmethod
    def create_dag(
        cls,
        dag_id,
        tags,
        start_date,
        schedule_interval,
        description,
        owner,
        end_date=None,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=16,
        depends_on_past=False,
        wait_for_downstream=True,
        default_args=dict(),
        user_defined_macros=dict(),
    ):
        """
        params:
            dag_id: (str)
                Please use a human readable name which conveys the objective of the dag
            tags: (List[str])
                Tags to help filter dags on the airflow interface
            start_date: (Date or Datetime or String)
                The execution date the dag must start execution from
            schedule_interval: (datetime.timedelta)
                The interval between two successive dag runs (if catchup is not occuring)
            description: str
                Please write a few lines to describe what the dag is trying to achieve
            owner: str
                Your name (ex: nirmal). This field helps identify the author of the DAG or
                the current maintainer of the dag
            end_date: (Date or Datetime or String) [Optional]
                If you want to DAG execution to end of a certain date, provide that
                parameter here
            catchup: (Bool)
                If you have created a dag today, and start_date is a day in the past,
                setting catchup to True will ensure historical dag runs are also executed.
            max_active_runs: (int)
                The maximum number of dag instances of this DAG which can be running at the
                same time. This is useful during backfills (coldstarts) when it is required
                to backfill historical records and we need to backfill a few number of records
            max_active_tasks: (int)
                The maximum number of tasks that can be scheduled across all instances of a
                dag run for this DAG
            depends_on_past: (bool)
                If set to True, the DAG run requires that the previous execution of the
                DAG was successful
            wait_for_downstream: (bool)
                when set to true, an instance of task X will wait for tasks immediately
                downstream of the previous instance of task X to finish successfully
                before it runs.
            default_args: (dict)
                Other default DAG arguments which are not listed above can passed here
        """
        from datetime import timedelta

        from airflow.models import DAG

        from dataengineering.utils.notifications import task_fail_slack_alert

        DEFAULT_ARGS = {
            "owner": owner,
            "retries": 5,
            "retry_delay": timedelta(minutes=5),
            "depends_on_past": depends_on_past,
            "wait_for_downstream": wait_for_downstream,
            "start_date": start_date,
        }
        if end_date:
            DEFAULT_ARGS["end_date"] = end_date

        DEFAULT_ARGS.update(default_args)
        dagargs = {
            "default_args": DEFAULT_ARGS,
            "schedule_interval": schedule_interval,
            "description": description,
            "tags": tags,
            "user_defined_macros": user_defined_macros,
            "max_active_runs": max_active_runs,
            "concurrency": max_active_tasks,
            "on_failure_callback": task_fail_slack_alert,
            "catchup": catchup,
        }

        dag = DAG(dag_id, **dagargs)
        return dag
