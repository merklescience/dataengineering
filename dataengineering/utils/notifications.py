import json
import logging

import requests
from decouple import config

from dataengineering.constants import ServerEnv


def task_fail_slack_alert(context):
    """
    Sends a slack notification on task failure
    This function needs to be attached to all dags from which you
    monitor failures
    """
    slack_webhook_url = config("SLACK_NOTIFICATION_URL", cast=str)
    server_env = config("SERVER_ENV", ServerEnv.LOCAL, cast=str)

    if server_env == ServerEnv.PRODUCTION:
        slack_msg = f"""
                :red_circle: Task Failed.
                *Task*: {context.get('task_instance').task_id}
                *Dag*: {context.get('task_instance').dag_id}
                *Execution Time*: {context.get('execution_date')}
                *Log Url*: {context.get('task_instance').log_url}
                """

        requests.post(slack_webhook_url, data=json.dumps({"text": slack_msg}))
    else:
        logging.warning(f"Not writing to slack because server_env={server_env}")
