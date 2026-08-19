"""Send Slack messages through Airflow's ``slack_default`` connection."""

from __future__ import annotations

from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.sdk import get_current_context

SLACK_WEBHOOK_CONN_ID = "slack_default"


def _send_message(message: str) -> None:
    SlackWebhookHook(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
    ).send_text(message)


def push_slack_notification(text: str) -> None:
    """Send a failure message with the current DAG and task identifiers."""
    context = get_current_context()
    task_instance = context["ti"]
    dag_run_url = task_instance.log_url.split("/tasks/", 1)[0]
    message = (
        "<!everyone> 🚨 Data pipeline failure 🚨\n"
        f"*DAG:* `{task_instance.dag_id}`\n"
        f"*Task:* `{task_instance.task_id}`\n"
        f"*Error:* {text}\n"
        f"<{dag_run_url}|Open DAG run in Airflow>"
    )

    _send_message(message)


def push_slack_success_notification(text: str) -> None:
    """Send a successful-load message with the current DAG identifier."""
    context = get_current_context()
    task_instance = context["ti"]
    dag_run_url = task_instance.log_url.split("/tasks/", 1)[0]
    message = (
        "🎉 Data pipeline completed successfully! 🎉\n"
        f"*DAG:* `{task_instance.dag_id}`\n"
        f"*Result:* {text}\n"
        f"<{dag_run_url}|Open DAG run in Airflow>"
    )

    _send_message(message)