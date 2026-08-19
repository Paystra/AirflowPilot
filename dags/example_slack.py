"""Sample DAG that posts a message to Slack via an Incoming Webhook.

Triggering `example_slack` sends a message to the channel wired to the
`slack_default` webhook connection (see AIRFLOW_CONN_SLACK_DEFAULT in .env).
It is unscheduled; trigger it manually from the UI at http://localhost:8080.

Prerequisites (see README):
  - An Incoming Webhook created in your Slack workspace
  - AIRFLOW_CONN_SLACK_DEFAULT set in .env (or a UI connection named
    `slack_default`)
"""

from __future__ import annotations

import pendulum

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.sdk import dag

from slack_notifications import SLACK_WEBHOOK_CONN_ID, slack_failure_notifier


@dag(
    dag_id="example_slack",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Vilnius"),
    catchup=False,
    tags=["pilot", "slack"],
    on_failure_callback=slack_failure_notifier,
)

def example_slack():
    SlackWebhookOperator(
        task_id="post_message",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message="🟢🟡🔴 Hello from Airflow ({{ ts }})",
    )


example_slack()
