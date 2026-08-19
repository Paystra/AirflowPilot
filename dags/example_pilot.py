"""Example pilot DAG demonstrating the TaskFlow API on Airflow 3.x.

It runs a tiny three-step pipeline: pick numbers -> sum them -> report.
Once the stack is up it appears in the UI at http://localhost:8080 as
`example_pilot`. Unpause it to see it run on its daily schedule, or trigger
it manually.
"""

from __future__ import annotations

import pendulum

from airflow.sdk import dag, task

from slack_notifications import slack_failure_notifier


@dag(
    dag_id="example_pilot",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pilot", "example"],
    on_failure_callback=slack_failure_notifier,
)
def example_pilot():
    @task
    def pick_numbers() -> list[int]:
        return [1, 2, 3, 4, 5]

    @task
    def add_them_up(numbers: list[int]) -> int:
        return sum(numbers)

    @task
    def report(total: int) -> None:
        print(f"The total is {total}")

    report(add_them_up(pick_numbers()))


example_pilot()
