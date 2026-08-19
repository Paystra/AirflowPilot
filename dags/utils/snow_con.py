"""Snowflake query helpers intended to run inside Airflow tasks."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from utils.slack_notifications import push_slack_notification

logger = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_default"


def snow_execute_query(
    sql: str,
    parameters: Mapping[str, Any] | None = None,
) -> None:
    """Execute SQL immediately and surface failures in the calling task log."""
    logger.info("Executing Snowflake query")
    logger.debug(f"SQL: {sql}")
    logger.debug(f"Parameters: {parameters}")

    try:
        SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        ).run(sql, parameters=parameters)
    except Exception as e:
        logger.exception("Snowflake query failed")
        push_slack_notification(f"Error executing query: {e}")
        raise

    logger.info("Snowflake query completed successfully")