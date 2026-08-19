"""Connectivity-test DAG for the Snowflake `snowflake_default` connection.

Triggering this DAG runs a trivial query against Snowflake to confirm the
key-pair credentials and connection settings work. It is paused by default;
unpause or trigger it manually from the UI at http://localhost:8080.

Prerequisites (see README):
  - ./secrets/rsa_key.p8 present (mounted at /opt/airflow/secrets/rsa_key.p8)
  - AIRFLOW_CONN_SNOWFLAKE_DEFAULT set in .env (or a UI connection named
    `snowflake_default`)
  - The matching public key registered on the Snowflake user
"""

from __future__ import annotations

import pendulum
import requests
import logging
import os
import json

#from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Param, dag, get_current_context, task, task_group
from utils.slack_notifications import (
  push_slack_notification,
  push_slack_success_notification,
)


from utils.snow_con import snow_execute_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COUNTRY_REGIONS = ["Africa", "Americas", "Asia", "Europe", "Oceania"]


@task
def get_countries_list():
    region = get_current_context()["params"]["region"]
    logger.info("Getting countries list for region: %s", region)

    try:
      response = requests.get(
        "https://api.restcountries.com/countries/v5",
        params={"region": region, "limit": 100},
        headers={"Authorization": os.getenv("RESTCOUNTRIES_API_KEY")},
        timeout=30,
      )
      status_code = response.status_code
    except Exception as e:
      logger.error(f"Error getting countries list: {e}")
      push_slack_notification(f"Error getting countries list: {e}")
      raise e

    if status_code != 200:
      logger.error(f"Error getting countries list: {status_code}")
      push_slack_notification(f"Error getting countries list: {status_code}")
      raise Exception(f"Error getting countries list: {status_code}")

    # build dict for snowflake
    data = response.json()
    reason = response.reason
    elapsed = response.elapsed
    request_time = pendulum.now() + elapsed

    logger.info(
      f"Status code: {status_code}\nReason: {reason}\nResponse body: {data}\n"
    )
    return dict(
      data=data,
      status_code=status_code,
      reason=reason,
      request_time=request_time,
      elapsed=elapsed
    )

@task
def write_to_snowflake(data: dict):
  query = """
    INSERT INTO AIRFLOW_PILOT.COUNTRIES.RAW_COUNTRIES
    (
      RAW_DATA
    )
    SELECT PARSE_JSON(%(raw_data)s)
    """

  parameters = {
    "raw_data": json.dumps(data["data"]),
  }

  logger.info("Writing countries payload to Snowflake")
  snow_execute_query(query, parameters)
  logger.info("Countries payload written to Snowflake")

@task
def truncate_staging_table():
  query = "truncate table AIRFLOW_PILOT.COUNTRIES.STG_COUNTRIES;"
  logger.info("Truncating staging table")
  snow_execute_query(query)
  logger.info("Staging table truncated")

@task
def insert_into_staging():
  query = """
  insert into AIRFLOW_PILOT.COUNTRIES.STG_COUNTRIES
  (
      LOAD_ID,
      COMMON_COUNTRY_NAME,
      OFFICIAL_COUNTRY_NAME,
      ALPHA3_CODE,
      CCN3_NUMERIC_CODE,
      CURRENCY,
      GOOGLE_MAPS_URL,
      OFFICIAL_LINK,
      WIKIPEDIA_LINK,
      TIMEZONES,
      LANGUAGES
  )

  select
      c.load_id,
      -- obj.value,
      obj.value:names:common::string as common_country_name,
      obj.value:names:official::string as official_country_name,
      obj.value:codes:alpha_3::string as alpha3_code,
      obj.value:codes:ccn3::string as ccn3_numeric_code,
      obj.value:currencies[0]:name::string as currency,
      obj.value:links:google_maps::string as google_maps_url,
      obj.value:links:official::string as official_link,
      obj.value:links:wikipedia::string as wikipedia_link,
      listagg(distinct tzs.value::string,',') within group (order by tzs.value::string asc) as timezones,
      listagg(lang.value:name::string,',') as languages,
  from airflow_pilot.countries.raw_countries c,
  lateral flatten(raw_data:data:objects) as obj,
  lateral flatten(obj.value:languages) as lang,
  lateral flatten(obj.value:timezones) tzs
  group by all
  qualify dense_rank() over (order by load_id desc) = 1 -- Pulls latest run body.
  ;
  """

  logger.info("Inserting into staging table")
  snow_execute_query(query)
  logger.info("Staging table inserted")

@task
def merge_into_curated():
  query = """
  MERGE INTO AIRFLOW_PILOT.COUNTRIES.CURATED_COUNTRIES AS target
  USING (
      SELECT
          LOAD_ID,
          COMMON_COUNTRY_NAME,
          OFFICIAL_COUNTRY_NAME,
          ALPHA3_CODE,
          CCN3_NUMERIC_CODE,
          CURRENCY,
          GOOGLE_MAPS_URL,
          OFFICIAL_LINK,
          WIKIPEDIA_LINK,
          TIMEZONES,
          LANGUAGES
      FROM AIRFLOW_PILOT.COUNTRIES.STG_COUNTRIES
      WHERE ALPHA3_CODE IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
          PARTITION BY ALPHA3_CODE
          ORDER BY LOAD_ID DESC
      ) = 1
  ) AS incoming
      ON target.COMMON_COUNTRY_NAME = incoming.COMMON_COUNTRY_NAME
  WHEN MATCHED AND (
      target.COMMON_COUNTRY_NAME IS DISTINCT FROM incoming.COMMON_COUNTRY_NAME
      OR target.OFFICIAL_COUNTRY_NAME IS DISTINCT FROM incoming.OFFICIAL_COUNTRY_NAME
      OR target.CCN3_NUMERIC_CODE IS DISTINCT FROM incoming.CCN3_NUMERIC_CODE
      OR target.CURRENCY IS DISTINCT FROM incoming.CURRENCY
      OR target.GOOGLE_MAPS_URL IS DISTINCT FROM incoming.GOOGLE_MAPS_URL
      OR target.OFFICIAL_LINK IS DISTINCT FROM incoming.OFFICIAL_LINK
      OR target.WIKIPEDIA_LINK IS DISTINCT FROM incoming.WIKIPEDIA_LINK
      OR target.TIMEZONES IS DISTINCT FROM incoming.TIMEZONES
      OR target.LANGUAGES IS DISTINCT FROM incoming.LANGUAGES
  )
  THEN UPDATE SET
      target.LOAD_ID = incoming.LOAD_ID,
      target.COMMON_COUNTRY_NAME = incoming.COMMON_COUNTRY_NAME,
      target.OFFICIAL_COUNTRY_NAME = incoming.OFFICIAL_COUNTRY_NAME,
      target.CCN3_NUMERIC_CODE = incoming.CCN3_NUMERIC_CODE,
      target.CURRENCY = incoming.CURRENCY,
      target.GOOGLE_MAPS_URL = incoming.GOOGLE_MAPS_URL,
      target.OFFICIAL_LINK = incoming.OFFICIAL_LINK,
      target.WIKIPEDIA_LINK = incoming.WIKIPEDIA_LINK,
      target.TIMEZONES = incoming.TIMEZONES,
      target.LANGUAGES = incoming.LANGUAGES,
      target.LAST_UPDATE_TS = CURRENT_TIMESTAMP(),
      target.LAST_UPDATE_USER = CURRENT_USER()
  WHEN NOT MATCHED THEN INSERT
      (
          LOAD_ID,
          COMMON_COUNTRY_NAME,
          OFFICIAL_COUNTRY_NAME,
          ALPHA3_CODE,
          CCN3_NUMERIC_CODE,
          CURRENCY,
          GOOGLE_MAPS_URL,
          OFFICIAL_LINK,
          WIKIPEDIA_LINK,
          TIMEZONES,
          LANGUAGES
      )
      VALUES
      (
          incoming.LOAD_ID,
          incoming.COMMON_COUNTRY_NAME,
          incoming.OFFICIAL_COUNTRY_NAME,
          incoming.ALPHA3_CODE,
          incoming.CCN3_NUMERIC_CODE,
          incoming.CURRENCY,
          incoming.GOOGLE_MAPS_URL,
          incoming.OFFICIAL_LINK,
          incoming.WIKIPEDIA_LINK,
          incoming.TIMEZONES,
          incoming.LANGUAGES
      );
  """
  logger.info("Merging into curated table")
  snow_execute_query(query)
  logger.info("Curated table merged")


@task
def notify_load_success():
  region = get_current_context()["params"]["region"]
  push_slack_success_notification(
    f"🚀 Countries for *{region}* were loaded into Snowflake successfully! 🚀"
  )


@task_group(group_id="snowflake")
def snowflake_pipeline(country_list):
  write_raw = write_to_snowflake(country_list)
  truncate_staging = truncate_staging_table()
  load_staging = insert_into_staging()
  merge_curated = merge_into_curated()

  write_raw >> truncate_staging >> load_staging >> merge_curated
  return merge_curated

@dag(
    dag_id="countries_api",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["countries", "api"],
    params={
      "region": Param(
        default="Europe",
        type="string",
        enum=COUNTRY_REGIONS,
        title="Country region",
        description="Region to request from the REST Countries API.",
      ),
    },
)

def countries_api():
  countries = get_countries_list()
  snowflake_load = snowflake_pipeline(countries)
  snowflake_load >> notify_load_success()

countries_api()