# Airflow Pilot

A lightweight local [Apache Airflow](https://airflow.apache.org/) 3.3.1 environment
running on Docker Compose with the **CeleryExecutor**, **PostgreSQL 16**, and
**Redis 7.2**. The image bundles Airflow's Snowflake and Slack providers so DAGs
can run SQL against Snowflake (via key-pair auth) and post notifications to Slack.

The headline example is the **`countries_api`** pipeline: it pulls country data
from the [REST Countries API](https://restcountries.com), lands the raw JSON in
Snowflake, transforms it through a staging table, merges it into a curated
dimension, and reports the result to Slack.

> This setup is for local development only. Do not use it as-is in production.

## Prerequisites

- Docker Desktop / Docker Engine with the Compose v2 plugin (`docker compose`)
- At least 4 GB of memory allocated to Docker
- (Optional) A Snowflake account and a Slack workspace to exercise the
  `countries_api` and Slack DAGs

## Layout

| Path            | Purpose                                                       |
| --------------- | ------------------------------------------------------------- |
| `dags/`         | DAG definitions (mounted into every container)                |
| `dags/utils/`   | Shared helpers (`snow_con.py`, `slack_notifications.py`)      |
| `Snowflake/DDL/`| Snowflake table DDL + merge DML applied manually before runs  |
| `plugins/`      | Custom Airflow plugins                                        |
| `config/`       | Optional `airflow.cfg` overrides                              |
| `logs/`         | Task logs (generated at runtime, git-ignored)                 |
| `secrets/`      | Private keys mounted read-only (git-ignored)                  |
| `Dockerfile`    | Extends the official image with `requirements.txt`            |
| `compose.yaml`  | Airflow, Celery worker, Redis, and Postgres services          |
| `requirements.txt` | Extra Python packages baked into the image                 |
| `.env`          | Local secrets/config (git-ignored)                            |

## DAGs

| DAG ID          | File                       | Schedule           | What it does                                             |
| --------------- | -------------------------- | ------------------ | -------------------------------------------------------- |
| `countries_api` | `dags/countries_api.py`    | Manual (parametric)| REST Countries API → Snowflake ETL + Slack notification  |

DAGs are paused at creation (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`).
Unpause them in the UI to schedule, or trigger them manually.

## First run

```bash
# 1. Build the extended Airflow image (installs requirements.txt)
docker compose build

# 2. Initialise the metadata DB and create the admin user
docker compose up airflow-init

# 3. Start the whole stack in the background
docker compose up -d
```

Then open the web UI at http://localhost:8080 and log in with:

- **Username:** `airflow`
- **Password:** `airflow`

You should see the `countries_api` DAG listed above. Unpause it (toggle on the
left) or trigger it manually with the play button.

## Common commands

```bash
# Tail logs for all services
docker compose logs -f

# List DAGs via the CLI (one-off container)
docker compose run --rm airflow-cli airflow dags list

# Trigger the countries pipeline (uses the default region, Europe)
docker compose run --rm airflow-cli airflow dags trigger countries_api

# Start Flower to monitor Celery workers, then open http://localhost:5555
docker compose --profile flower up -d flower

# Stop everything (keeps the database volume)
docker compose down

# Stop and wipe the database volume too
docker compose down --volumes
```

## The `countries_api` pipeline

The DAG takes a single `region` parameter (`Africa`, `Americas`, `Asia`,
`Europe`, or `Oceania`; default `Europe`) and runs the following flow:

```
get_countries_list                       # GET the REST Countries API for the region
└── snowflake (task group)
      ├── write_to_snowflake             # INSERT raw JSON into RAW_COUNTRIES
      ├── truncate_staging_table         # TRUNCATE STG_COUNTRIES
      ├── insert_into_staging            # Flatten latest raw load into STG_COUNTRIES
      └── merge_into_curated             # MERGE into CURATED_COUNTRIES
└── notify_load_success                  # Slack success message
```

**Source:** `GET https://api.restcountries.com/countries/v5?region=<region>&limit=100`
with an `Authorization: <RESTCOUNTRIES_API_KEY>` header.

**Target objects** live in the `AIRFLOW_PILOT.COUNTRIES` schema:

| Table               | Role                                                          |
| ------------------- | ------------------------------------------------------------ |
| `RAW_COUNTRIES`     | Raw API JSON (`RAW_DATA VARIANT`, auto-increment `LOAD_ID`)  |
| `STG_COUNTRIES`     | Flattened staging (names, codes, currency, links, timezones) |
| `CURATED_COUNTRIES` | Curated dimension deduplicated by `ALPHA3_CODE`, with audit columns |

### Snowflake schema setup (required before first run)

Apply the DDL scripts in `Snowflake/DDL/` against your Snowflake account, in this
order, before triggering the DAG:

1. `Snowflake/DDL/RAW_COUNTRIES.sql`
2. `Snowflake/DDL/STG_COUNTRIES.sql`
3. `Snowflake/DDL/CURATED_COUNTRIES.sql`

(`Snowflake/DDL/Merge dml.sql` mirrors the merge logic embedded in the DAG and is
kept for reference.)

### REST Countries API key

Set `RESTCOUNTRIES_API_KEY` in `.env`. It is sent verbatim as the `Authorization`
header on the API request.

## Adding dependencies

Add Python packages to `requirements.txt`, then rebuild:

```bash
docker compose build
docker compose up -d
```

The image currently installs:

- `apache-airflow-providers-fab`
- `apache-airflow-providers-celery`
- `apache-airflow-providers-snowflake`
- `apache-airflow-providers-slack`

`requests` and `pendulum` are used by the DAGs; `pendulum` ships with Airflow, and
`requests` is available in the base image.

## Snowflake connection (key-pair auth)

The stack expects a `snowflake_default` connection. The `countries_api` pipeline
runs its SQL through the `snow_execute_query` helper in
[`dags/utils/snow_con.py`](dags/utils/snow_con.py), which uses this connection.

### 1. Create / register a key-pair

If you don't already have one, generate a PKCS#8 RSA key-pair:

```bash
# Unencrypted private key (simplest for a pilot)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Encrypted private key instead (you'll supply the passphrase to Airflow)
# openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -v2 aes-256-cbc -out rsa_key.p8

# Public key to register in Snowflake
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Register the public key on the Snowflake user (strip the header/footer lines and
newlines from `rsa_key.pub`):

```sql
ALTER USER <your_user> SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';
```

### 2. Place the private key

Copy the private key to `secrets/rsa_key.p8`. It is mounted read-only into every
Airflow container at `/opt/airflow/secrets/rsa_key.p8`. The `secrets/` folder is
git-ignored, so the key is never committed.

### 3. Configure the connection

Two equivalent options:

**Option A - `.env` (recommended, auto-loaded).** Edit the
`AIRFLOW_CONN_SNOWFLAKE_DEFAULT` line in `.env` with your values, then restart:

```bash
AIRFLOW_CONN_SNOWFLAKE_DEFAULT='{"conn_type":"snowflake","login":"YOUR_USER","password":"KEY_PASSPHRASE_OR_EMPTY","schema":"COUNTRIES","extra":{"account":"ORGNAME-ACCOUNT","warehouse":"COMPUTE_WH","database":"AIRFLOW_PILOT","role":"YOUR_ROLE","private_key_file":"/opt/airflow/secrets/rsa_key.p8"}}'
```

```bash
docker compose up -d   # picks up the new env-var
```

- `password` is the private-key **passphrase** (leave `""` if the key is
  unencrypted) - not the Snowflake account password.
- `account` is the `ORGNAME-ACCOUNT` identifier (or a locator like
  `xy12345.eu-central-1`).

**Option B - Airflow UI.** Go to **Admin -> Connections -> +**, then set:

| Field           | Value                                              |
| --------------- | -------------------------------------------------- |
| Connection Id   | `snowflake_default`                                |
| Connection Type | `Snowflake`                                        |
| Login           | your Snowflake user                                |
| Password        | private-key passphrase (blank if unencrypted)      |
| Schema          | e.g. `COUNTRIES`                                   |
| Extra (JSON)    | `{"account":"ORGNAME-ACCOUNT","warehouse":"COMPUTE_WH","database":"AIRFLOW_PILOT","role":"YOUR_ROLE","private_key_file":"/opt/airflow/secrets/rsa_key.p8"}` |

> Note: if `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` is set in `.env`, it **overrides** a
> UI connection with the same id. Use one or the other to avoid confusion.

## Slack messages (Incoming Webhook)

The stack expects a `slack_default` connection. The `countries_api` pipeline sends
success/failure notifications through the helpers in
[`dags/utils/slack_notifications.py`](dags/utils/slack_notifications.py).

### 1. Create an Incoming Webhook

1. Go to https://api.slack.com/apps -> **Create New App** -> **From scratch** and
   pick your workspace.
2. **Features -> Incoming Webhooks** -> toggle **On** -> **Add New Webhook to
   Workspace** -> choose the target channel.
3. Copy the webhook URL, e.g.
   `https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX`.

> A webhook posts as the app (not your personal Slack user) and is locked to the
> channel you selected. To post to multiple channels/DMs, switch to a bot token
> later.

### 2. Configure the connection

Edit the `AIRFLOW_CONN_SLACK_DEFAULT` line in `.env`, putting **only the part
after `/services/`** into `password`, then restart:

```bash
AIRFLOW_CONN_SLACK_DEFAULT='{"conn_type":"slackwebhook","host":"https://hooks.slack.com/services","password":"T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"}'
```

```bash
docker compose up -d   # picks up the new env-var
```

The UI equivalent is **Admin -> Connections -> +**, Connection Id
`slack_default`, Connection Type `Slack Incoming Webhook`, with the token in the
Password field. (The `.env` var overrides a same-named UI connection.)

### 3. Notification helpers

Reusable notifiers live in
[`dags/utils/slack_notifications.py`](dags/utils/slack_notifications.py):

- `push_slack_notification(text)` - failure alert with DAG/task context and a link
  back to the Airflow run.
- `push_slack_success_notification(text)` - success message with DAG context.

Import them from a DAG (the `dags/` directory is on `PYTHONPATH`, so `utils` is
importable):

```python
from utils.slack_notifications import (
    push_slack_notification,
    push_slack_success_notification,
)
```

## Environment variables

Local secrets and config live in `.env` (git-ignored). Key entries:

| Variable                          | Purpose                                                    |
| --------------------------------- | ---------------------------------------------------------- |
| `AIRFLOW_UID`                     | Host user ID for file ownership (default `50000`)          |
| `FERNET_KEY`                      | Encrypts Airflow connections/variables at rest             |
| `_AIRFLOW_WWW_USER_USERNAME`      | Web UI admin username (default `airflow`)                  |
| `_AIRFLOW_WWW_USER_PASSWORD`      | Web UI admin password (default `airflow`)                  |
| `AIRFLOW__API_AUTH__JWT_SECRET`   | JWT signing secret for the internal API                    |
| `AIRFLOW_CONN_SNOWFLAKE_DEFAULT`  | Snowflake connection JSON (key-pair auth)                  |
| `AIRFLOW_CONN_SLACK_DEFAULT`      | Slack Incoming Webhook connection JSON                     |
| `RESTCOUNTRIES_API_KEY`           | Bearer token sent to the REST Countries API                |

Regenerate the `FERNET_KEY` for any shared or long-lived environment:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Configuration notes

- Airflow's bundled examples are disabled (`AIRFLOW__CORE__LOAD_EXAMPLES=false`).
  Flip it to `true` in `compose.yaml` if you want them.
- There is currently no automated test suite; validate DAGs by triggering them
  from the UI or CLI. `test.json` holds a sample REST Countries API response
  (Albania) useful as a schema reference.
