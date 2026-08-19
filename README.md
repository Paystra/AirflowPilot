# Airflow Pilot

A lightweight local [Apache Airflow](https://airflow.apache.org/) 3.3.1 environment
running on Docker Compose with the **CeleryExecutor**, **PostgreSQL**, and **Redis**.
The image includes Airflow's Snowflake provider for Snowflake hooks, operators,
transfers, and SQL execution.

## Prerequisites

- Docker Desktop / Docker Engine with the Compose v2 plugin (`docker compose`)
- At least 4 GB of memory allocated to Docker

## Layout

| Path          | Purpose                                              |
| ------------- | ---------------------------------------------------- |
| `dags/`       | Your DAG definitions (mounted into every container)  |
| `plugins/`    | Custom Airflow plugins                               |
| `config/`     | Optional `airflow.cfg` overrides                     |
| `logs/`       | Task logs (generated at runtime)                     |
| `secrets/`    | Private keys mounted read-only (git-ignored)         |
| `Dockerfile`  | Extends the official image with `requirements.txt`   |
| `compose.yaml`| Airflow, Celery worker, Redis, and Postgres services |
| `.env`        | Local secrets/config (git-ignored)                   |

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

You should see the `example_pilot` DAG. Unpause it (toggle on the left) to let it
run, or trigger it manually with the play button.

## Common commands

```bash
# Tail logs for all services
docker compose logs -f

# List DAGs via the CLI (one-off container)
docker compose run --rm airflow-cli airflow dags list

# Start Flower to monitor Celery workers, then open http://localhost:5555
docker compose --profile flower up -d flower

# Stop everything (keeps the database volume)
docker compose down

# Stop and wipe the database volume too
docker compose down --volumes
```

## Adding dependencies

Add Python packages to `requirements.txt`, then rebuild:

```bash
docker compose build
docker compose up -d
```

The image currently installs:

- `apache-airflow-providers-snowflake`
- `apache-airflow-providers-celery`
- `apache-airflow-providers-fab`
- `apache-airflow-providers-slack`

## Snowflake connection (key-pair auth)

The stack ships with a sample `snowflake_default` connection and a test DAG
(`example_snowflake`) that runs `SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(),
CURRENT_ROLE();` to verify connectivity.

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
AIRFLOW_CONN_SNOWFLAKE_DEFAULT='{"conn_type":"snowflake","login":"YOUR_USER","password":"KEY_PASSPHRASE_OR_EMPTY","schema":"PUBLIC","extra":{"account":"ORGNAME-ACCOUNT","warehouse":"COMPUTE_WH","database":"YOUR_DB","role":"YOUR_ROLE","private_key_file":"/opt/airflow/secrets/rsa_key.p8"}}'
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
| Schema          | e.g. `PUBLIC`                                       |
| Extra (JSON)    | `{"account":"ORGNAME-ACCOUNT","warehouse":"COMPUTE_WH","database":"YOUR_DB","role":"YOUR_ROLE","private_key_file":"/opt/airflow/secrets/rsa_key.p8"}` |

> Note: if `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` is set in `.env`, it **overrides** a
> UI connection with the same id. Use one or the other to avoid confusion.

### 4. Test it

Trigger the `example_snowflake` DAG from the UI (or the CLI below) and check the
`check_connection` task log for the returned version/account/role.

```bash
docker compose run --rm airflow-cli airflow dags trigger example_snowflake
```

## Slack messages (Incoming Webhook)

The stack ships with a sample `slack_default` connection, a test DAG
(`example_slack`) that posts a message, and a reusable failure-alert notifier.

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

### 3. Test it

Trigger the `example_slack` DAG and confirm the message lands in your channel:

```bash
docker compose run --rm airflow-cli airflow dags trigger example_slack
```

### 4. Send failure alerts from any DAG

A reusable notifier lives in [dags/slack_notifications.py](dags/slack_notifications.py).
Attach it to any DAG to get a Slack message whenever a task fails:

```python
from slack_notifications import slack_failure_notifier

@dag(..., on_failure_callback=slack_failure_notifier)
def my_dag():
    ...
```

It is already wired into `example_pilot` and `example_snowflake`.

## Configuration notes

- Environment defaults live in `.env`. It is git-ignored, so regenerate the
  `FERNET_KEY` for any shared or long-lived environment:

  ```bash
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```

- Example DAGs are disabled (`AIRFLOW__CORE__LOAD_EXAMPLES=false`). Flip it to
  `true` in `compose.yaml` if you want Airflow's bundled examples.

> This setup is for local development only. Do not use it as-is in production.
