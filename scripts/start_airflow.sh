#!/bin/bash
# Start Airflow standalone (scheduler + API server + triggerer)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

source "$PROJECT_DIR/venv/bin/activate"
source "$PROJECT_DIR/.env"

export AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER
export AIRFLOW__CORE__LOAD_EXAMPLES
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
export AIRFLOW__API__PORT
export PYTHONPATH
export GOOGLE_APPLICATION_CREDENTIALS
export GCP_PROJECT_ID
export BQ_DATASET
export BRONZE_PATH
export SILVER_PATH
export no_proxy
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY
export AIRFLOW__CORE__MP_START_METHOD
export PYTHONFAULTHANDLER
export OTEL_SDK_DISABLED

echo "Starting Airflow 3 standalone on port 8081..."
airflow standalone
