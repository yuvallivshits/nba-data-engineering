#!/bin/bash
# Initialize Airflow for local development

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

source "$PROJECT_DIR/venv/bin/activate"
source "$PROJECT_DIR/.env"

export AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER
export AIRFLOW__CORE__LOAD_EXAMPLES
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN

mkdir -p "$AIRFLOW_HOME"

echo "Initializing Airflow DB..."
airflow db migrate

echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Done. Run './scripts/start_airflow.sh' to start Airflow."
