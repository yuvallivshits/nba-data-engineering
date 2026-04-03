#!/bin/bash
# Start Streamlit dashboard

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

source "$PROJECT_DIR/venv/bin/activate"
source "$PROJECT_DIR/.env"

export PYTHONPATH
export DUCKDB_PATH

echo "Starting Streamlit dashboard on http://localhost:8501..."
streamlit run "$PROJECT_DIR/dashboard/app.py"
