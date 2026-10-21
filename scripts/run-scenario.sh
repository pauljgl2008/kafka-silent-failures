#!/usr/bin/env bash
# Runs one scenario from a clean state and prints what the reconciler found.
# Usage: scripts/run-scenario.sh naive|safe [count]
set -euo pipefail

mode="${1:?usage: run-scenario.sh naive|safe [count]}"
count="${2:-1000}"
app="http://localhost:8080"

echo "Starting a clean stack in $mode mode..."
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
PIPELINE_MODE="$mode" docker compose up -d --build

echo "Waiting for the app..."
until curl -sf "$app/actuator/health" >/dev/null; do sleep 2; done

echo "Creating $count payments..."
curl -sf -X POST "$app/simulate?count=$count"
echo

echo "Waiting for the pipeline to settle (grace period + a few polls)..."
sleep 20

echo "Reconciliation:"
curl -sf "$app/reconciliation"
echo
echo "Grafana: http://localhost:3001   Prometheus alerts: http://localhost:9090/alerts"
