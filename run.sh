#!/bin/sh
# run.sh - Gold Intelligence v1.5.0
# Bind 127.0.0.1: porta accessibile SOLO da HA Ingress, non dalla LAN
# Questo elimina l'errore "invalid authentication" nel log HA

OPTIONS=/data/options.json

get_opt() {
    python3 -c "
import json, sys
try:
    d = json.load(open('${OPTIONS}'))
    v = d.get('$1', '$2')
    print(str(v) if v is not None and str(v) != '' else '$2')
except Exception as e:
    print('$2')
"
}

export ANTHROPIC_API_KEY=$(get_opt "anthropic_api_key" "")
export PERPLEXITY_API_KEY=$(get_opt "perplexity_api_key" "")
export SCORE_THRESHOLD=$(get_opt "score_threshold" "30")
export ENGINE_MODE=$(get_opt "engine_mode" "auto")
export SCHEDULER_MINUTES=$(get_opt "scheduler_interval_minutes" "60")
export BIND_HOST="127.0.0.1"
export INGRESS_PORT=$(get_opt "ingress_port" "8099")

echo "[Gold Intelligence v1.5.0] Avvio..."
echo "[Config] Engine=${ENGINE_MODE} | Soglia=+-${SCORE_THRESHOLD} | Scheduler=${SCHEDULER_MINUTES}min"
echo "[Config] Bind=${BIND_HOST}:${INGRESS_PORT} | Perplexity=$([ -n "${PERPLEXITY_API_KEY}" ] && echo ON || echo OFF)"

exec python3 /app/main.py
