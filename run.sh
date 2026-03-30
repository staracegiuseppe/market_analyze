#!/bin/sh
# run.sh - Market Analyze v2.1 - Minimale, nessuna logica bloccante

# Rimuovi proxy che interferiscono con Yahoo Finance
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY no_proxy NO_PROXY
export NO_PROXY="*"

OPTIONS=/data/options.json

# Leggi opzioni con python one-liner
get_opt() {
    python3 -c "
import json,sys
try:
    d=json.load(open('${OPTIONS}'))
    v=d.get('$1','$2')
    print(str(v) if v is not None and str(v)!='' else '$2')
except:
    print('$2')
" 2>/dev/null || echo "$2"
}

export ANTHROPIC_API_KEY=$(get_opt "anthropic_api_key" "")
export PERPLEXITY_API_KEY=$(get_opt "perplexity_api_key" "")
export SCORE_THRESHOLD=$(get_opt "score_threshold" "25")
export SCHEDULER_MINUTES=$(get_opt "scheduler_interval_minutes" "60")
export BIND_HOST="0.0.0.0"
export INGRESS_PORT="8099"

echo "[Market Analyze v2.1] Starting on ${BIND_HOST}:${INGRESS_PORT}"
echo "[Config] Claude=$([ -n "${ANTHROPIC_API_KEY}" ] && echo ON || echo MISSING) | Perplexity=$([ -n "${PERPLEXITY_API_KEY}" ] && echo ON || echo OFF)"

exec python3 /app/main.py