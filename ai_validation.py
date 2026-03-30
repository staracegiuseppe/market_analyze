# ai_validation.py v2.0
# Optional LLM enrichment. Called only for top-N shortlisted signals.
# MAX_LLM_ASSETS_PER_RUN = 3. HOLD and NO_DATA are skipped entirely.

import os, json, logging, time
from datetime import datetime
from typing   import Dict, List, Optional
import requests

log = logging.getLogger("ai_validation")

MAX_LLM_ASSETS_PER_RUN = 3
MIN_CONF_FOR_LLM       = 50   # skip enrichment if confidence below this
LLM_CACHE: Dict[str, Dict] = {}
CACHE_TTL_SEC = 3600


def _cache_key(sym: str) -> str:
    bucket = datetime.now().strftime("%Y%m%d%H")
    return f"{sym}_{bucket}"


def _cached(sym: str) -> Optional[Dict]:
    k = _cache_key(sym)
    e = LLM_CACHE.get(k)
    if e and (time.time() - e["ts"]) < CACHE_TTL_SEC:
        return e["v"]
    return None


def _set_cache(sym: str, v: Dict):
    LLM_CACHE[_cache_key(sym)] = {"v": v, "ts": time.time()}


def _compact_snapshot(sig: Dict) -> str:
    """Minimal JSON snapshot for LLM — no OHLC, no history."""
    ind = sig.get("indicators", {})
    return json.dumps({
        "symbol":   sig["symbol"],
        "market":   sig["market"],
        "action":   sig["action"],
        "conf":     sig["confidence"],
        "price":    sig["price"],
        "rsi":      ind.get("rsi"),
        "adx":      ind.get("adx"),
        "macd":     ind.get("macd_hist"),
        "obv":      ind.get("obv_trend"),
        "bb_pos":   ind.get("bb_pos"),
        "support":  ind.get("support"),
        "resistance":ind.get("resistance"),
        "atr":      ind.get("atr"),
        "reasons":  sig.get("reasons", [])[:3],
    }, separators=(",", ":"))


# Short prompt — JSON only, no market education
CLAUDE_PROMPT = (
    'You are a market signal validator.\n'
    'Input: compact quantitative snapshot.\n'
    'Return strict JSON only:\n'
    '{"summary":"max 40 words","risk_flags":["short"],'
    '"confidence_adjustment":-5,"news_bias":"neutral","action_override":"none"}\n'
    'Rules: use only provided data. action_override must be "none" unless data strongly contradicts.\n'
    'Snapshot:\n'
)

PPLX_PROMPT = (
    'Return max 3 recent headlines for SYMBOL. '
    'Prioritize earnings, guidance, M&A, regulation. Skip commentary. '
    'JSON only: [{"headline":"...","source":"...","date":"..."}]'
)


def enrich_with_claude(sig: Dict, api_key: str) -> Dict:
    sym = sig["symbol"]
    cached = _cached(f"claude_{sym}")
    if cached:
        log.info(f"[AI] {sym}: Claude cache hit")
        return cached

    snapshot = _compact_snapshot(sig)
    prompt   = CLAUDE_PROMPT + snapshot

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 200,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=20,
        )
        if r.status_code == 200:
            text  = r.json()["content"][0]["text"]
            clean = text.replace("```json","").replace("```","").strip()
            result = json.loads(clean)
            _set_cache(f"claude_{sym}", result)
            log.info(f"[AI] {sym}: Claude OK news_bias={result.get('news_bias')} "
                     f"adj={result.get('confidence_adjustment')}")
            return result
        log.warning(f"[AI] {sym}: Claude {r.status_code}")
    except Exception as e:
        log.warning(f"[AI] {sym}: Claude error: {e}")

    return {"summary": "", "risk_flags": [], "confidence_adjustment": 0,
            "news_bias": "neutral", "action_override": "none"}


def enrich_with_perplexity(sym: str, api_key: str) -> List[Dict]:
    cached = _cached(f"pplx_{sym}")
    if cached:
        log.info(f"[AI] {sym}: Perplexity cache hit")
        return cached

    prompt = PPLX_PROMPT.replace("SYMBOL", sym)
    try:
        r = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "sonar-pro",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 300, "temperature": 0.1,
                  "search_recency_filter": "day"},
            timeout=20,
        )
        if r.status_code == 200:
            text = r.json()["choices"][0]["message"]["content"]
            clean = text.replace("```json","").replace("```","").strip()
            result = json.loads(clean)
            _set_cache(f"pplx_{sym}", result)
            log.info(f"[AI] {sym}: Perplexity OK {len(result)} headlines")
            return result
        log.warning(f"[AI] {sym}: Perplexity {r.status_code}")
    except Exception as e:
        log.warning(f"[AI] {sym}: Perplexity error: {e}")
    return []


def apply_ai_enrichment(
    signals:    List[Dict],
    claude_key: str,
    pplx_key:   str,
) -> List[Dict]:
    """
    Enrich top-N active signals with LLM validation.
    Skips HOLD, NO_DATA, and signals below MIN_CONF_FOR_LLM.
    """
    candidates = [
        s for s in signals
        if s["action"] in ("BUY", "SELL", "WATCHLIST")
        and s["confidence"] >= MIN_CONF_FOR_LLM
        and s.get("has_real_data")
    ][:MAX_LLM_ASSETS_PER_RUN]

    if not candidates:
        log.info("[AI] No candidates for LLM enrichment (all below threshold)")
        return signals

    log.info(f"[AI] Enriching {len(candidates)}/{len(signals)} signals "
             f"(cap={MAX_LLM_ASSETS_PER_RUN})")

    enriched_map = {}
    for sig in candidates:
        sym = sig["symbol"]
        claude_r  = enrich_with_claude(sig, claude_key) if claude_key else {}
        pplx_news = enrich_with_perplexity(sym, pplx_key) if pplx_key else []

        # Apply confidence adjustment (max ±5 from AI)
        adj = max(-5, min(5, int(claude_r.get("confidence_adjustment", 0))))
        enriched_map[sym] = {
            "ai_summary":    claude_r.get("summary", ""),
            "ai_risk_flags": claude_r.get("risk_flags", []),
            "ai_news_bias":  claude_r.get("news_bias", "neutral"),
            "ai_conf_adj":   adj,
            "news":          pplx_news[:3],
        }

    # Merge into signals
    result = []
    for s in signals:
        sym = s["symbol"]
        if sym in enriched_map:
            e = enriched_map[sym]
            s = {**s,
                 "ai_summary":    e["ai_summary"],
                 "ai_risk_flags": e["ai_risk_flags"],
                 "ai_news_bias":  e["ai_news_bias"],
                 "confidence":    max(0, min(99, s["confidence"] + e["ai_conf_adj"])),
                 "news":          e["news"],
                 "ai_enriched":   True}
        else:
            s = {**s, "ai_enriched": False}
        result.append(s)
    return result