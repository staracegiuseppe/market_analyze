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


# Cache macro context (aggiornato dallo smart_money module ogni 6h)
_MACRO_CONTEXT: dict = {}

def set_macro_context(ctx: dict):
    """Chiamato da main.py dopo ogni analisi smart_money."""
    global _MACRO_CONTEXT
    _MACRO_CONTEXT = ctx

def _compact_snapshot(sig: Dict) -> str:
    """Minimal JSON snapshot + macro context for LLM."""
    ind = sig.get("indicators", {})
    macro = {}
    if _MACRO_CONTEXT.get("macro_regime"):
        mr = _MACRO_CONTEXT["macro_regime"]
        macro = {
            "rate_env":    mr.get("rate_environment", "HOLD"),
            "growth":      mr.get("growth_outlook", "?"),
            "risk_app":    mr.get("risk_appetite", "NEUTRAL"),
            "tail_risks":  mr.get("key_tail_risks", [])[:2],
            "favored":     mr.get("favored_sectors", [])[:3],
            "headwinds":   mr.get("headwind_sectors", [])[:3],
        }
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
        "macro":    macro,
    }, separators=(",", ":"))


# Short prompt — JSON only, no market education
# Macro context è aggiunto dinamicamente da _compact_snapshot()
# Viene iniettato il regime macro globale (tassi, inflazione, geopolitica)
CLAUDE_PROMPT = (
    'You are a macro-aware quantitative analyst validating a trading signal.\n'
    'You receive: (1) technical signal snapshot, (2) current global macro context.\n'
    'Assess if the macro environment SUPPORTS or CONTRADICTS the signal direction.\n\n'
    'MACRO ADJUSTMENT RULES:\n'
    '- BUY growth/tech stock + Fed HIKING cycle → confidence -3 to -5\n'
    '- BUY energy/defense + high geopolitical risk → confidence +3 to +5\n'
    '- BUY gold/bonds + recession risk rising → confidence +3\n'
    '- SELL cyclicals + recession signals → confidence +2 to +4\n'
    '- BUY defensives (healthcare/utilities) + slowdown → confidence +2\n'
    '- Signal aligned with macro tailwind → macro_alignment=supportive\n'
    '- Signal against macro headwind → macro_alignment=contradicts\n\n'
    'confidence_adjustment: integer -5 to +5 ONLY. action_override: always none.\n'
    'Return ONLY valid JSON:\n'
    '{"summary":"max 40 words citing macro+technical","risk_flags":["specific risk"],'
    '"confidence_adjustment":0,"news_bias":"bullish|bearish|neutral",'
    '"macro_alignment":"supportive|neutral|contradicts","action_override":"none"}\n'
    'INPUTS:\n'
)

PPLX_PROMPT = (
    'For SYMBOL: find max 3 headlines from last 7 days. '
    'Prioritize: earnings, guidance, M&A, regulation, macro exposure (rates/USD/oil/sanctions). '
    'Include macro-relevant events affecting this specific asset or sector. '
    'Skip generic analyst opinions without hard data. '
    'JSON only: [{"headline":"...","source":"...","date":"...","macro_relevant":true}]'
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
            if not clean or not clean.startswith("["):
                log.info(f"[AI] {sym}: Perplexity risposta non JSON array — skip")
                return []
            result = json.loads(clean)
            if not isinstance(result, list):
                result = []
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
        try:
            raw_adj = claude_r.get("confidence_adjustment", 0)
            adj = max(-5, min(5, int(float(raw_adj))))
        except (ValueError, TypeError):
            adj = 0
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
