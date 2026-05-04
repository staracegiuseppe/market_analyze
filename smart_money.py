# smart_money.py v1.0
# Analisi istituzionale "Smart Money":
# 1. Perplexity cerca dati real-time su 13F, insider buys, flussi istituzionali
# 2. Claude analizza con framework hedge fund e produce tabella strutturata
# Output: JSON con opportunities[], summary, warnings, timestamp

import os
import json
import logging
import time
from datetime import datetime
from typing   import Dict, List, Optional
import requests

log = logging.getLogger("smart_money")

# Cache: l'analisi viene rinnovata ogni 6 ore (i dati 13F cambiano lentamente)
_CACHE: Dict = {"data": None, "ts": 0}
CACHE_TTL = 6 * 3600  # 6 ore
_CLAUDE_DISABLED_REASON = ""
_PPLX_DISABLED_REASON = ""


def _provider_error_detail(response: requests.Response) -> str:
    try:
        data = response.json()
        err = data.get("error", data)
        if isinstance(err, dict):
            return str(err.get("message") or err.get("type") or data)[:180]
        return str(err)[:180]
    except Exception:
        return response.text[:180]


def _is_provider_disabled_status(status_code: int) -> bool:
    return status_code in (400, 401, 402, 403, 429)


# ── Prompt Claude — hedge fund analyst ───────────────────────────────────────
CLAUDE_SYSTEM = """You are a senior macro-aware hedge fund analyst.
Your mandate: identify high-potential opportunities by synthesizing institutional flows WITH the current global macro regime.
Core belief: a good stock in a bad macro environment is still a risky trade.
A mediocre stock with strong macro tailwinds can significantly outperform.
You must be explicitly macro-first, skeptical, and transparent about data limitations.
Return ONLY valid JSON with no markdown.

LANGUAGE REQUIREMENT — MANDATORY:
Write ALL descriptive text in ITALIAN. This includes:
  why_matters, macro_rationale, fundamental_snapshot, risk_summary,
  strategic_summary.macro_regime_summary, strategic_summary.trend,
  strategic_summary.sector_insights, strategic_summary.conviction_level,
  strategic_summary.discovery_insight, all warnings[], all key_tail_risks[].
Keep these fields in English (they are code/enum values):
  signal_type ("New Position"|"Increase"|"Convergence"|"Insider Buy"|"Discovery"),
  action ("Monitor"|"Accumulate"|"Avoid"),
  macro_alignment ("TAILWIND"|"NEUTRAL"|"HEADWIND"|"CONTRARIAN"),
  technical_status ("Bullish"|"Neutral"|"Weak"),
  rate_environment ("HIKING"|"CUTTING"|"HOLD"),
  growth_outlook ("EXPANSION"|"SLOWDOWN"|"RECESSION"),
  risk_appetite ("RISK-ON"|"RISK-OFF"|"NEUTRAL"),
  data_quality ("high"|"medium"|"low"),
  company (nome ufficiale dell'azienda), ticker, sector, favored_sectors[], headwind_sectors[]."""

def _build_claude_prompt(pplx_data: str, watched_symbols: List[str]) -> str:
    today = datetime.now().strftime("%d %B %Y")
    symbols_str = ", ".join(watched_symbols[:20])
    return f"""Date: {today}
Watched assets: {symbols_str}

REMINDER: All descriptive text fields MUST be in ITALIAN. Keep enum/code values in English.

Real-time data from institutional sources:
{pplx_data}

ANALYSIS FRAMEWORK — MACRO-FIRST:

STEP 1 — MACRO REGIME (from Global Macro Context section above)
Classify: Rate env (HIKING/CUTTING/HOLD) | Growth (EXPANSION/SLOWDOWN/RECESSION) | Risk (ON/OFF/NEUTRAL)
State explicitly: which sectors have TAILWIND vs HEADWIND in this regime.

STEP 2 — INSTITUTIONAL SIGNAL DETECTION
   - New positions by top hedge funds / superinvestors
   - Position increases ≥20%
   - Convergence: multiple top investors buying same stock
   - Insider buying clusters (especially during market stress)

STEP 3 — MACRO × SIGNAL VALIDATION (CRITICAL)
For each signal: does the macro regime SUPPORT or CONTRADICT it?
   TAILWIND examples: buying energy during oil supply shock; buying gold during rate cuts
   HEADWIND examples: buying unprofitable tech during rate hikes; buying EM during USD surge
   CONTRARIAN (high conviction): insider buying DESPITE macro headwinds

STEP 4 — FUNDAMENTALS FILTER
   - Revenue growth >5-10% | Healthy margins | Sustainable debt (critical when rates high)

STEP 5 — SCORING (0-100) — MACRO-WEIGHTED:
   - Macro alignment (20%): does macro environment favor this asset class?
   - Institutional signal strength (25%): quality and convergence
   - Fundamental quality (25%): revenue, margins, balance sheet
   - Technical confirmation (20%): trend, accumulation
   - Risk/reward profile (10%)

STEP 6 — DISCOVERY: SCOPERTE AD ALTO POTENZIALE (from SCOPERTE section)
For each stock/ETF found outside the watchlist with high profit potential:
   a) Verify macro alignment (does current environment strongly favor this sector?)
   b) Assess institutional accumulation quality (real data or noise?)
   c) Verify fundamentals: margin >15%, revenue growth >10%, reasonable valuation
   d) Add to opportunities list with signal_type=Discovery and is_watchlist=false
   e) WHY_MATTERS must explain in Italian: why big money is buying, what the upside thesis is
   f) FUNDAMENTAL_SNAPSHOT must include in Italian: margin %, revenue growth, P/E or EV/EBITDA
   g) Include ETFs if strong sector inflows are documented
   h) Score honestly: 60-80 for solid evidence, >80 only for exceptional convergence of signals
   i) Be specific — no vague claims, only data-backed statements

HARD RULES:
   - If macro strongly contradicts the signal: cap score at 55
   - High rate env: penalize growth/speculative stocks (P/E >40, no profits)
   - Recession risk HIGH: favor defensives, penalize cyclicals
   - Geopolitical risk HIGH: favor energy/defense/gold, reduce EM/global trade
   - Prefer 3-5 high-quality ideas over 10 weak ones
   - Discovery stocks: cap score at 70 unless evidence is exceptionally strong
   - If data is weak or conflicting, say so explicitly

Return ONLY this JSON structure (no markdown):
{{
  "analysis_date": "{today}",
  "data_quality": "high|medium|low",
  "macro_regime": {{
    "rate_environment": "HIKING|CUTTING|HOLD",
    "growth_outlook": "EXPANSION|SLOWDOWN|RECESSION",
    "risk_appetite": "RISK-ON|RISK-OFF|NEUTRAL",
    "key_tail_risks": ["risk1"],
    "favored_sectors": ["sector1"],
    "headwind_sectors": ["sector2"]
  }},
  "opportunities": [
    {{
      "company": "Full Company Name",
      "ticker": "TICKER",
      "sector": "Sector",
      "signal_type": "New Position|Increase|Convergence|Insider Buy|Discovery",
      "is_watchlist": true,
      "key_investors": ["Investor1"],
      "macro_alignment": "TAILWIND|NEUTRAL|HEADWIND|CONTRARIAN",
      "macro_rationale": "One sentence: why macro supports or risks this trade",
      "why_matters": "Max 2 sentences on institutional signal.",
      "fundamental_snapshot": "Revenue growth, margin, debt in 10 words",
      "technical_status": "Bullish|Neutral|Weak",
      "risk_summary": "Main risk in one sentence",
      "score": 75,
      "action": "Monitor|Accumulate|Avoid"
    }}
  ],
  "strategic_summary": {{
    "macro_regime_summary": "2 sentences on current macro regime and implications",
    "trend": "What institutional trend is emerging",
    "sector_insights": "Sector-level observations given macro environment",
    "conviction_level": "Overall institutional conviction assessment",
    "discovery_insight": "Overlooked themes or sectors emerging from discovery signals"
  }},
  "warnings": [
    "13F filings have 45-day delay - data may not reflect current positions",
    "Other specific warnings"
  ],
  "sources_used": ["List of sources found"]
}}"""


# ── Ricerca Perplexity ─────────────────────────────────────────────────────────
def _perplexity_search(query: str, pplx_key: str, max_tokens: int = 800) -> str:
    """Cerca dati real-time su flussi istituzionali."""
    global _PPLX_DISABLED_REASON
    if _PPLX_DISABLED_REASON:
        log.info(f"[SMART_MONEY] Perplexity skipped ({_PPLX_DISABLED_REASON})")
        return ""

    try:
        r = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={"Authorization": f"Bearer {pplx_key}", "Content-Type": "application/json"},
            json={
                "model":                "sonar-pro",
                "messages": [
                    {"role": "system", "content": "You are a financial research assistant. Find factual, recent data only. Be concise and cite sources."},
                    {"role": "user",   "content": query},
                ],
                "max_tokens":            max_tokens,
                "temperature":           0.1,
                "search_recency_filter": "month",
                "return_citations":      True,
            },
            timeout=25,
        )
        if r.status_code == 200:
            j       = r.json()
            content = j["choices"][0]["message"]["content"]
            cites   = j.get("citations", [])
            if cites:
                content += "\n\nSources: " + ", ".join(cites[:5])
            log.info(f"[SMART_MONEY] Perplexity OK: {len(content)} chars, {len(cites)} cite")
            return content
        detail = _provider_error_detail(r)
        log.warning(f"[SMART_MONEY] Perplexity {r.status_code}: {detail}")
        if _is_provider_disabled_status(r.status_code):
            _PPLX_DISABLED_REASON = f"HTTP {r.status_code}"
        return ""
    except Exception as e:
        log.warning(f"[SMART_MONEY] Perplexity error: {e}")
        return ""


def _gather_institutional_data(watched_symbols: List[str], pplx_key: str) -> str:
    """
    Esegue 3 ricerche Perplexity mirate per raccogliere dati istituzionali:
    - 13F recenti dei top hedge fund
    - Insider buying recente
    - Flussi ETF e rotazioni settoriali
    """
    if not pplx_key:
        return "Perplexity key not configured - using Claude general knowledge only."

    log.info("[SMART_MONEY] Ricerca dati istituzionali via Perplexity...")
    parts = []

    # Query 0: MACRO GLOBALE — fondamentale, eseguita per prima
    q0 = (
        "Current global macro environment for investors today (be specific with numbers): "
        "1) Fed and ECB: last decision, current rate level, next meeting guidance. "
        "2) Inflation: latest CPI and PCE data vs target. "
        "3) USD index (DXY): current level and trend. "
        "4) 10-year US Treasury yield: current level vs 6 months ago. "
        "5) Geopolitical risks: active conflicts, sanctions, trade wars. "
        "6) Recession indicators: yield curve shape, PMI manufacturing, unemployment. "
        "7) Oil price (WTI/Brent) and trend. "
        "8) VIX level and whether markets are in risk-on or risk-off mode. "
        "Conclude with: which asset classes have TAILWIND vs HEADWIND in this environment."
    )
    r0 = _perplexity_search(q0, pplx_key, 700)
    if r0:
        parts.append("=== GLOBAL MACRO CONTEXT ===\n" + r0)

    # Query 1: 13F filings con nota macro
    q1 = (
        "Latest 13F filing changes from top hedge funds and superinvestors in the last 45 days. "
        "Focus on: new positions, significant increases (>20%), high-conviction buys. "
        "Note if positioning aligns with current macro regime (rate cycle, sector rotation). "
        "Include fund name, stock, position size, and any stated macro rationale. "
        f"Priority tickers: {', '.join(watched_symbols[:15])}"
    )
    r1 = _perplexity_search(q1, pplx_key, 700)
    if r1:
        parts.append("=== 13F FILINGS / HEDGE FUND MOVES ===\n" + r1)

    # Query 2: Insider buying con filtro macro
    q2 = (
        "Recent insider buying activity in the last 30 days. "
        "Focus: significant buys (>$100K), cluster buying, purchases during market stress. "
        "Flag if insiders are buying DESPITE macro headwinds (strong contrarian signal). "
        "Include: company, insider role, amount, date."
    )
    r2 = _perplexity_search(q2, pplx_key, 500)
    if r2:
        parts.append("=== INSIDER BUYING ===\n" + r2)

    # Query 3: Rotazioni settoriali guidate da macro
    q3 = (
        "Current institutional sector rotation driven by macro factors. "
        "Which sectors are being accumulated given current rate/inflation/geopolitical environment? "
        "Focus: defensive vs cyclical rotation, commodities flows, bond market. "
        "Any notable ETF inflow/outflow or block trade data."
    )
    r3 = _perplexity_search(q3, pplx_key, 400)
    if r3:
        parts.append("=== SECTOR ROTATION / MACRO FLOWS ===\n" + r3)

    # Query 4: Scoperte ad alto potenziale FUORI dalla watchlist
    watched_str = ", ".join(watched_symbols[:20])
    q4 = (
        "Identify 5-7 stocks, ETFs or instruments that top institutional investors and hedge funds "
        "are quietly accumulating NOW with high profit potential. "
        "Priority criteria (all must be verified with data): "
        "1) Rising institutional ownership in last 2 quarters (13F data). "
        "2) Revenue growth >10% YoY or strong free cash flow yield. "
        "3) Operating margin >15% or rapidly expanding. "
        "4) Undervalued vs sector peers (P/E or EV/EBITDA below median). "
        "5) Macro tailwind alignment (energy transition, defense, AI infrastructure, reshoring). "
        "6) Low retail coverage — not yet mainstream. "
        "Include: ETFs with strong inflows and sector tailwinds too. "
        "For EACH provide: company/fund full name, ticker WITH exchange suffix, country, sector, "
        "approximate current price, P/E or EV/EBITDA, revenue growth %, "
        "why smart money is quietly building position, potential upside reason. "
        "Explicitly EXCLUDE these already-monitored tickers: " + watched_str + ". "
        "Be very specific with data. NO speculation without supporting evidence."
    )
    r4 = _perplexity_search(q4, pplx_key, 800)
    if r4:
        parts.append("=== SCOPERTE AD ALTO POTENZIALE (outside watchlist) ===\n" + r4)

    result = "\n\n".join(parts) if parts else "No data retrieved from Perplexity."
    log.info(f"[SMART_MONEY] Dati raccolti: {len(result)} chars")
    return result


# ── Analisi Claude ────────────────────────────────────────────────────────────
def _claude_analyze(prompt: str, claude_key: str) -> Optional[Dict]:
    """Invia il prompt a Claude e ottieni l'analisi strutturata."""
    global _CLAUDE_DISABLED_REASON
    if _CLAUDE_DISABLED_REASON:
        log.info(f"[SMART_MONEY] Claude skipped ({_CLAUDE_DISABLED_REASON})")
        return None

    try:
        log.info("[SMART_MONEY] Invio a Claude (~" + str(len(prompt)//4) + " token)...")
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         claude_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-sonnet-4-20250514",
                "max_tokens": 3000,
                "system":     CLAUDE_SYSTEM,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=90,
        )
        if r.status_code == 200:
            text  = r.json()["content"][0]["text"]
            clean = text.replace("```json", "").replace("```", "").strip()
            data  = json.loads(clean)
            n_opp = len(data.get("opportunities", []))
            log.info(f"[SMART_MONEY] Claude OK: {n_opp} opportunità trovate")
            return data
        detail = _provider_error_detail(r)
        log.error(f"[SMART_MONEY] Claude {r.status_code}: {detail}")
        if _is_provider_disabled_status(r.status_code):
            _CLAUDE_DISABLED_REASON = f"HTTP {r.status_code}"
        return None
    except json.JSONDecodeError as e:
        log.error(f"[SMART_MONEY] JSON parse error: {e}")
        return None
    except Exception as e:
        log.error(f"[SMART_MONEY] Claude error: {e}")
        return None


# ── Entry point pubblico ──────────────────────────────────────────────────────
def run_smart_money_analysis(
    watched_symbols: List[str],
    claude_key:      str,
    pplx_key:        str,
    force_refresh:   bool = False,
) -> Dict:
    """
    Esegue l'analisi Smart Money completa.
    Cache di 6 ore — usa force_refresh=True per forzare aggiornamento.
    """
    global _CACHE

    # Controlla cache
    if not force_refresh and _CACHE["data"] and (time.time() - _CACHE["ts"]) < CACHE_TTL:
        age_min = int((time.time() - _CACHE["ts"]) / 60)
        log.info(f"[SMART_MONEY] Cache hit (aggiornato {age_min} min fa)")
        return _CACHE["data"]

    if not claude_key:
        return {
            "error":      "Claude API key non configurata",
            "analysis_date": datetime.now().strftime("%d %B %Y"),
            "opportunities": [],
            "strategic_summary": {"trend": "", "sector_insights": "", "conviction_level": ""},
            "warnings":   ["Claude API key mancante"],
            "sources_used": [],
            "data_quality": "none",
        }

    log.info("[SMART_MONEY] ========== AVVIO ANALISI ISTITUZIONALE ==========")

    # Step 1: Raccolta dati real-time
    pplx_data = _gather_institutional_data(watched_symbols, pplx_key)

    # Step 2: Analisi Claude
    prompt = _build_claude_prompt(pplx_data, watched_symbols)
    result = _claude_analyze(prompt, claude_key)

    if not result:
        result = {
            "error":         "Analisi non disponibile",
            "analysis_date": datetime.now().strftime("%d %B %Y"),
            "opportunities": [],
            "strategic_summary": {"trend": "N/A", "sector_insights": "N/A", "conviction_level": "N/A"},
            "warnings":      ["Analisi fallita - riprova tra qualche minuto"],
            "sources_used":  [],
            "data_quality":  "none",
        }

    result["computed_at"]    = datetime.now().isoformat()
    result["cache_until"]    = datetime.fromtimestamp(time.time() + CACHE_TTL).isoformat()
    result["symbols_watched"]= watched_symbols

    _CACHE = {"data": result, "ts": time.time()}
    log.info(f"[SMART_MONEY] ========== DONE: {len(result.get('opportunities',[]))} opportunità ==========")
    return result


# ── Sezione email HTML ────────────────────────────────────────────────────────
_SIGNAL_ICONS_IT = {
    "New Position": "🆕 Nuova Posizione",
    "Increase":     "📈 Aumento",
    "Convergence":  "🔄 Convergenza",
    "Insider Buy":  "👔 Acquisto Insider",
    "Discovery":    "🔭 Scoperta",
}
_ACTION_IT  = {"Accumulate": "Accumulare", "Monitor": "Monitorare", "Avoid": "Evitare"}
_TECH_IT    = {"Bullish": "Rialzista", "Neutral": "Neutrale", "Weak": "Debole"}
_ALIGN_IT   = {"TAILWIND": "Vento favorevole", "HEADWIND": "Vento contrario",
                "CONTRARIAN": "Contrarian", "NEUTRAL": "Neutrale"}


def validate_signals_with_perplexity(
    signals: List[Dict],
    history_by_symbol: Dict,
    pplx_key: str,
) -> Dict[str, Dict]:
    """
    Invia i segnali computati a Perplexity per validazione finale asset per asset.
    Usa formato ultra-compatto per risparmiare token.
    Return: {symbol: {"validation": "CONFIRM|CAUTION|OVERRIDE", "note": str}}
    """
    if not pplx_key:
        return {}

    # Filtra solo segnali attivi con dati reali
    active = [
        s for s in signals
        if s.get("has_real_data") and s.get("action") not in ("HOLD", "NO_DATA")
    ]
    if not active:
        return {}

    lines = []
    for s in active[:25]:  # cap a 25 asset per non sforare il contesto
        sym    = s["symbol"]
        action = s.get("action", "?")
        cs     = s.get("composite_score", s.get("score", 0))
        conf   = s.get("confidence", 0)
        sub    = s.get("sub_scores", {})
        ind    = s.get("indicators", {})
        rm     = s.get("risk_metrics", {})
        fd     = (s.get("fundamental_detail") or {}).get("metrics", {})

        # Sub-scores compatti (solo non-zero)
        sub_parts = [f"{k[:2].upper()}{v:+d}" for k, v in sub.items() if v != 0]
        sub_str   = " ".join(sub_parts) if sub_parts else ""

        # Indicatori tecnici chiave
        ind_parts = []
        if ind.get("rsi")      is not None: ind_parts.append(f"RSI={ind['rsi']}")
        if ind.get("adx")      is not None: ind_parts.append(f"ADX={ind['adx']:.0f}")
        if ind.get("ma_cross") is not None: ind_parts.append(f"MA={ind['ma_cross'][:5]}")
        if ind.get("obv_trend"):             ind_parts.append(f"OBV={ind['obv_trend'][:4]}")
        ind_str = " ".join(ind_parts)

        # Risk metrics
        risk_parts = []
        if rm.get("sharpe_1y")          is not None: risk_parts.append(f"Sh={rm['sharpe_1y']}")
        if rm.get("max_drawdown_1y_pct") is not None: risk_parts.append(f"DD={rm['max_drawdown_1y_pct']}%")
        if rm.get("beta")                is not None: risk_parts.append(f"β={rm['beta']}")
        risk_str = " ".join(risk_parts)

        # Fundamentals (se disponibili)
        fund_parts = []
        if fd.get("div_yield") and fd["div_yield"] > 0:
            fund_parts.append(f"Div={fd['div_yield']*100:.1f}%")
        if fd.get("roe") is not None:
            fund_parts.append(f"ROE={fd['roe']*100:.0f}%")
        fund_str = " ".join(fund_parts)

        # Storico ultimi 3 segnali (se disponibile)
        hist = history_by_symbol.get(sym.upper(), [])[:3]
        hist_str = " ".join(
            f"[{h['action'][:1]}{h.get('composite_score', 0):+d}]"
            for h in hist
        ) if hist else "—"

        # Momentum
        mom = s.get("signal_momentum", {})
        mom_str = f"mom={mom.get('score_trend', 0):+.0f}(×{mom.get('n_confirms', 0)})" if mom else ""

        # Assembla riga compatta
        parts = [f"{sym} {action} CS={cs:+d}({conf}%)"]
        if sub_str:   parts.append(sub_str)
        if ind_str:   parts.append(ind_str)
        if risk_str:  parts.append(risk_str)
        if fund_str:  parts.append(fund_str)
        if mom_str:   parts.append(mom_str)
        if hist_str != "—": parts.append(f"St:{hist_str}")
        lines.append(" | ".join(parts))

    if not lines:
        return {}

    signals_block = "\n".join(lines)
    prompt = (
        "Sei un analista quant senior. Valida questi segnali di mercato computati algoritmicamente.\n"
        "Considera: forza tecnica (CS/sub-scores), indicatori, metriche di rischio, fondamentali, "
        "coerenza storica (St:), momentum.\n"
        "RISPOSTA: SOLO un JSON array valido, nessun altro testo.\n"
        'Formato: [{"s":"SYMBOL","v":"CONFIRM","n":"breve nota IT max 15 parole"},{"s":"..."}]\n'
        "v può essere: CONFIRM (segnale solido), CAUTION (valido con riserve), OVERRIDE (segnale debole).\n\n"
        "LEGENDA: CS=CompositeScore Sh=Sharpe DD=MaxDrawdown β=Beta Div=DivYield "
        "St=storico[azione:score] mom=momentum\n\n"
        "SEGNALI DA VALIDARE:\n"
        f"{signals_block}"
    )

    log.info(f"[PPLX_VALIDATE] Invio {len(lines)} segnali a Perplexity ({len(prompt)} chars)...")
    raw = _perplexity_search(prompt, pplx_key, max_tokens=800)
    if not raw:
        return {}

    # Parse JSON array dalla risposta
    import re as _re
    out: Dict[str, Dict] = {}
    try:
        m = _re.search(r'\[[\s\S]*?\]', raw)
        if m:
            items = json.loads(m.group())
            for item in items:
                sym_key = item.get("s", "").strip()
                if sym_key:
                    out[sym_key] = {
                        "validation": item.get("v", "CONFIRM"),
                        "note":       item.get("n", ""),
                    }
            log.info(f"[PPLX_VALIDATE] Parsed {len(out)} validazioni")
        else:
            log.warning("[PPLX_VALIDATE] Nessun JSON array trovato nella risposta Perplexity")
    except Exception as e:
        log.warning(f"[PPLX_VALIDATE] Parse error: {e} — risposta raw: {raw[:200]}")
    return out


def build_email_section(data: Dict, assets: List = None) -> str:
    """
    Genera la sezione Smart Money per l'email report unificata.
    assets: lista asset [{symbol, isin, ...}] per arricchire con ISIN i titoli noti.
    """
    if not data or data.get("error") or not data.get("opportunities"):
        return ""

    # Lookup ISIN da assets (watchlist)
    isin_lookup: Dict[str, str] = {}
    if assets:
        for a in assets:
            sym  = (a.get("symbol") or "").upper()
            isin = (a.get("isin")   or "")
            if sym and isin and isin not in ("", "N/A"):
                isin_lookup[sym] = isin
                base = sym.split(".")[0]
                if base not in isin_lookup:
                    isin_lookup[base] = isin

    opportunities = data.get("opportunities", [])
    summary       = data.get("strategic_summary", {})
    warnings      = data.get("warnings", [])
    quality       = data.get("data_quality", "?")
    date          = data.get("analysis_date", "")
    regime        = data.get("macro_regime", {})

    action_colors = {"Accumulate": "#16A34A", "Monitor": "#F59E0B", "Avoid": "#DC2626"}
    tech_colors   = {"Bullish": "#16A34A",    "Neutral": "#F59E0B", "Weak": "#DC2626"}
    quality_col   = {"high": "#16A34A", "medium": "#F59E0B", "low": "#DC2626"}.get(quality, "#6B7280")

    # ── Regime macro ─────────────────────────────────────────────────────────
    regime_html = ""
    if regime.get("rate_environment"):
        rate_it   = {"HIKING": "🔺 Tassi in rialzo", "CUTTING": "🔻 Tassi in calo", "HOLD": "➡ Tassi stabili"}.get(regime.get("rate_environment",""), regime.get("rate_environment",""))
        growth_it = {"EXPANSION": "📈 Espansione", "SLOWDOWN": "📉 Rallentamento", "RECESSION": "⚠ Recessione"}.get(regime.get("growth_outlook",""), regime.get("growth_outlook",""))
        risk_it   = {"RISK-ON": "🟢 Risk-On", "RISK-OFF": "🔴 Risk-Off", "NEUTRAL": "⚪ Neutrale"}.get(regime.get("risk_appetite",""), regime.get("risk_appetite",""))
        favored   = " · ".join((regime.get("favored_sectors") or [])[:4])
        headwind  = " · ".join((regime.get("headwind_sectors") or [])[:3])
        risks     = "; ".join((regime.get("key_tail_risks") or [])[:2])
        regime_html = (
            f'<div style="background:#0A0F1A;border:1px solid #1F2937;border-radius:8px;padding:12px 14px;margin-bottom:14px">'
            f'<div style="font-size:9px;color:#6B7280;letter-spacing:.1em;margin-bottom:8px">REGIME MACRO ATTUALE</div>'
            f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">'
            f'<span style="font-size:10px;padding:2px 8px;border-radius:8px;background:#1F2937;color:#D1D5DB">{rate_it}</span>'
            f'<span style="font-size:10px;padding:2px 8px;border-radius:8px;background:#1F2937;color:#D1D5DB">{growth_it}</span>'
            f'<span style="font-size:10px;padding:2px 8px;border-radius:8px;background:#1F2937;color:#D1D5DB">{risk_it}</span>'
            f'</div>'
            + (f'<div style="font-size:10px;color:#16A34A;margin-bottom:2px">✓ Settori favoriti: {favored}</div>' if favored else "")
            + (f'<div style="font-size:10px;color:#DC2626;margin-bottom:2px">✗ Settori sfavoriti: {headwind}</div>' if headwind else "")
            + (f'<div style="font-size:10px;color:#F59E0B">⚠ Rischi: {risks}</div>' if risks else "")
            + (f'<div style="font-size:11px;color:#9CA3AF;margin-top:8px;border-top:1px solid #1F2937;padding-top:8px;line-height:1.6">{summary.get("macro_regime_summary","")}</div>' if summary.get("macro_regime_summary") else "")
            + '</div>'
        )

    # ── Separazione watchlist / scoperte ─────────────────────────────────────
    sorted_opps  = sorted(opportunities, key=lambda o: o.get("score",0), reverse=True)
    in_watchlist = [o for o in sorted_opps if o.get("signal_type") != "Discovery"]
    discoveries  = [o for o in sorted_opps if o.get("signal_type") == "Discovery"]

    def _opp_rows(opps: List[Dict], is_disc: bool = False) -> str:
        rows = ""
        for opp in opps:
            action    = opp.get("action", "Monitor")
            tech      = opp.get("technical_status", "Neutral")
            score     = opp.get("score", 0)
            score_col = "#16A34A" if score >= 70 else "#F59E0B" if score >= 50 else "#DC2626"
            ac_col    = action_colors.get(action, "#6B7280")
            tc_col    = tech_colors.get(tech, "#9CA3AF")
            sig_label = _SIGNAL_ICONS_IT.get(opp.get("signal_type",""), opp.get("signal_type",""))
            action_it = _ACTION_IT.get(action, action)
            tech_it   = _TECH_IT.get(tech, tech)
            align_it  = _ALIGN_IT.get(opp.get("macro_alignment",""), opp.get("macro_alignment",""))
            investors = ", ".join((opp.get("key_investors") or [])[:2])

            # ISIN lookup
            ticker = (opp.get("ticker") or "").upper()
            isin   = isin_lookup.get(ticker) or isin_lookup.get(ticker.split(".")[0]) or ""
            isin_html = f'<div style="font-size:9px;color:#6B7280;margin-top:2px">ISIN: {isin}</div>' if isin else ""
            disc_badge = ('<div style="display:inline-block;font-size:9px;font-weight:700;padding:1px 6px;border-radius:6px;'
                          'background:#7C3AED22;color:#A78BFA;border:1px solid #7C3AED44;margin-top:3px">🔭 SCOPERTA</div>') if is_disc else ""

            rows += (
                f'<tr style="border-bottom:1px solid #1F2937;{"background:#160D28;" if is_disc else ""}">'
                # Azienda + ticker + ISIN
                f'<td style="padding:10px 12px;vertical-align:top">'
                f'<div style="font-weight:700;color:#F9FAFB;font-size:13px">{opp.get("company","")}</div>'
                f'<div style="color:#9CA3AF;font-size:11px;margin-top:2px">{opp.get("ticker","")} · {opp.get("sector","")}</div>'
                f'{isin_html}{disc_badge}'
                f'</td>'
                # Segnale + investitori
                f'<td style="padding:10px 12px;vertical-align:top;white-space:nowrap">'
                f'<div style="font-size:11px;color:#D1D5DB">{sig_label}</div>'
                + (f'<div style="font-size:10px;color:#9CA3AF;margin-top:3px">👥 {investors}</div>' if investors else "")
                + (f'<div style="font-size:9px;color:#6B7280;margin-top:2px">{align_it}</div>' if align_it else "")
                + f'</td>'
                # Perché conta + fondamentali + macro
                f'<td style="padding:10px 12px;vertical-align:top;max-width:230px">'
                f'<div style="font-size:11px;color:#D1D5DB;line-height:1.6">{opp.get("why_matters","")}</div>'
                + (f'<div style="font-size:10px;color:#6B7280;margin-top:5px">📊 {opp.get("fundamental_snapshot","")}</div>' if opp.get("fundamental_snapshot") else "")
                + (f'<div style="font-size:10px;color:#93C5FD;margin-top:4px;font-style:italic">{opp.get("macro_rationale","")}</div>' if opp.get("macro_rationale") else "")
                + f'</td>'
                # Tecnico + rischio
                f'<td style="padding:10px 12px;vertical-align:top;text-align:center;white-space:nowrap">'
                f'<div style="color:{tc_col};font-size:11px;font-weight:600">{tech_it}</div>'
                + (f'<div style="font-size:10px;color:#6B7280;margin-top:4px;max-width:120px">{opp.get("risk_summary","")[:60]}</div>' if opp.get("risk_summary") else "")
                + f'</td>'
                # Score
                f'<td style="padding:10px 12px;vertical-align:top;text-align:center">'
                f'<div style="font-size:22px;font-weight:800;color:{score_col};font-family:monospace">{score}</div>'
                f'<div style="height:4px;background:#1F2937;border-radius:2px;margin:5px auto 0;width:44px">'
                f'<div style="height:4px;width:{score}%;background:{score_col};border-radius:2px"></div></div>'
                f'</td>'
                # Azione
                f'<td style="padding:10px 12px;vertical-align:top;text-align:center">'
                f'<div style="display:inline-block;padding:4px 10px;border-radius:12px;font-size:11px;font-weight:700;'
                f'background:{ac_col}22;color:{ac_col};border:1px solid {ac_col}55">{action_it}</div>'
                f'</td>'
                f'</tr>'
            )
        return rows

    def _table(opps: List[Dict], is_disc: bool = False) -> str:
        if not opps:
            return ""
        hdr_bg = "#160D28" if is_disc else "#0D1420"
        hdr_c  = "#A78BFA"  if is_disc else "#6B7280"
        return (
            f'<table style="width:100%;border-collapse:collapse;background:#111827;'
            f'border:1px solid {"#7C3AED44" if is_disc else "#1F2937"};border-radius:8px;overflow:hidden;margin-bottom:10px">'
            f'<thead><tr style="background:{hdr_bg}">'
            + "".join(
                f'<th style="padding:8px 12px;color:{hdr_c};font-size:9px;letter-spacing:.1em;text-align:{a};font-weight:600">{h}</th>'
                for h, a in [("AZIENDA · TICKER · ISIN","left"),("SEGNALE · INVESTITORI","left"),
                              ("ANALISI","left"),("TECNICO","center"),("SCORE","center"),("AZIONE","center")]
            )
            + f'</tr></thead>'
            f'<tbody>{_opp_rows(opps, is_disc)}</tbody>'
            f'</table>'
        )

    # ── Summary strategico ────────────────────────────────────────────────────
    summary_items = [
        ("TREND EMERGENTE",         summary.get("trend","")),
        ("SETTORI CHIAVE",          summary.get("sector_insights","")),
        ("LIVELLO DI CONVINZIONE",  summary.get("conviction_level","")),
        ("SCOPERTE EMERGENTI",      summary.get("discovery_insight","")),
    ]
    summ_cells = "".join(
        f'<td style="padding:10px 12px;vertical-align:top;background:#0A0F1A;border-right:1px solid #1F2937">'
        f'<div style="font-size:9px;color:#6B7280;letter-spacing:.1em;margin-bottom:5px">{lbl}</div>'
        f'<div style="font-size:11px;color:#D1D5DB;line-height:1.6">{val}</div>'
        f'</td>'
        for lbl, val in summary_items if val
    )
    summary_html = (
        f'<table style="width:100%;border-collapse:collapse;background:#0A0F1A;'
        f'border:1px solid #1F2937;border-radius:8px;margin-top:14px">'
        f'<tr>{summ_cells}</tr></table>'
    ) if summ_cells else ""

    # ── Warning ───────────────────────────────────────────────────────────────
    warn_items = "".join(
        f'<li style="margin-bottom:4px;color:#9CA3AF;font-size:11px">⚠ {w}</li>'
        for w in warnings
    )
    warn_html = f'<ul style="margin:10px 0 0 0;padding:0 0 0 16px">{warn_items}</ul>' if warn_items else ""

    return (
        f'<div style="margin-top:30px;border-top:2px solid #1F2937;padding-top:24px">'

        # Intestazione sezione
        f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">'
        f'<div>'
        f'<div style="font-size:20px;font-weight:800;color:#F9FAFB">🏦 Analisi Smart Money</div>'
        f'<div style="font-size:11px;color:#6B7280;margin-top:2px">'
        f'Flussi istituzionali · 13F · Acquisti Insider · Scoperte · {date}</div>'
        f'</div>'
        f'<div style="text-align:right">'
        f'<div style="font-size:9px;color:#6B7280;letter-spacing:.1em">QUALITÀ DATI</div>'
        f'<div style="font-size:13px;font-weight:700;color:{quality_col}">{quality.upper()}</div>'
        f'</div></div>'

        # Regime macro
        + regime_html

        # Contatori
        + f'<div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap">'
        + "".join(
            f'<div style="background:#111827;border:1px solid {c};border-radius:6px;padding:8px 12px;text-align:center;min-width:80px">'
            f'<div style="font-size:20px;font-weight:900;color:{c}">{n}</div>'
            f'<div style="font-size:9px;color:#6B7280;margin-top:2px">{l}</div></div>'
            for l, n, c in [
                ("Accumulare", sum(1 for o in opportunities if o.get("action")=="Accumulate"), "#16A34A"),
                ("Monitorare", sum(1 for o in opportunities if o.get("action")=="Monitor"),    "#F59E0B"),
                ("Evitare",    sum(1 for o in opportunities if o.get("action")=="Avoid"),      "#DC2626"),
                ("Scoperte",   len(discoveries),                                                "#A78BFA"),
            ]
        )
        + f'</div>'

        # Tabella asset in watchlist
        + (f'<div style="font-size:10px;color:#6B7280;letter-spacing:.08em;text-transform:uppercase;margin:10px 0 6px">📊 Segnali su Asset Monitorati ({len(in_watchlist)})</div>' if in_watchlist else "")
        + _table(in_watchlist, is_disc=False)

        # Tabella scoperte
        + (
            f'<div style="margin-top:18px;padding:10px 14px;background:#160D28;border:1px solid #7C3AED44;border-radius:8px;margin-bottom:10px">'
            f'<div style="font-size:14px;font-weight:800;color:#A78BFA">🔭 Scoperte — Titoli ad Alto Potenziale</div>'
            f'<div style="font-size:11px;color:#7C3AED;margin-top:3px">Titoli fuori watchlist identificati dai grandi investitori — non ancora sul radar del grande pubblico</div>'
            f'</div>'
            + _table(discoveries, is_disc=True)
            if discoveries else ""
        )

        # Riepilogo strategico
        + summary_html

        # Warning
        + (
            f'<div style="margin-top:12px;padding:10px 14px;background:#1A0F0A;border:1px solid #DC262622;border-radius:6px">'
            f'<div style="font-size:9px;color:#DC2626;letter-spacing:.1em;font-weight:600;margin-bottom:2px">⚠ AVVERTENZE</div>'
            + warn_html + f'</div>'
            if warn_html else ""
        )

        + f'</div>'
    )
