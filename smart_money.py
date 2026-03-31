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


# ── Prompt Claude — hedge fund analyst ───────────────────────────────────────
CLAUDE_SYSTEM = """You are a senior macro-aware hedge fund analyst.
Your mandate: identify high-potential opportunities by synthesizing institutional flows WITH the current global macro regime.
Core belief: a good stock in a bad macro environment is still a risky trade.
A mediocre stock with strong macro tailwinds can significantly outperform.
You must be explicitly macro-first, skeptical, and transparent about data limitations.
Return ONLY valid JSON with no markdown."""

def _build_claude_prompt(pplx_data: str, watched_symbols: List[str]) -> str:
    today = datetime.now().strftime("%d %B %Y")
    symbols_str = ", ".join(watched_symbols[:20])
    return f"""Date: {today}
Watched assets: {symbols_str}

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

STEP 6 — DISCOVERY: EMERGING OPPORTUNITIES (from EMERGING OPPORTUNITIES section)
For each stock found outside the watchlist:
   a) Verify macro alignment (does current environment favor this sector?)
   b) Assess institutional signal quality (real accumulation or noise?)
   c) Flag if it represents a genuine overlooked opportunity
   d) Add to opportunities list with signal_type=Discovery and is_watchlist=false
   e) Be MORE skeptical: less data = more uncertainty = default lower scores

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
        log.warning(f"[SMART_MONEY] Perplexity {r.status_code}: {r.text[:100]}")
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

    # Query 4: Titoli emergenti FUORI dalla watchlist
    watched_str = ", ".join(watched_symbols[:20])
    q4 = (
        "Identify 3-5 stocks that top institutional investors are quietly accumulating NOW, "
        "NOT widely covered in mainstream media. "
        "Focus on: companies with rising institutional ownership but low retail awareness, "
        "small or mid-cap with strong fundamentals, "
        "sectors with macro tailwinds currently ignored by retail investors. "
        "For each provide: company full name, ticker, country/exchange, sector, "
        "why smart money is interested, approximate current price. "
        "Explicitly EXCLUDE these already-monitored tickers: " + watched_str + ". "
        "Be factual and specific. Avoid mega-caps already on every fund radar."
    )
    r4 = _perplexity_search(q4, pplx_key, 600)
    if r4:
        parts.append("=== EMERGING OPPORTUNITIES (outside watchlist) ===\n" + r4)

    result = "\n\n".join(parts) if parts else "No data retrieved from Perplexity."
    log.info(f"[SMART_MONEY] Dati raccolti: {len(result)} chars")
    return result


# ── Analisi Claude ────────────────────────────────────────────────────────────
def _claude_analyze(prompt: str, claude_key: str) -> Optional[Dict]:
    """Invia il prompt a Claude e ottieni l'analisi strutturata."""
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
        log.error(f"[SMART_MONEY] Claude {r.status_code}: {r.text[:150]}")
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
def build_email_section(data: Dict) -> str:
    """Genera la sezione Smart Money per l'email report."""
    if not data or data.get("error") or not data.get("opportunities"):
        return ""

    opportunities = data.get("opportunities", [])
    summary       = data.get("strategic_summary", {})
    warnings      = data.get("warnings", [])
    quality       = data.get("data_quality", "?")
    date          = data.get("analysis_date", "")

    action_colors = {"Accumulate": "#16A34A", "Monitor": "#F59E0B", "Avoid": "#DC2626"}
    tech_colors   = {"Bullish": "#16A34A",    "Neutral": "#F59E0B", "Weak": "#DC2626"}

    # Tabella opportunità
    rows = ""
    for opp in opportunities:
        action = opp.get("action", "Monitor")
        tech   = opp.get("technical_status", "Neutral")
        score  = opp.get("score", 0)
        score_col = "#16A34A" if score >= 70 else "#F59E0B" if score >= 50 else "#DC2626"
        signal_icons = {
            "New Position":  "🆕", "Increase":    "📈",
            "Convergence":   "🔄", "Insider Buy": "👔",
        }
        signal_icon = signal_icons.get(opp.get("signal_type",""), "📊")

        rows += f"""
        <tr style="border-bottom:1px solid #1F2937">
          <td style="padding:10px 12px;vertical-align:top">
            <div style="font-weight:700;color:#F9FAFB;font-size:13px">{opp.get("company","")}</div>
            <div style="color:#9CA3AF;font-size:11px;margin-top:2px">{opp.get("ticker","")} · {opp.get("sector","")}</div>
          </td>
          <td style="padding:10px 12px;vertical-align:top;white-space:nowrap">
            <div style="font-size:12px">{signal_icon} {opp.get("signal_type","")}</div>
            <div style="font-size:10px;color:#9CA3AF;margin-top:3px">{", ".join(opp.get("key_investors",[])[:2])}</div>
          </td>
          <td style="padding:10px 12px;vertical-align:top;max-width:220px">
            <div style="font-size:11px;color:#D1D5DB;line-height:1.6">{opp.get("why_matters","")}</div>
            <div style="font-size:10px;color:#6B7280;margin-top:4px">{opp.get("fundamental_snapshot","")}</div>
          </td>
          <td style="padding:10px 12px;vertical-align:top;text-align:center;white-space:nowrap">
            <div style="color:{tech_colors.get(tech,'#9CA3AF')};font-size:11px;font-weight:600">{tech}</div>
            <div style="font-size:10px;color:#6B7280;margin-top:3px">{opp.get("risk_summary","")[:50]}</div>
          </td>
          <td style="padding:10px 12px;vertical-align:top;text-align:center">
            <div style="font-size:20px;font-weight:800;color:{score_col};font-family:monospace">{score}</div>
            <div style="height:4px;background:#1F2937;border-radius:2px;margin-top:4px;width:50px">
              <div style="height:4px;width:{score}%;background:{score_col};border-radius:2px"></div>
            </div>
          </td>
          <td style="padding:10px 12px;vertical-align:top;text-align:center">
            <div style="display:inline-block;padding:4px 10px;border-radius:12px;font-size:11px;font-weight:700;
                 background:{action_colors.get(action,'#6B7280')}22;color:{action_colors.get(action,'#9CA3AF')};
                 border:1px solid {action_colors.get(action,'#6B7280')}55">{action}</div>
          </td>
        </tr>"""

    # Summary strategico
    summary_html = ""
    if summary.get("trend"):
        summary_html = f"""
        <div style="margin-top:16px;display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px">
          <div style="background:#0A0F1A;border-radius:6px;padding:12px">
            <div style="font-size:9px;color:#6B7280;letter-spacing:.1em;margin-bottom:5px">TREND EMERGENTE</div>
            <div style="font-size:11px;color:#D1D5DB;line-height:1.6">{summary.get("trend","")}</div>
          </div>
          <div style="background:#0A0F1A;border-radius:6px;padding:12px">
            <div style="font-size:9px;color:#6B7280;letter-spacing:.1em;margin-bottom:5px">SETTORI</div>
            <div style="font-size:11px;color:#D1D5DB;line-height:1.6">{summary.get("sector_insights","")}</div>
          </div>
          <div style="background:#0A0F1A;border-radius:6px;padding:12px">
            <div style="font-size:9px;color:#6B7280;letter-spacing:.1em;margin-bottom:5px">CONVICTION LEVEL</div>
            <div style="font-size:11px;color:#D1D5DB;line-height:1.6">{summary.get("conviction_level","")}</div>
          </div>
        </div>"""

    # Warning
    warn_items = "".join(
        f'<li style="margin-bottom:4px;color:#9CA3AF;font-size:11px">⚠ {w}</li>'
        for w in warnings
    )
    warn_html = f'<ul style="margin:10px 0 0 0;padding:0 0 0 16px">{warn_items}</ul>' if warn_items else ""

    quality_col = {"high": "#16A34A", "medium": "#F59E0B", "low": "#DC2626"}.get(quality, "#6B7280")

    return f"""
    <div style="margin-top:30px;border-top:2px solid #1F2937;padding-top:24px">

      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
        <div>
          <div style="font-size:18px;font-weight:800;color:#F9FAFB">🏦 Smart Money Analysis</div>
          <div style="font-size:11px;color:#6B7280;margin-top:2px">
            Flussi istituzionali · 13F · Insider Buying · {date}
          </div>
        </div>
        <div style="text-align:right">
          <div style="font-size:9px;color:#6B7280;letter-spacing:.1em">QUALITÀ DATI</div>
          <div style="font-size:13px;font-weight:700;color:{quality_col}">{quality.upper()}</div>
        </div>
      </div>

      <table style="width:100%;border-collapse:collapse;background:#111827;border:1px solid #1F2937;border-radius:8px;overflow:hidden">
        <thead>
          <tr style="background:#0D1420">
            <th style="padding:10px 12px;color:#6B7280;font-size:9px;letter-spacing:.1em;text-align:left;font-weight:600">AZIENDA</th>
            <th style="padding:10px 12px;color:#6B7280;font-size:9px;letter-spacing:.1em;text-align:left;font-weight:600">SEGNALE</th>
            <th style="padding:10px 12px;color:#6B7280;font-size:9px;letter-spacing:.1em;text-align:left;font-weight:600">PERCHÉ CONTA</th>
            <th style="padding:10px 12px;color:#6B7280;font-size:9px;letter-spacing:.1em;text-align:center;font-weight:600">TECNICO / RISCHIO</th>
            <th style="padding:10px 12px;color:#6B7280;font-size:9px;letter-spacing:.1em;text-align:center;font-weight:600">SCORE</th>
            <th style="padding:10px 12px;color:#6B7280;font-size:9px;letter-spacing:.1em;text-align:center;font-weight:600">AZIONE</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>

      {summary_html}

      <div style="margin-top:12px;padding:10px 14px;background:#1A0F0A;border:1px solid #DC262622;border-radius:6px">
        <div style="font-size:9px;color:#DC2626;letter-spacing:.1em;font-weight:600;margin-bottom:2px">⚠ AVVERTENZE ANALISI</div>
        {warn_html}
      </div>

    </div>"""
