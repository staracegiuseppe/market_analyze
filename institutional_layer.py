# institutional_layer.py v1.0
# Layer istituzionale: FMP (13F, holders, insider) + SEC API
#
# OUTPUT per asset:
#   institutional_score   -10 a +10
#   institutional_detail  dict con sottopunteggi e spiegazioni
#
# SEGNALI rilevati:
#   A. Smart money accumulation    (13F QoQ increase)
#   B. Smart money distribution    (13F QoQ decrease)
#   C. Insider buying              (acquisti da executives/directors)
#   D. Crowding risk               (concentrazione eccessiva = rischio)
#   E. Institutional ownership     (variazione % ownership)

import os, json, logging, time
from datetime import datetime, timedelta
from typing   import Dict, List, Optional, Tuple
import requests

log = logging.getLogger("institutional")

_FMP_BASE = "https://financialmodelingprep.com/api/v3"
_CACHE: Dict[str, Dict] = {}
_CACHE_TTL = 6 * 3600  # 6 ore — dati 13F cambiano lentamente
_FMP_STATE: Dict[str, object] = {"blocked_until": 0.0, "reason": None, "http_status": None}

# ── HTTP session ──────────────────────────────────────────────────────────────
_session: Optional[requests.Session] = None
def _sess() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": "MarketAnalyze/1.0"})
        _session.proxies = {"http": None, "https": None}
    return _session

def _fmp(endpoint: str, fmp_key: str, params: dict = None) -> Optional[object]:
    if not fmp_key:
        return None
    if _FMP_STATE.get("blocked_until", 0) > time.time():
        return None
    p = {"apikey": fmp_key}
    if params:
        p.update(params)
    try:
        r = _sess().get(f"{_FMP_BASE}/{endpoint}", params=p, timeout=15)
        if r.status_code == 200:
            _FMP_STATE.update({"blocked_until": 0.0, "reason": None, "http_status": 200})
            d = r.json()
            return d if d else None
        if r.status_code in (401, 403):
            _FMP_STATE.update({
                "blocked_until": time.time() + 1800,
                "reason": "fmp_auth_or_plan_error",
                "http_status": r.status_code,
            })
        log.warning(f"[INST] FMP {endpoint}: HTTP {r.status_code}")
    except Exception as e:
        log.warning(f"[INST] FMP {endpoint}: {e}")
    return None


def _provider_unavailable_detail(note: str = "Provider FMP non disponibile") -> Dict:
    return {
        "available": False,
        "provider_unavailable": True,
        "provider": "FMP",
        "http_status": _FMP_STATE.get("http_status"),
        "reason": _FMP_STATE.get("reason") or "provider_unavailable",
        "note": note,
    }


# ── A. SMART MONEY ACCUMULATION/DISTRIBUTION (13F QoQ) ────────────────────────
def _score_13f(symbol: str, fmp_key: str) -> Tuple[int, Dict]:
    """
    Analizza variazione QoQ nelle posizioni 13F.
    Formula:
      ownership_change = (current_shares - prev_shares) / prev_shares * 100
      +5 se aumento > 20%   (strong accumulation)
      +3 se aumento 5-20%
      -3 se calo 5-20%
      -5 se calo > 20%       (distribution)
      +1 bonus se nuovi holder
    Limite: 13F ha 45 giorni di ritardo.
    """
    data = _fmp(f"institutional-holder/{symbol}", fmp_key)
    if not data or not isinstance(data, list) or len(data) == 0:
        return 0, {"signal": "no_data", "note": "Nessun dato 13F disponibile"}

    # Calcola variazione media ownership top holders
    changes = []
    total_shares_now  = 0
    total_shares_prev = 0
    new_positions     = 0

    for h in data[:20]:  # top 20 holders
        curr = h.get("shares")       or h.get("sharesHeld") or 0
        prev = h.get("prevShares")   or h.get("previousShares") or 0
        if prev and prev > 0:
            chg = (curr - prev) / prev * 100
            changes.append(chg)
            total_shares_now  += curr
            total_shares_prev += prev
        elif curr > 0 and not prev:
            new_positions += 1  # nuova posizione

    if not changes and not new_positions:
        return 0, {"signal": "no_change_data", "note": "Dati variazione non disponibili"}

    avg_change = sum(changes) / len(changes) if changes else 0

    # Scoring
    if avg_change > 20:    score = 5;  signal = "strong_accumulation"
    elif avg_change > 5:   score = 3;  signal = "accumulation"
    elif avg_change > 1:   score = 1;  signal = "mild_accumulation"
    elif avg_change > -1:  score = 0;  signal = "neutral"
    elif avg_change > -5:  score = -1; signal = "mild_distribution"
    elif avg_change > -20: score = -3; signal = "distribution"
    else:                  score = -5; signal = "strong_distribution"

    # Bonus nuove posizioni
    if new_positions >= 3:
        score = min(5, score + 1)

    return score, {
        "signal":          signal,
        "avg_qoq_change":  round(avg_change, 1),
        "new_positions":   new_positions,
        "holders_analyzed":len(changes),
        "note":            "Ritardo 45 giorni sui dati 13F",
    }


# ── B. INSIDER BUYING/SELLING ─────────────────────────────────────────────────
def _score_insider(symbol: str, fmp_key: str) -> Tuple[int, Dict]:
    """
    Acquisti insider (executives/directors) ultimi 90 giorni.
    Formula:
      buy_value  = sum delle transazioni BUY > $50K
      sell_value = sum delle transazioni SELL
      net_ratio  = (buy_value - sell_value) / (buy_value + sell_value)
    Interpretazione:
      net_ratio > +0.5 → insider buying forte → +4
      net_ratio > +0.2 → mild buying → +2
      net_ratio < -0.5 → insider selling → -3
      Cluster buying (3+ insiders) → bonus +1
    Limite: valore informativo elevato, ma soggetto a pianificazione fiscale.
    """
    data = _fmp("insider-trading", fmp_key, {"symbol": symbol, "limit": "30"})
    if not data or not isinstance(data, list):
        return 0, {"signal": "no_data", "note": "Nessun dato insider disponibile"}

    cutoff = datetime.now() - timedelta(days=90)
    buy_value    = 0.0
    sell_value   = 0.0
    buy_persons  = set()
    sell_persons = set()

    for t in data:
        try:
            ttype = (t.get("transactionType") or "").lower()
            value = float(t.get("transactionPrice", 0) or 0) * float(t.get("securitiesTransacted", 0) or 0)
            date  = datetime.strptime(t.get("transactionDate","2000-01-01")[:10], "%Y-%m-%d")
            if date < cutoff or value < 50_000:
                continue
            if "purchase" in ttype or "buy" in ttype:
                buy_value  += value
                buy_persons.add(t.get("reportingName",""))
            elif "sale" in ttype or "sell" in ttype:
                sell_value += value
                sell_persons.add(t.get("reportingName",""))
        except Exception:
            continue

    total = buy_value + sell_value
    if total < 50_000:
        return 0, {"signal": "no_significant_activity", "buy_value": 0, "sell_value": 0}

    net_ratio = (buy_value - sell_value) / total

    if net_ratio > 0.7:   score = 4;  signal = "strong_insider_buying"
    elif net_ratio > 0.4: score = 2;  signal = "insider_buying"
    elif net_ratio > 0.1: score = 1;  signal = "mild_insider_buying"
    elif net_ratio > -0.1:score = 0;  signal = "neutral"
    elif net_ratio > -0.4:score = -1; signal = "mild_insider_selling"
    elif net_ratio > -0.7:score = -2; signal = "insider_selling"
    else:                 score = -3; signal = "strong_insider_selling"

    # Cluster buying bonus
    if len(buy_persons) >= 3:
        score = min(5, score + 1)

    return score, {
        "signal":       signal,
        "net_ratio":    round(net_ratio, 2),
        "buy_value":    round(buy_value / 1_000),   # in migliaia
        "sell_value":   round(sell_value / 1_000),
        "buy_persons":  len(buy_persons),
        "sell_persons": len(sell_persons),
        "note":         "Acquisti/vendite insider ultimi 90gg > $50K",
    }


# ── C. OWNERSHIP CONCENTRATION E CROWDING RISK ───────────────────────────────
def _score_ownership(symbol: str, fmp_key: str) -> Tuple[int, Dict]:
    """
    Analizza concentrazione ownership istituzionale.
    Formula crowding:
      concentration = % posseduta dai top 10 holder
      Se concentration > 70% → crowding alto → penalità -2 (rischio uscite massive)
      Se ownership totale ist. > 80% e in calo → -2
      Se ownership totale ist. in crescita → +2
    """
    data = _fmp(f"institutional-holder/{symbol}", fmp_key)
    if not data or not isinstance(data, list):
        return 0, {"signal": "no_data"}

    # Top 10 holders — concentrazione
    total_shares_company = 0
    shares_top10 = 0
    ownership_changes = []

    for i, h in enumerate(data[:10]):
        shares = h.get("shares") or h.get("sharesHeld") or 0
        shares_top10 += shares
        prev = h.get("prevShares") or 0
        if prev > 0:
            ownership_changes.append((shares - prev) / prev)

    # Stima totale azioni (se disponibile)
    total_pct = sum(h.get("dateReported_pcnt") or h.get("percentageHeld") or 0 for h in data[:50])
    concentration_top10 = min(100, total_pct * 0.5) if total_pct > 0 else 50  # stima

    avg_change_pct = sum(ownership_changes) / len(ownership_changes) if ownership_changes else 0

    score = 0
    signal = "neutral"

    # Crowding risk
    if concentration_top10 > 70:
        score -= 2; signal = "high_crowding_risk"
    elif concentration_top10 > 50:
        score -= 1; signal = "moderate_crowding"

    # Trend ownership
    if avg_change_pct > 0.05:
        score += 2; signal = signal + "_accumulation" if score >= 0 else signal
    elif avg_change_pct < -0.05:
        score -= 2

    return max(-4, min(4, score)), {
        "signal":            signal,
        "concentration_top10": round(concentration_top10, 1),
        "avg_change_pct":    round(avg_change_pct * 100, 1),
        "holders_count":     len(data),
        "note":              "Concentrazione top10 holders",
    }


# ── Entry point pubblico ──────────────────────────────────────────────────────
def fetch_institutional_score(symbol: str, asset_type: str, fmp_key: str) -> Dict:
    """
    Calcola institutional_score (-10 a +10) per un singolo asset.
    Aggrega: 13F QoQ + insider + crowding.
    """
    if not fmp_key:
        return {
            "institutional_score": 0,
            "institutional_detail": {
                "available": False,
                "provider_unavailable": True,
                "note": "FMP API key non configurata",
                "signal_13f": "no_key", "signal_insider": "no_key",
            },
        }
    if _FMP_STATE.get("blocked_until", 0) > time.time():
        return {
            "institutional_score": 0,
            "available": False,
            "institutional_detail": _provider_unavailable_detail("FMP ha risposto 401/403: key non valida, piano insufficiente o quota bloccata"),
        }

    # Cache check
    cache_key = f"{symbol}_{datetime.now().strftime('%Y%m%d%H')}"
    if cache_key in _CACHE:
        log.debug(f"[INST] {symbol}: cache hit")
        return _CACHE[cache_key]

    log.info(f"[INST] {symbol}: fetching 13F/insider/ownership...")

    # ETF: analisi diversa (holders ETF, non 13F classico)
    if asset_type == "etf":
        etf_data = _fmp(f"etf-holder/{symbol}", fmp_key)
        if etf_data and isinstance(etf_data, list):
            # ETF con grandi holder istituzionali → segnale positivo di liquidità
            n_holders = len(etf_data)
            score = 2 if n_holders > 100 else 1 if n_holders > 20 else 0
            result = {
                "institutional_score": score,
                "available": True,
                "institutional_detail": {
                    "available": True,
                    "signal": "etf_institutional_support",
                    "holders_count": n_holders,
                    "note": "ETF: analisi holder (non 13F)",
                },
            }
            _CACHE[cache_key] = result
            return result
        if _FMP_STATE.get("blocked_until", 0) > time.time():
            return {"institutional_score": 0, "available": False, "institutional_detail": _provider_unavailable_detail("FMP ETF holder non disponibile")}
        return {"institutional_score": 0, "available": False, "institutional_detail": {"signal": "etf_no_data", "available": False}}

    # Stocks: analisi completa
    score_13f,    detail_13f    = _score_13f(symbol, fmp_key)
    score_insider,detail_insider= _score_insider(symbol, fmp_key)
    score_own,    detail_own    = _score_ownership(symbol, fmp_key)

    # Pesi: 13F (50%) + insider (35%) + ownership (15%)
    raw = score_13f * 0.50 + score_insider * 0.35 + score_own * 0.15
    final_score = max(-10, min(10, int(round(raw))))

    # Segnale narrativo
    if final_score >= 4:
        narrative = "Forte accumulo istituzionale — smart money in ingresso"
    elif final_score >= 2:
        narrative = "Accumulo istituzionale moderato"
    elif final_score >= -1:
        narrative = "Posizionamento istituzionale neutro"
    elif final_score >= -3:
        narrative = "Distribuzione istituzionale in corso"
    else:
        narrative = "Forte distribuzione — smart money in uscita"

    log.info(f"[INST] {symbol}: score={final_score:+d} | "
             f"13F={score_13f:+d} insider={score_insider:+d} own={score_own:+d} | {narrative}")

    result = {
        "institutional_score": final_score,
        "available": True,
        "institutional_detail": {
            "available": True,
            "narrative":      narrative,
            "score_13f":      score_13f,
            "score_insider":  score_insider,
            "score_ownership":score_own,
            "detail_13f":     detail_13f,
            "detail_insider": detail_insider,
            "detail_ownership":detail_own,
        },
    }
    _CACHE[cache_key] = result
    return result


def fetch_all_institutional(assets: List[Dict], fmp_key: str) -> Dict[str, Dict]:
    """Fetch istituzionale per tutti gli asset. Applica delay per rate limit."""
    if not fmp_key:
        log.info("[INST] FMP key mancante — layer istituzionale disabilitato")
        return {}
    results = {}
    for i, asset in enumerate(assets):
        sym  = asset["symbol"]
        atype= asset.get("asset_type","stock")
        if _FMP_STATE.get("blocked_until", 0) > time.time():
            results[sym] = {
                "institutional_score": 0,
                "available": False,
                "institutional_detail": _provider_unavailable_detail("FMP bloccato dopo risposta 401/403"),
            }
            continue
        results[sym] = fetch_institutional_score(sym, atype, fmp_key)
        if i < len(assets) - 1:
            time.sleep(0.5)  # 120 req/min FMP free tier
    log.info(f"[INST] Completato: {len(results)} asset analizzati")
    return results
