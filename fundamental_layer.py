# fundamental_layer.py v1.0
# Layer fondamentali + istituzionali via Financial Modeling Prep (FMP)
# Frequenza: quarterly per fundamentals, daily per institutional changes
# Output: fundamental_score (-15 a +15) per asset

import os, json, logging, time
from typing import Dict, List, Optional, Tuple
import requests

log = logging.getLogger("fundamental")

_FMP_BASE  = "https://financialmodelingprep.com/api/v3"
_CACHE: Dict[str, Dict] = {}
CACHE_TTL  = 12 * 3600  # 12 ore (dati trimestrali cambiano lentamente)

_SESSION: Optional[requests.Session] = None

def _sess() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({"Accept": "application/json"})
        _SESSION.proxies = {"http": None, "https": None}
        for k in ["http_proxy","https_proxy","HTTP_PROXY","HTTPS_PROXY"]:
            os.environ.pop(k, None)
    return _SESSION


def _fmp_get(endpoint: str, fmp_key: str, params: Dict = None) -> Optional[Dict]:
    if not fmp_key:
        return None
    try:
        p = {"apikey": fmp_key}
        if params:
            p.update(params)
        r = _sess().get(f"{_FMP_BASE}/{endpoint}", params=p, timeout=15)
        if r.status_code == 200:
            return r.json()
        log.warning(f"[FUND] FMP {endpoint}: HTTP {r.status_code}")
        return None
    except Exception as e:
        log.warning(f"[FUND] FMP {endpoint}: {e}")
        return None


# ── Fundamentals per azioni ───────────────────────────────────────────────────
def _get_ratios(symbol: str, fmp_key: str) -> Optional[Dict]:
    """Key Metrics TTM: PE, EPS growth, revenue growth, FCF, margini."""
    data = _fmp_get(f"key-metrics-ttm/{symbol}", fmp_key)
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]
    return None

def _get_income(symbol: str, fmp_key: str, limit: int = 4) -> Optional[List]:
    """Income statement ultimi N trimestri per calcolare trend."""
    return _fmp_get(f"income-statement/{symbol}", fmp_key, {"period": "quarter", "limit": str(limit)})

def _get_institutional(symbol: str, fmp_key: str) -> Optional[Dict]:
    """Institutional holders e variazioni."""
    holders = _fmp_get(f"institutional-holder/{symbol}", fmp_key)
    return holders if holders and isinstance(holders, list) else None

def _get_insider(symbol: str, fmp_key: str) -> Optional[List]:
    """Insider trading ultimi 90 giorni."""
    return _fmp_get(f"insider-trading", fmp_key, {"symbol": symbol, "limit": "20"})

def _get_etf_info(symbol: str, fmp_key: str) -> Optional[Dict]:
    """Info ETF: AUM, expense ratio, net assets."""
    data = _fmp_get(f"etf-info/{symbol}", fmp_key)
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]
    return None

def _get_dividend_growth(symbol: str, fmp_key: str) -> Optional[float]:
    """
    Calcola CAGR dividendo sugli ultimi anni da FMP dividend history.
    Return: float (es. 0.06 = +6%/anno) o None se dati insufficienti.
    """
    data = _fmp_get(f"historical-price-full/stock_dividend/{symbol}", fmp_key, {"limit": "8"})
    if not data:
        return None
    hist = data.get("historical", []) if isinstance(data, dict) else []
    divs = [h.get("adjDividend") or h.get("dividend", 0) for h in hist if (h.get("adjDividend") or h.get("dividend", 0)) > 0]
    if len(divs) < 4:
        return None
    # Confronta media ultimi 2 pagamenti vs media dei 2 più vecchi disponibili
    recent = sum(divs[:2]) / 2
    old    = sum(divs[-2:]) / 2
    if old <= 0 or recent <= 0:
        return None
    years = max(1, len(divs) / 4)  # stima anni in base alla frequenza (4 = annuale, 8 = biennale)
    try:
        cagr = (recent / old) ** (1 / years) - 1
        return round(float(cagr), 4)
    except Exception:
        return None


# ── Scoring fondamentali per azioni ──────────────────────────────────────────
def _score_stock(symbol: str, fmp_key: str) -> Tuple[int, Dict]:
    """
    fundamental_score per azione: -20 a +20
    Dimensioni: crescita, margini, valutazione, FCF, qualità, dividendo
    """
    score     = 0
    detail    = {}
    reasons   = []

    # Key metrics TTM
    km = _get_ratios(symbol, fmp_key)
    if not km:
        log.info(f"[FUND] {symbol}: no FMP data — score=0")
        return 0, {"score": 0, "detail": {}, "reasons": ["Fondamentali non disponibili"], "source": "none"}

    # 1. Revenue growth (+/-4)
    rev_growth = km.get("revenueGrowth")
    if rev_growth is not None:
        if rev_growth > 0.20:
            score += 4; detail["rev_growth"] = +4; reasons.append(f"Revenue +{rev_growth*100:.1f}% → crescita forte")
        elif rev_growth > 0.05:
            score += 2; detail["rev_growth"] = +2; reasons.append(f"Revenue +{rev_growth*100:.1f}% → crescita sana")
        elif rev_growth > 0:
            score += 1; detail["rev_growth"] = +1
        elif rev_growth > -0.10:
            score -= 1; detail["rev_growth"] = -1
        else:
            score -= 4; detail["rev_growth"] = -4; reasons.append(f"Revenue {rev_growth*100:.1f}% → contrazione")

    # 2. Margine operativo (+/-3)
    op_margin = km.get("operatingIncomeRatioTTM") or km.get("operatingProfitMargin")
    if op_margin is not None:
        if op_margin > 0.20:
            score += 3; detail["op_margin"] = +3; reasons.append(f"Margine operativo {op_margin*100:.1f}% → eccellente")
        elif op_margin > 0.10:
            score += 1; detail["op_margin"] = +1; reasons.append(f"Margine operativo {op_margin*100:.1f}% → sano")
        elif op_margin <= 0:
            score -= 3; detail["op_margin"] = -3; reasons.append("Margine operativo negativo → rischio")

    # 3. Margine lordo (+/-2)
    gross_margin = km.get("grossProfitMarginTTM")
    if gross_margin is not None:
        if gross_margin > 0.50:
            score += 2; detail["gross_margin"] = +2; reasons.append(f"Margine lordo {gross_margin*100:.1f}% → pricing power")
        elif gross_margin > 0.30:
            score += 1; detail["gross_margin"] = +1
        elif gross_margin < 0.20:
            score -= 1; detail["gross_margin"] = -1; reasons.append(f"Margine lordo basso {gross_margin*100:.1f}%")

    # 4. P/FCF — price-to-free-cash-flow (+/-3)
    p_fcf = km.get("priceToFreeCashFlowTTM")
    if p_fcf is not None and p_fcf != 0:
        if 0 < p_fcf < 15:
            score += 3; detail["p_fcf"] = +3; reasons.append(f"P/FCF {p_fcf:.1f} → sottovalutato su FCF")
        elif p_fcf < 25:
            score += 2; detail["p_fcf"] = +2; reasons.append(f"P/FCF {p_fcf:.1f} → ragionevole")
        elif p_fcf < 40:
            score += 1; detail["p_fcf"] = +1
        elif p_fcf > 40:
            score -= 2; detail["p_fcf"] = -2; reasons.append(f"P/FCF {p_fcf:.1f} → caro su FCF")
        elif p_fcf < 0:
            score -= 2; detail["p_fcf"] = -2; reasons.append("FCF negativo → attenzione")

    # 5. P/E (+/-3)
    pe = km.get("peRatioTTM")
    if pe is not None and pe > 0:
        if pe < 15:
            score += 3; detail["pe"] = +3; reasons.append(f"P/E {pe:.1f} → sottovalutato")
        elif pe < 25:
            score += 1; detail["pe"] = +1; reasons.append(f"P/E {pe:.1f} → valutazione ragionevole")
        elif pe < 40:
            score -= 1; detail["pe"] = -1
        else:
            score -= 3; detail["pe"] = -3; reasons.append(f"P/E {pe:.1f} → sopravvalutato")
    elif pe is not None and pe < 0:
        score -= 2; detail["pe"] = -2; reasons.append("P/E negativo (perdite)")

    # 6. ROE (+/-2)
    roe = km.get("roeTTM")
    if roe is not None:
        if roe > 0.20:
            score += 2; detail["roe"] = +2; reasons.append(f"ROE {roe*100:.1f}% → redditività eccellente")
        elif roe > 0.15:
            score += 1; detail["roe"] = +1
        elif roe < 0.05:
            score -= 2; detail["roe"] = -2; reasons.append(f"ROE {roe*100:.1f}% → redditività bassa")

    # 7. ROIC (+/-2)
    roic = km.get("roicTTM")
    if roic is not None:
        if roic > 0.15:
            score += 2; detail["roic"] = +2; reasons.append(f"ROIC {roic*100:.1f}% → creazione valore")
        elif roic > 0.10:
            score += 1; detail["roic"] = +1
        elif roic < 0.05:
            score -= 2; detail["roic"] = -2; reasons.append(f"ROIC {roic*100:.1f}% → capitale mal allocato")

    # 8. Debt/Equity (+/-2)
    de = km.get("debtToEquityTTM")
    if de is not None:
        if de < 0.3:
            score += 2; detail["debt"] = +2; reasons.append("Basso indebitamento")
        elif de < 1.0:
            pass
        elif de < 2.0:
            score -= 1; detail["debt"] = -1
        else:
            score -= 2; detail["debt"] = -2; reasons.append(f"Debito elevato D/E={de:.1f}")

    # 9. Dividend yield (+/-3)
    div_yield = km.get("dividendYieldTTM")
    if div_yield is not None:
        dy_pct = div_yield * 100
        if 4.0 <= dy_pct <= 6.0:
            score += 3; detail["div_yield"] = +3; reasons.append(f"Dividend yield {dy_pct:.1f}% → reddito eccellente")
        elif 2.0 <= dy_pct < 4.0:
            score += 2; detail["div_yield"] = +2; reasons.append(f"Dividend yield {dy_pct:.1f}% → buona rendita")
        elif 1.0 <= dy_pct < 2.0:
            score += 1; detail["div_yield"] = +1
        elif dy_pct > 6.0:
            score += 2; detail["div_yield"] = +2; reasons.append(f"Dividend yield {dy_pct:.1f}% → alto (verificare sostenibilità)")

    # 10. Payout ratio (+/-2)
    payout = km.get("payoutRatioTTM")
    if payout is not None and payout > 0:
        if payout < 0.40:
            score += 2; detail["payout"] = +2; reasons.append(f"Payout {payout*100:.0f}% → dividendo sostenibile")
        elif payout < 0.70:
            score += 1; detail["payout"] = +1
        elif payout > 0.90:
            score -= 2; detail["payout"] = -2; reasons.append(f"Payout {payout*100:.0f}% → dividendo a rischio taglio")

    # 11. Dividend growth (nuovo helper, +/-2)
    div_growth = _get_dividend_growth(symbol, fmp_key)
    if div_growth is not None:
        if div_growth > 0.05:
            score += 2; detail["div_growth"] = +2; reasons.append(f"Crescita dividendo +{div_growth*100:.1f}%/anno")
        elif div_growth > 0.01:
            score += 1; detail["div_growth"] = +1
        elif div_growth < 0:
            score -= 1; detail["div_growth"] = -1; reasons.append("Dividendo in riduzione")

    score = max(-20, min(20, score))
    return score, {
        "score":   score,
        "detail":  detail,
        "reasons": reasons,
        "metrics": {
            "pe":          pe,
            "p_fcf":       p_fcf,
            "rev_growth":  rev_growth,
            "op_margin":   op_margin,
            "gross_margin":gross_margin,
            "roe":         roe,
            "roic":        roic,
            "de":          de,
            "div_yield":   div_yield,
            "payout":      payout,
            "div_growth":  div_growth,
        },
        "source": "FMP",
    }


# ── Scoring fondamentali per ETF ──────────────────────────────────────────────
def _score_etf(symbol: str, fmp_key: str) -> Tuple[int, Dict]:
    """
    fundamental_score per ETF: -10 a +10
    AUM, TER, fund flows
    """
    etf = _get_etf_info(symbol, fmp_key)
    if not etf:
        return 0, {"score": 0, "detail": {}, "reasons": ["ETF info non disponibili"], "source": "none"}

    score   = 0
    detail  = {}
    reasons = []

    aum       = etf.get("netAssets") or etf.get("totalAssets")
    ter       = etf.get("expenseRatio")
    div_yield = etf.get("dividendYield") or etf.get("dividendYieldTTM")

    # AUM (+/-3)
    if aum:
        if aum > 10e9:
            score += 3; detail["aum"] = +3; reasons.append(f"AUM {aum/1e9:.1f}B → ETF liquido e stabile")
        elif aum > 1e9:
            score += 1; detail["aum"] = +1
        elif aum < 100e6:
            score -= 2; detail["aum"] = -2; reasons.append("AUM basso → rischio liquidità")

    # TER (+/-2)
    if ter is not None:
        if ter < 0.10:
            score += 2; detail["ter"] = +2; reasons.append(f"TER {ter:.2f}% → ottimo costo")
        elif ter < 0.30:
            score += 1; detail["ter"] = +1
        elif ter > 0.60:
            score -= 2; detail["ter"] = -2; reasons.append(f"TER {ter:.2f}% → costoso")

    # Dividend yield ETF (+/-2)
    if div_yield is not None and div_yield > 0:
        dy_pct = div_yield * 100 if div_yield < 1 else div_yield  # normalizza se già in %
        if dy_pct >= 3.0:
            score += 2; detail["div_yield"] = +2; reasons.append(f"Distribuzione ETF {dy_pct:.1f}% → buona rendita")
        elif dy_pct >= 1.5:
            score += 1; detail["div_yield"] = +1
    else:
        div_yield = None

    score = max(-10, min(10, score))
    return score, {
        "score":   score,
        "detail":  detail,
        "reasons": reasons,
        "metrics": {"aum": aum, "ter": ter, "div_yield": div_yield},
        "source":  "FMP",
    }


# ── Institutional score ────────────────────────────────────────────────────────
def _institutional_score(symbol: str, fmp_key: str) -> Tuple[int, Dict]:
    """
    institutional_score: -10 a +10
    Basato su: concentrazione holder, trend insider buying, ownership change
    """
    score   = 0
    detail  = {}
    reasons = []

    # Institutional holders
    holders = _get_institutional(symbol, fmp_key)
    if holders:
        n_holders = len(holders)
        total_shares = sum(h.get("shares", 0) for h in holders)
        # Concentrazione top-5
        top5 = sorted(holders, key=lambda h: h.get("shares", 0), reverse=True)[:5]
        top5_shares = sum(h.get("shares", 0) for h in top5)
        concentration = top5_shares / total_shares if total_shares > 0 else 0

        if n_holders > 500:
            score += 2; detail["n_holders"] = +2; reasons.append(f"{n_holders} istituzioni → ampia distribuzione")
        elif n_holders > 100:
            score += 1; detail["n_holders"] = +1
        elif n_holders < 20:
            score -= 1; detail["n_holders"] = -1; reasons.append("Pochi holder istituzionali")

        if concentration < 0.20:
            score += 2; detail["concentration"] = +2; reasons.append("Ownership distribuita → minor rischio crowding")
        elif concentration > 0.50:
            score -= 1; detail["concentration"] = -1; reasons.append("Alta concentrazione → crowding risk")

    # Insider trading
    insider = _get_insider(symbol, fmp_key)
    if insider:
        buys  = [t for t in insider if t.get("transactionType", "").upper() in ("P-PURCHASE","BUY","B")]
        sells = [t for t in insider if t.get("transactionType", "").upper() in ("S-SALE","SELL","S")]
        net_trades = len(buys) - len(sells)
        if net_trades >= 2:
            score += 3; detail["insider"] = +3; reasons.append(f"Insider buying ({len(buys)} acquisti) → segnale forte")
        elif net_trades >= 1:
            score += 2; detail["insider"] = +2; reasons.append("Insider buying recente")
        elif net_trades <= -2:
            score -= 2; detail["insider"] = -2; reasons.append(f"Insider selling ({len(sells)} vendite)")

    score = max(-10, min(10, score))
    return score, {
        "score":   score,
        "detail":  detail,
        "reasons": reasons,
        "n_holders": len(holders) if holders else None,
        "insider_net": len(buys) - len(sells) if insider else None,
        "source": "FMP" if fmp_key else "none",
    }


# ── Entry point pubblico ──────────────────────────────────────────────────────
def fetch_fundamental_score(
    symbol:    str,
    asset_type:str,  # "stock" | "etf" | "index"
    fmp_key:   str = "",
    force_refresh: bool = False,
) -> Dict:
    """
    Recupera e calcola fundamental_score e institutional_score.
    Cache 12 ore per simbolo. Graceful degradation se FMP non configurato.
    """
    cache_key = f"{symbol}_{asset_type}"
    if not force_refresh and cache_key in _CACHE:
        if (time.time() - _CACHE[cache_key]["ts"]) < CACHE_TTL:
            log.debug(f"[FUND] {symbol}: cache hit")
            return _CACHE[cache_key]["data"]

    if not fmp_key:
        result = {
            "symbol":            symbol,
            "fundamental_score": 0,
            "institutional_score": 0,
            "fundamental_detail": {"reasons": ["FMP key non configurata — fondamentali non disponibili"]},
            "institutional_detail": {},
            "source": "none",
        }
        _CACHE[cache_key] = {"data": result, "ts": time.time()}
        return result

    log.info(f"[FUND] {symbol} ({asset_type}): fetching FMP...")

    if asset_type == "stock":
        f_score, f_detail = _score_stock(symbol, fmp_key)
    elif asset_type == "etf":
        f_score, f_detail = _score_etf(symbol, fmp_key)
    else:
        f_score, f_detail = 0, {"reasons": ["Indice — fondamentali non applicabili"]}

    i_score, i_detail = _institutional_score(symbol, fmp_key)

    log.info(f"[FUND] {symbol}: fund_score={f_score:+d} inst_score={i_score:+d}")

    result = {
        "symbol":               symbol,
        "fundamental_score":    f_score,
        "institutional_score":  i_score,
        "fundamental_detail":   f_detail,
        "institutional_detail": i_detail,
        "source": "FMP",
        "timestamp": __import__('datetime').datetime.now().isoformat(),
    }
    _CACHE[cache_key] = {"data": result, "ts": time.time()}
    return result


def fetch_all_fundamentals(assets: list, fmp_key: str) -> Dict[str, Dict]:
    """Fetch fondamentali per tutti gli asset in sequenza."""
    results = {}
    for asset in assets:
        sym  = asset["symbol"]
        atyp = asset.get("asset_type", "stock")
        if atyp == "index":
            results[sym] = {"symbol": sym, "fundamental_score": 0, "institutional_score": 0,
                            "fundamental_detail": {"reasons": ["Indice"]}, "institutional_detail": {}}
            continue
        results[sym] = fetch_fundamental_score(sym, atyp, fmp_key)
        __import__('time').sleep(0.3)  # rate limit FMP
    return results
