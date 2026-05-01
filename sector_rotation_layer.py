# sector_rotation_layer.py v1.0
# Layer rotazione settoriale: performance relativa ETF settoriali vs benchmark.
#
# METODOLOGIA:
# 1. Scarica prezzi degli ETF settoriali US (XLE, XLK, XLF, XLU, XLV, XLC, XLI, XLB, XLRE, XLP, XLY)
#    e benchmark SPY (1m, 3m)
# 2. Calcola relative strength: RS = (ETF return) - (SPY return)
# 3. Classifica settori in: LEADER / IMPROVING / LAGGING / WEAKENING
# 4. Mappa ogni asset al suo settore → sector_score -10/+10
#
# OUTPUT:
#   sector_rotation_context: dict con ranking, regime, leaders, laggards
#   Per ogni asset: sector_score + sector_label

import logging
import time
from datetime import datetime
from typing   import Dict, List, Optional, Tuple
import requests

log = logging.getLogger("sector_rotation")

# ETF settoriali US e loro categoria
SECTOR_ETFS = {
    "XLE":  "Energy",
    "XLK":  "Technology",
    "XLF":  "Financials",
    "XLV":  "Healthcare",
    "XLU":  "Utilities",
    "XLC":  "Communication",
    "XLI":  "Industrials",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    "XLP":  "Consumer Staples",
    "XLY":  "Consumer Discretionary",
    "GLD":  "Gold",
    "TLT":  "Long Bonds",
    "IWM":  "Small Cap",
}

# Mappa simbolo asset → categoria settoriale
SYMBOL_TO_SECTOR = {
    # Energia
    "ENI.MI": "Energy", "XLE": "Energy", "XOM": "Energy",
    # Tecnologia
    "AAPL": "Technology", "MSFT": "Technology", "NVDA": "Technology",
    "XLK": "Technology", "QQQ": "Technology",
    # Finanziari
    "UCG.MI": "Financials", "ISP.MI": "Financials", "XLF": "Financials",
    # Comunicazioni
    "META": "Communication", "AMZN": "Communication", "XLC": "Communication",
    # Difensivi
    "ENEL.MI": "Utilities", "SRG.MI": "Utilities", "XLU": "Utilities",
    "TLT": "Long Bonds", "GLD": "Gold",
    # Small cap
    "IWM": "Small Cap",
    # Index / Multi-sector
    "SPY": "Broad Market", "QQQ": "Technology",
    "DIA": "Broad Market", "IWDA.AS": "Broad Market",
    "VWRL.AS": "Broad Market", "XDWD.DE": "Broad Market",
    "EXS1.DE": "Broad Market", "IEMG": "Emerging Markets",
    "FTSEMIB.MI": "Broad Market",
    # Auto
    "STLAM.MI": "Consumer Discretionary",
    # Industrials
    "LDO.MI": "Industrials",
    # Materials/Chemicals
    "PIRC.MI": "Materials",
    "TIT.MI": "Communication",
}

RISK_ON_SECTORS  = {"Technology", "Consumer Discretionary", "Communication", "Financials", "Industrials"}
RISK_OFF_SECTORS = {"Utilities", "Consumer Staples", "Healthcare", "Gold", "Long Bonds"}
MACRO_SECTORS    = {"Energy", "Materials"}

_CACHE: Dict = {"data": None, "ts": 0}
_CACHE_TTL = 3 * 3600  # 3 ore


def _yahoo_return(symbol: str, session: requests.Session, days: int = 63) -> Optional[float]:
    """Ritorna la performance % a N giorni via Yahoo Finance."""
    try:
        r = session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": "1d", "range": f"{days}d"},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        d = r.json()
        closes = d["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if len(closes) < 5:
            return None
        return round((closes[-1] - closes[0]) / closes[0] * 100, 2)
    except Exception:
        return None


def fetch_sector_rotation() -> Dict:
    """
    Calcola rotazione settoriale corrente.
    Scarica rendimenti 1M e 3M per 11 ETF settoriali US + SPY.
    Restituisce ranking + classificazione per ciascun settore.
    """
    global _CACHE
    if _CACHE["data"] and (time.time() - _CACHE["ts"]) < _CACHE_TTL:
        log.debug("[SECTOR] Cache hit rotazione settoriale")
        return _CACHE["data"]

    log.info("[SECTOR] Fetching rotazione settoriale (11 ETF + SPY)...")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    })
    session.proxies = {"http": None, "https": None}

    # Benchmark
    spy_1m  = _yahoo_return("SPY", session, 21)
    spy_3m  = _yahoo_return("SPY", session, 63)

    if spy_1m is None:
        log.warning("[SECTOR] SPY non disponibile — layer rotazione disabilitato")
        return {"available": False, "note": "Dati SPY non disponibili"}

    # Fetch ETF settoriali
    sector_data = {}
    for etf, sector_name in SECTOR_ETFS.items():
        ret_1m = _yahoo_return(etf, session, 21)
        ret_3m = _yahoo_return(etf, session, 63)
        if ret_1m is None:
            continue
        rs_1m = round(ret_1m - spy_1m, 2)              # Relative Strength 1M
        rs_3m = round((ret_3m or ret_1m) - (spy_3m or spy_1m), 2) if ret_3m else rs_1m
        sector_data[sector_name] = {
            "etf":    etf,
            "ret_1m": ret_1m,
            "ret_3m": ret_3m,
            "rs_1m":  rs_1m,   # vs SPY
            "rs_3m":  rs_3m,
        }
        time.sleep(0.2)

    if not sector_data:
        return {"available": False, "note": "Nessun dato ETF settoriale disponibile"}

    # Classifica per RS 1M
    ranked = sorted(sector_data.items(), key=lambda x: x[1]["rs_1m"], reverse=True)

    # Classificazione qualitativa basata su RS 1M e 3M (Mansfield-style)
    classified = {}
    for sector, d in sector_data.items():
        rs1 = d["rs_1m"]; rs3 = d["rs_3m"]
        if rs1 > 1.5 and rs3 > 2:
            status = "LEADER"      # forte sia a 1M che 3M
        elif rs1 > 1.5 and rs3 <= 2:
            status = "IMPROVING"   # forza recente ma non storica
        elif rs1 < -1.5 and rs3 < -2:
            status = "LAGGING"     # debolezza persistente
        elif rs1 < -1.5 and rs3 >= -2:
            status = "WEAKENING"   # debolezza recente
        else:
            status = "NEUTRAL"
        classified[sector] = {**d, "status": status}

    # Identifica rotazione in corso
    leaders   = [s for s,d in classified.items() if d["status"]=="LEADER"]
    improving = [s for s,d in classified.items() if d["status"]=="IMPROVING"]
    lagging   = [s for s,d in classified.items() if d["status"]=="LAGGING"]

    # Risk-on / risk-off da rotazione
    risk_on_score  = sum(1 for s in leaders+improving if s in RISK_ON_SECTORS)
    risk_off_score = sum(1 for s in leaders+improving if s in RISK_OFF_SECTORS)
    rotation_regime = (
        "RISK_ON"  if risk_on_score >= 2 and risk_off_score == 0 else
        "RISK_OFF" if risk_off_score >= 2 and risk_on_score == 0 else
        "MIXED"
    )

    ranking_1m = []
    for sector_name, _ in ranked:
        d = classified[sector_name]
        ranking_1m.append({
            "sector": sector_name,
            "rs_1m": d["rs_1m"],
            "status": d["status"],
        })

    result = {
        "available":       True,
        "timestamp":       datetime.now().isoformat(),
        "spy_1m":          spy_1m,
        "spy_3m":          spy_3m,
        "sectors":         classified,
        "ranking_1m":      ranking_1m,
        "leaders":         leaders,
        "improving":       improving,
        "lagging":         lagging,
        "rotation_regime": rotation_regime,
    }

    _CACHE = {"data": result, "ts": time.time()}
    log.info(f"[SECTOR] Completato: leaders={leaders} regime={rotation_regime} "
             f"SPY 1M={spy_1m}% 3M={spy_3m}%")
    return result


def get_sector_score(symbol: str, asset_type: str, rotation_ctx: Dict) -> Tuple[int, Dict]:
    """
    Dato un asset e il contesto di rotazione settoriale,
    restituisce sector_score -10/+10 e dettagli narrativi.
    """
    if not rotation_ctx or not rotation_ctx.get("available"):
        return 0, {"signal": "no_rotation_data", "sector": "Unknown"}

    sector = SYMBOL_TO_SECTOR.get(symbol, "Unknown")
    if sector == "Unknown" or sector == "Broad Market":
        return 0, {"signal": "sector_not_classified", "sector": sector}

    sectors = rotation_ctx.get("sectors", {})
    if sector not in sectors:
        return 0, {"signal": "sector_data_missing", "sector": sector}

    d      = sectors[sector]
    status = d.get("status", "NEUTRAL")
    rs_1m  = d.get("rs_1m", 0)
    rs_3m  = d.get("rs_3m", 0)
    regime = rotation_ctx.get("rotation_regime", "MIXED")

    # Score base da status
    score_map = {
        "LEADER":    8,
        "IMPROVING": 4,
        "NEUTRAL":   0,
        "WEAKENING": -4,
        "LAGGING":   -8,
    }
    score = score_map.get(status, 0)

    # Bonus/malus da regime
    if regime == "RISK_ON"  and sector in RISK_ON_SECTORS:  score += 2
    if regime == "RISK_OFF" and sector in RISK_OFF_SECTORS: score += 2
    if regime == "RISK_ON"  and sector in RISK_OFF_SECTORS: score -= 2
    if regime == "RISK_OFF" and sector in RISK_ON_SECTORS:  score -= 2

    score = max(-10, min(10, score))

    # Narrativa
    if status == "LEADER":
        narrative = f"Settore {sector} in leadership — RS 1M={rs_1m:+.1f}% vs SPY"
    elif status == "IMPROVING":
        narrative = f"Settore {sector} in miglioramento — RS 1M={rs_1m:+.1f}%"
    elif status == "WEAKENING":
        narrative = f"Settore {sector} in indebolimento — RS 1M={rs_1m:+.1f}%"
    elif status == "LAGGING":
        narrative = f"Settore {sector} in ritardo persistente — RS 1M={rs_1m:+.1f}%"
    else:
        narrative = f"Settore {sector} neutrale — RS 1M={rs_1m:+.1f}%"

    return score, {
        "sector":          sector,
        "status":          status,
        "rs_1m":           rs_1m,
        "rs_3m":           rs_3m,
        "rotation_regime": regime,
        "narrative":       narrative,
        "score":           score,
    }
