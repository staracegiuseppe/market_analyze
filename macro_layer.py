# macro_layer.py v1.0
# Layer macro: FRED (USA) + ECB (Europa) + EIA (energia) + Yahoo Finance (proxy VIX/DXY)
# Frequenza aggiornamento: daily per serie frequenti, weekly per slow data
# Output: MacroContext dict + macro_score (-20 a +20)

import os, json, logging, time
from datetime import datetime, timedelta
from typing   import Dict, Optional, Tuple
import requests

log = logging.getLogger("macro_layer")

# ── Cache ────────────────────────────────────────────────────────────────────
_CACHE: Dict = {"data": None, "ts": 0}
CACHE_TTL = 4 * 3600  # 4 ore

# ── Sessione HTTP ─────────────────────────────────────────────────────────────
_SESSION: Optional[requests.Session] = None

def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({"Accept": "application/json", "User-Agent": "MarketAnalyze/2.0"})
        _SESSION.proxies = {"http": None, "https": None}
        for k in ["http_proxy","https_proxy","HTTP_PROXY","HTTPS_PROXY"]:
            os.environ.pop(k, None)
    return _SESSION


# ── FRED API ─────────────────────────────────────────────────────────────────
# Documentazione: https://fred.stlouisfed.org/docs/api/fred/
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

FRED_SERIES = {
    "fed_funds":      "FEDFUNDS",    # Federal Funds Rate
    "cpi_usa":        "CPIAUCSL",    # CPI USA (YoY calcolato)
    "treasury_2y":    "DGS2",        # 2Y Treasury yield
    "treasury_10y":   "DGS10",       # 10Y Treasury yield
    "treasury_30y":   "DGS30",       # 30Y Treasury yield
    "yield_curve":    "T10Y2Y",      # Spread 10Y-2Y (inversione curva)
    "unemp_usa":      "UNRATE",      # Unemployment rate USA
    "gdp_growth":     "A191RL1Q225SBEA", # Real GDP growth QoQ
}

def _fred_latest(series_id: str, api_key: str) -> Optional[float]:
    """Recupera l'ultima osservazione disponibile da FRED."""
    try:
        r = _session().get(
            FRED_BASE,
            params={
                "series_id":   series_id,
                "api_key":     api_key,
                "file_type":   "json",
                "sort_order":  "desc",
                "limit":       "5",
                "observation_end": datetime.now().strftime("%Y-%m-%d"),
            },
            timeout=10,
        )
        if r.status_code == 200:
            obs = [o for o in r.json().get("observations", []) if o.get("value") != "."]
            if obs:
                return float(obs[0]["value"])
        log.warning(f"[MACRO] FRED {series_id}: HTTP {r.status_code}")
        return None
    except Exception as e:
        log.warning(f"[MACRO] FRED {series_id}: {e}")
        return None


def _fred_delta(series_id: str, api_key: str, periods: int = 1) -> Optional[float]:
    """Variazione dell'ultima vs n periodi precedenti."""
    try:
        r = _session().get(
            FRED_BASE,
            params={
                "series_id":  series_id, "api_key": api_key, "file_type": "json",
                "sort_order": "desc", "limit": str(periods + 1),
            },
            timeout=10,
        )
        if r.status_code == 200:
            obs = [float(o["value"]) for o in r.json().get("observations", []) if o.get("value") != "."]
            if len(obs) >= 2:
                return obs[0] - obs[-1]
        return None
    except Exception as e:
        log.warning(f"[MACRO] FRED delta {series_id}: {e}")
        return None


# ── ECB API (SDMX 2.1) ───────────────────────────────────────────────────────
# Documentazione: https://data-api.ecb.europa.eu/help/
ECB_BASE = "https://data-api.ecb.europa.eu/service/data"

ECB_SERIES = {
    "ecb_rate":      ("FM", "B.U2.EUR.RT0.MM.EONIA.1W"),   # Tasso BCE (proxy EONIA)
    "cpi_eurozone":  ("ICP", "M.U2.N.000000.4.ANR"),        # Inflazione eurozone YoY
}

def _ecb_latest(flow: str, key: str) -> Optional[float]:
    """Recupera l'ultima osservazione ECB via SDMX."""
    try:
        url = f"{ECB_BASE}/{flow}/{key}"
        r = _session().get(
            url,
            params={"format": "jsondata", "lastNObservations": "3"},
            timeout=12,
        )
        if r.status_code == 200:
            d    = r.json()
            obs  = d.get("dataSets", [{}])[0].get("series", {})
            # Estrae il primo valore disponibile
            for series_key, series_val in obs.items():
                observations = series_val.get("observations", {})
                if observations:
                    last_key = max(observations.keys(), key=int)
                    val = observations[last_key][0]
                    if val is not None:
                        return float(val)
        log.warning(f"[MACRO] ECB {flow}/{key}: HTTP {r.status_code}")
        return None
    except Exception as e:
        log.warning(f"[MACRO] ECB {flow}/{key}: {e}")
        return None


# ── EIA API v2 ───────────────────────────────────────────────────────────────
# Documentazione: https://www.eia.gov/opendata/
EIA_BASE = "https://api.eia.gov/v2"

def _eia_oil_price(api_key: str) -> Optional[float]:
    """Recupera prezzo Brent o WTI (spot price)."""
    try:
        # WTI spot price
        r = _session().get(
            f"{EIA_BASE}/petroleum/pri/spt/data",
            params={
                "api_key":        api_key,
                "data[]":         "value",
                "facets[series][]": "RWTC",  # WTI Cushing
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length":         "3",
            },
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json().get("response", {}).get("data", [])
            if data:
                return float(data[0]["value"])
        # Fallback: Brent
        r2 = _session().get(
            f"{EIA_BASE}/petroleum/pri/spt/data",
            params={"api_key": api_key, "data[]": "value",
                    "facets[series][]": "RBRTE", "length": "3",
                    "sort[0][column]": "period", "sort[0][direction]": "desc"},
            timeout=10,
        )
        if r2.status_code == 200:
            data2 = r2.json().get("response", {}).get("data", [])
            if data2:
                return float(data2[0]["value"])
        log.warning(f"[MACRO] EIA oil: HTTP {r.status_code}")
        return None
    except Exception as e:
        log.warning(f"[MACRO] EIA oil: {e}")
        return None


# ── Yahoo Finance proxy per VIX e DXY ────────────────────────────────────────
def _yf_quote(symbol: str) -> Optional[float]:
    """Fetch rapido via Yahoo Finance Chart API."""
    try:
        r = _session().get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": "1d", "range": "5d"},
            timeout=10,
        )
        if r.status_code == 200:
            result = r.json().get("chart", {}).get("result", [{}])[0]
            closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
            closes = [c for c in closes if c is not None]
            return round(float(closes[-1]), 2) if closes else None
        return None
    except Exception as e:
        log.warning(f"[MACRO] YF {symbol}: {e}")
        return None


# ── Scoring macro ─────────────────────────────────────────────────────────────
def _macro_score(m: Dict) -> Tuple[int, Dict]:
    """
    Calcola macro_score da -20 a +20.
    Positivo = contesto macro favorevole (risk-on, tassi stabili, inflazione bassa)
    Negativo = contesto macro sfavorevole (recessione, iperinflazione, tassi alti)
    """
    score      = 0
    breakdown  = {}
    notes      = []

    fed        = m.get("fed_funds")
    cpi        = m.get("cpi_usa")
    t10y       = m.get("treasury_10y")
    t2y        = m.get("treasury_2y")
    yield_crv  = m.get("yield_curve")      # 10Y-2Y: negativo = inversione = recessione
    vix        = m.get("vix")
    dxy        = m.get("dxy")
    oil        = m.get("oil_wti")
    ecb_rate   = m.get("ecb_rate")
    cpi_eu     = m.get("cpi_eurozone")

    # 1. Tassi Fed (+/-5)
    if fed is not None:
        if fed < 2.0:
            score += 5; breakdown["fed"] = +5; notes.append("Tassi Fed bassi → stimolativo")
        elif fed < 4.0:
            score += 2; breakdown["fed"] = +2; notes.append("Tassi Fed moderati")
        elif fed < 5.5:
            score -= 2; breakdown["fed"] = -2; notes.append("Tassi Fed elevati → restrittivo")
        else:
            score -= 5; breakdown["fed"] = -5; notes.append("Tassi Fed molto alti → headwind forte")

    # 2. Inflazione USA (+/-4)
    if cpi is not None:
        if cpi < 2.5:
            score += 4; breakdown["cpi"] = +4; notes.append(f"CPI USA {cpi:.1f}% → inflazione sotto controllo")
        elif cpi < 3.5:
            score += 1; breakdown["cpi"] = +1; notes.append(f"CPI USA {cpi:.1f}% → inflazione moderata")
        elif cpi < 5.0:
            score -= 2; breakdown["cpi"] = -2; notes.append(f"CPI USA {cpi:.1f}% → inflazione elevata")
        else:
            score -= 4; breakdown["cpi"] = -4; notes.append(f"CPI USA {cpi:.1f}% → inflazione fuori controllo")

    # 3. Curva dei rendimenti (+/-4)
    if yield_crv is not None:
        if yield_crv > 0.5:
            score += 4; breakdown["yield_curve"] = +4; notes.append("Curva normale → espansione")
        elif yield_crv > 0.0:
            score += 1; breakdown["yield_curve"] = +1; notes.append("Curva quasi piatta")
        elif yield_crv > -0.5:
            score -= 2; breakdown["yield_curve"] = -2; notes.append("Curva piatta/invertita → warning recessione")
        else:
            score -= 4; breakdown["yield_curve"] = -4; notes.append("Curva invertita → segnale recessivo forte")

    # 4. VIX - paura di mercato (+/-4)
    if vix is not None:
        if vix < 15:
            score += 4; breakdown["vix"] = +4; notes.append(f"VIX {vix:.1f} → mercati tranquilli, risk-on")
        elif vix < 20:
            score += 2; breakdown["vix"] = +2; notes.append(f"VIX {vix:.1f} → volatilità normale")
        elif vix < 30:
            score -= 2; breakdown["vix"] = -2; notes.append(f"VIX {vix:.1f} → volatilità elevata")
        else:
            score -= 4; breakdown["vix"] = -4; notes.append(f"VIX {vix:.1f} → panico di mercato, risk-off")

    # 5. DXY - dollaro (+/-3)
    if dxy is not None:
        if dxy < 98:
            score += 3; breakdown["dxy"] = +3; notes.append("USD debole → positivo per EM e commodity")
        elif dxy < 104:
            score += 1; breakdown["dxy"] = +1; notes.append("USD neutrale")
        elif dxy < 108:
            score -= 1; breakdown["dxy"] = -1; notes.append("USD forte → pressione su EM")
        else:
            score -= 3; breakdown["dxy"] = -3; notes.append("USD molto forte → headwind globale")

    # Cap finale
    score = max(-20, min(20, score))
    return score, {"score": score, "breakdown": breakdown, "notes": notes}


# ── Entry point pubblico ──────────────────────────────────────────────────────
def fetch_macro_context(
    fred_key: str = "",
    ecb_enabled: bool = True,
    eia_key: str = "",
    force_refresh: bool = False,
) -> Dict:
    """
    Recupera contesto macro completo da FRED, ECB, EIA, Yahoo.
    Cache 4 ore. Graceful degradation se API non configurate.
    """
    global _CACHE
    if not force_refresh and _CACHE["data"] and (time.time() - _CACHE["ts"]) < CACHE_TTL:
        log.info(f"[MACRO] Cache hit ({int((time.time()-_CACHE['ts'])/60)} min fa)")
        return _CACHE["data"]

    log.info("[MACRO] ========== FETCH CONTESTO MACRO ==========")
    m: Dict = {}

    # ── FRED ─────────────────────────────────────────────────────────────────
    if fred_key:
        log.info("[MACRO] FRED: fetching Fed Funds, CPI, Treasury yields...")
        m["fed_funds"]   = _fred_latest("FEDFUNDS",  fred_key)
        m["cpi_usa"]     = _fred_latest("CPIAUCSL",  fred_key)
        m["treasury_2y"] = _fred_latest("DGS2",      fred_key)
        m["treasury_10y"]= _fred_latest("DGS10",     fred_key)
        m["treasury_30y"]= _fred_latest("DGS30",     fred_key)
        m["yield_curve"] = _fred_latest("T10Y2Y",    fred_key)  # spread 10Y-2Y
        m["unemp_usa"]   = _fred_latest("UNRATE",    fred_key)
        # Variazione Fed Funds negli ultimi 12 mesi
        m["fed_delta_12m"]= _fred_delta("FEDFUNDS",  fred_key, 12)
        log.info(f"[MACRO] FRED: Fed={m.get('fed_funds')} CPI={m.get('cpi_usa')} 10Y={m.get('treasury_10y')} "
                 f"Curve={m.get('yield_curve')}")
    else:
        log.info("[MACRO] FRED: no api_key — skip")

    # ── ECB ───────────────────────────────────────────────────────────────────
    if ecb_enabled:
        log.info("[MACRO] ECB: fetching inflation Eurozona...")
        m["ecb_rate"]    = _ecb_latest("FM",  "B.U2.EUR.RT0.MM.EONIA.1W")
        m["cpi_eurozone"]= _ecb_latest("ICP", "M.U2.N.000000.4.ANR")
        log.info(f"[MACRO] ECB: tasso={m.get('ecb_rate')} CPI_EU={m.get('cpi_eurozone')}")

    # ── EIA ───────────────────────────────────────────────────────────────────
    if eia_key:
        log.info("[MACRO] EIA: fetching oil price...")
        m["oil_wti"] = _eia_oil_price(eia_key)
        log.info(f"[MACRO] EIA: WTI={m.get('oil_wti')}")
    else:
        # Fallback: leggi WTI da Yahoo Finance
        m["oil_wti"] = _yf_quote("CL=F")
        log.info(f"[MACRO] Yahoo WTI fallback: {m.get('oil_wti')}")

    # ── Yahoo Finance (VIX, DXY, Gold) ───────────────────────────────────────
    log.info("[MACRO] Yahoo: fetching VIX, DXY, Gold...")
    m["vix"]      = _yf_quote("^VIX")
    m["dxy"]      = _yf_quote("DX-Y.NYB")
    m["gold"]     = _yf_quote("GC=F")
    m["sp500"]    = _yf_quote("^GSPC")
    m["nasdaq"]   = _yf_quote("^IXIC")
    m["eurostoxx"]= _yf_quote("^STOXX50E")
    log.info(f"[MACRO] Yahoo: VIX={m.get('vix')} DXY={m.get('dxy')} "
             f"Oil={m.get('oil_wti')} Gold={m.get('gold')}")

    # ── Scoring macro ─────────────────────────────────────────────────────────
    score, score_detail = _macro_score(m)

    # ── Classifica regime macro ───────────────────────────────────────────────
    vix   = m.get("vix") or 20
    ycrv  = m.get("yield_curve")
    fed   = m.get("fed_funds") or 0
    regime = "NEUTRAL"
    if vix < 15 and (ycrv is None or ycrv > 0) and fed < 4:
        regime = "RISK_ON"
    elif vix > 30 or (ycrv is not None and ycrv < -0.5):
        regime = "RISK_OFF"
    elif vix > 20:
        regime = "CAUTIOUS"

    # ── Settori favoriti/headwind dal macro ────────────────────────────────────
    favored   = []
    headwinds = []
    if fed and fed > 4.5:
        favored   += ["Financials", "Energy", "Utilities"]
        headwinds += ["Growth Tech", "Biotech", "Real Estate"]
    if vix > 25:
        favored   += ["Gold", "Bonds", "Consumer Staples", "Healthcare"]
        headwinds += ["Consumer Discretionary", "Small Cap", "Crypto"]
    if ycrv is not None and ycrv < 0:
        favored   += ["Short Duration", "Cash", "Defensives"]
        headwinds += ["Banks", "Cyclicals"]
    if m.get("oil_wti") and m["oil_wti"] > 80:
        favored   += ["Energy", "Oil&Gas"]
        headwinds += ["Airlines", "Transportation"]

    result = {
        "timestamp":         datetime.now().isoformat(),
        "macro_score":       score,
        "macro_score_detail":score_detail,
        "regime":            regime,
        "favored_sectors":   list(dict.fromkeys(favored))[:5],
        "headwind_sectors":  list(dict.fromkeys(headwinds))[:5],
        "data": {
            "fed_funds":       m.get("fed_funds"),
            "fed_delta_12m":   m.get("fed_delta_12m"),
            "cpi_usa":         m.get("cpi_usa"),
            "treasury_2y":     m.get("treasury_2y"),
            "treasury_10y":    m.get("treasury_10y"),
            "yield_curve_10y2y": m.get("yield_curve"),
            "unemp_usa":       m.get("unemp_usa"),
            "ecb_rate":        m.get("ecb_rate"),
            "cpi_eurozone":    m.get("cpi_eurozone"),
            "oil_wti":         m.get("oil_wti"),
            "vix":             m.get("vix"),
            "dxy":             m.get("dxy"),
            "gold":            m.get("gold"),
            "sp500":           m.get("sp500"),
            "nasdaq":          m.get("nasdaq"),
            "eurostoxx":       m.get("eurostoxx"),
        },
        "sources": {
            "fred":  bool(fred_key),
            "ecb":   ecb_enabled,
            "eia":   bool(eia_key),
            "yahoo": True,
        },
    }

    log.info(f"[MACRO] DONE: score={score:+d} regime={regime} "
             f"favored={favored[:3]} headwinds={headwinds[:3]}")

    _CACHE = {"data": result, "ts": time.time()}
    return result
