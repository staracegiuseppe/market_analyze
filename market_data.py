# market_data.py v2.1
# Fetch diretto Yahoo Finance Chart API (no crumb/cookie issues)
# - Bypassa proxy env vars che interferiscono con Docker HA
# - Retry con backoff esponenziale
# - Logging dettagliato per debug HA
# - NO yfinance.Ticker().history() — usa requests diretti

import os, json, logging, time, warnings
from pathlib import Path
from typing  import Dict, List, Optional
from datetime import datetime
import numpy  as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")
log = logging.getLogger("market_data")

MIN_BARS = 52

# ── Yahoo Finance fetch diretto ───────────────────────────────────────────────

# Simboli che Yahoo Finance mappa diversamente
SYMBOL_MAP = {
    "FTSEMIB.MI": "FTSEMIB.MI",   # indice — tenta ^FTSEMIB se fallisce
}

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
YAHOO_CHART_URL2= "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"

_SESSION: Optional[requests.Session] = None

# ── Benchmark cache per Beta (^GSPC, TTL 4 ore) ───────────────────────────────
_BENCH_CACHE: Dict = {"returns": None, "ts": 0}
_BENCH_TTL = 4 * 3600

def _get_benchmark_returns() -> Optional[pd.Series]:
    """Ritorna le daily returns di ^GSPC (1y) dalla cache o fetcha di nuovo."""
    global _BENCH_CACHE
    if _BENCH_CACHE["returns"] is not None and (time.time() - _BENCH_CACHE["ts"]) < _BENCH_TTL:
        return _BENCH_CACHE["returns"]
    try:
        raw = _yahoo_fetch_raw("^GSPC", range_="1y")
        if raw is None:
            return None
        df = _raw_to_dataframe(raw, "^GSPC")
        if df is None or len(df) < 30:
            return None
        ret = df["Close"].pct_change().dropna()
        _BENCH_CACHE = {"returns": ret, "ts": time.time()}
        log.info(f"[MARKET] Benchmark ^GSPC aggiornato: {len(ret)} barre")
        return ret
    except Exception as e:
        log.warning(f"[MARKET] Benchmark ^GSPC fetch: {e}")
        return None


def _get_session() -> requests.Session:
    """Session con headers browser — bypassa proxy HA Docker."""
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    # Rimuove proxy env vars che causano 403 in alcuni container Docker
    for k in ["http_proxy","https_proxy","HTTP_PROXY","HTTPS_PROXY"]:
        os.environ.pop(k, None)

    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         "https://finance.yahoo.com/",
        "Origin":          "https://finance.yahoo.com",
        "Cache-Control":   "no-cache",
        "Pragma":          "no-cache",
    })
    # Disabilita proxy a livello di session
    s.proxies = {"http": None, "https": None}
    _SESSION = s
    log.info("[MARKET] Session HTTP inizializzata (proxy disabilitato)")
    return s


def _yahoo_fetch_raw(symbol: str, range_: str = "1y", interval: str = "1d") -> Optional[Dict]:
    """
    Chiama Yahoo Finance Chart API direttamente.
    Prova query1 poi query2. Retry 3 volte con backoff.
    Log dettagliato ogni passo.
    """
    session = _get_session()
    params  = {"interval": interval, "range": range_}

    for attempt in range(1, 4):
        for base_url in [YAHOO_CHART_URL, YAHOO_CHART_URL2]:
            url = base_url.format(symbol=symbol)
            try:
                log.debug(f"[MARKET] {symbol} fetch attempt {attempt} → {url}")
                r = session.get(url, params=params, timeout=20)
                log.info(f"[MARKET] {symbol}: HTTP {r.status_code} "
                         f"({len(r.content)} bytes, attempt {attempt})")

                if r.status_code == 200:
                    data = r.json()
                    result = data.get("chart", {}).get("result")
                    if result and len(result) > 0:
                        n_bars = len(result[0].get("timestamp", []))
                        log.info(f"[MARKET] {symbol}: {n_bars} bars ricevuti OK")
                        return result[0]
                    err = data.get("chart", {}).get("error", {})
                    log.warning(f"[MARKET] {symbol}: risposta vuota — {err}")
                    break  # prova query2

                elif r.status_code == 404:
                    log.warning(f"[MARKET] {symbol}: 404 — simbolo non trovato su {base_url}")
                    break
                elif r.status_code == 429:
                    wait = 2 ** attempt
                    log.warning(f"[MARKET] {symbol}: 429 rate limit — attendo {wait}s")
                    time.sleep(wait)
                else:
                    log.warning(f"[MARKET] {symbol}: HTTP {r.status_code} — {r.text[:120]}")

            except requests.exceptions.ProxyError as e:
                log.error(f"[MARKET] {symbol}: ProxyError (attempt {attempt}) — {e}")
                log.error(f"[MARKET] {symbol}: HINT: verifica che il container HA abbia accesso diretto a internet")
            except requests.exceptions.SSLError as e:
                log.error(f"[MARKET] {symbol}: SSL Error — {e}")
            except requests.exceptions.ConnectionError as e:
                log.error(f"[MARKET] {symbol}: ConnectionError — {e}")
            except requests.exceptions.Timeout:
                log.warning(f"[MARKET] {symbol}: Timeout (attempt {attempt})")
            except Exception as e:
                log.error(f"[MARKET] {symbol}: Errore inatteso — {type(e).__name__}: {e}")

        if attempt < 3:
            wait = attempt * 1.5
            log.info(f"[MARKET] {symbol}: retry tra {wait:.1f}s...")
            time.sleep(wait)

    log.error(f"[MARKET] {symbol}: tutti i tentativi falliti — restituisco NO_DATA")
    return None


def fetch_price_series(symbol: str, range_: str = "5d", interval: str = "1h", limit: int = 48) -> List[Dict]:
    """
    Ritorna una serie prezzi semplice da Yahoo Finance Chart API.
    Utile per sparkline e mini-grafici senza usare yfinance.Ticker().history().
    Output: [{"ts": <epoch_ms>, "price": <float>}]
    """
    raw = _yahoo_fetch_raw(symbol, range_=range_, interval=interval)
    if raw is None:
        return []
    df = _raw_to_dataframe(raw, symbol)
    if df is None or df.empty or "Close" not in df.columns:
        return []
    out = []
    for idx, row in df.tail(limit).iterrows():
        try:
            ts = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
            out.append({
                "ts": int(ts.timestamp() * 1000),
                "price": float(row["Close"]),
            })
        except Exception:
            continue
    return out


def _raw_to_dataframe(raw: Dict, symbol: str) -> Optional[pd.DataFrame]:
    """Converte la risposta JSON di Yahoo in DataFrame OHLCV."""
    try:
        timestamps = raw.get("timestamp", [])
        q          = raw.get("indicators", {}).get("quote", [{}])[0]
        closes     = q.get("close", [])
        highs      = q.get("high",  [])
        lows       = q.get("low",   [])
        opens      = q.get("open",  [])
        volumes    = q.get("volume",[])

        if not timestamps or not closes:
            log.warning(f"[MARKET] {symbol}: dati mancanti nel JSON (timestamps={len(timestamps)} closes={len(closes)})")
            return None

        df = pd.DataFrame({
            "Date":   pd.to_datetime(timestamps, unit="s", utc=True),
            "Open":   opens,
            "High":   highs,
            "Low":    lows,
            "Close":  closes,
            "Volume": volumes,
        }).set_index("Date")

        # Rimuovi righe con Close NaN
        df = df.dropna(subset=["Close"])
        log.info(f"[MARKET] {symbol}: DataFrame {len(df)} righe valide "
                 f"({df.index[0].date()} → {df.index[-1].date()})")
        return df

    except Exception as e:
        log.error(f"[MARKET] {symbol}: errore conversione DataFrame — {e}")
        return None


# ── Indicatori tecnici ─────────────────────────────────────────────────────────

def _rsi(s, p=14):
    d=s.diff(); g=d.clip(lower=0).ewm(com=p-1,min_periods=p).mean()
    l=(-d.clip(upper=0)).ewm(com=p-1,min_periods=p).mean()
    return round(float((100-100/(1+g/l.replace(0,np.nan))).iloc[-1]),2)

def _bollinger(s, p=20, k=2.0):
    mid=s.rolling(p).mean(); std=s.rolling(p).std()
    u,m,l=float((mid+k*std).iloc[-1]),float(mid.iloc[-1]),float((mid-k*std).iloc[-1])
    last=float(s.iloc[-1]); bw=round((u-l)/m*100,2) if m else 0
    pos=round((last-l)/(u-l)*100,1) if (u-l) else 50
    return {"upper":round(u,4),"middle":round(m,4),"lower":round(l,4),
            "position":pos,"bandwidth":bw,
            "signal":"OVERBOUGHT" if pos>80 else "OVERSOLD" if pos<20 else "NEUTRAL"}

def _ma(s):
    last=float(s.iloc[-1])
    def mv(p): return round(float(s.rolling(p).mean().iloc[-1]),4) if len(s)>=p else None
    ma20,ma50,ma200=mv(20),mv(50),mv(200)
    cross="none"
    if ma20 and ma50 and len(s)>=51:
        p20,p50=float(s.rolling(20).mean().iloc[-2]),float(s.rolling(50).mean().iloc[-2])
        cross=("golden_cross" if p20<p50 and ma20>ma50 else
               "death_cross"  if p20>p50 and ma20<ma50 else
               "ma20_above_ma50" if ma20>ma50 else "ma20_below_ma50")
    slope5=None
    if len(s)>=25:
        v=s.rolling(20).mean().dropna().iloc[-5:]
        if len(v)==5: slope5=round((float(v.iloc[-1])-float(v.iloc[0]))/float(v.iloc[0])*100,3)
    return {"ma20":ma20,"ma50":ma50,"ma200":ma200,
            "vs_ma20":round((last-ma20)/ma20*100,2) if ma20 else None,
            "vs_ma50":round((last-ma50)/ma50*100,2) if ma50 else None,
            "vs_ma200":round((last-ma200)/ma200*100,2) if ma200 else None,
            "cross":cross,"slope_ma20_5d":slope5}

def _macd(s):
    e12=s.ewm(span=12,adjust=False).mean(); e26=s.ewm(span=26,adjust=False).mean()
    macd=e12-e26; sig=macd.ewm(span=9,adjust=False).mean(); hist=macd-sig
    h,hp=float(hist.iloc[-1]),(float(hist.iloc[-2]) if len(hist)>=2 else 0)
    return {"macd":round(float(macd.iloc[-1]),4),"signal":round(float(sig.iloc[-1]),4),
            "histogram":round(h,4),"trend":"bullish" if h>0 else "bearish",
            "crossing":"bullish_cross" if hp<0<h else "bearish_cross" if hp>0>h else "none"}

def _stoch(hi,lo,cl,k=14,d=3):
    lk=lo.rolling(k).min(); hk=hi.rolling(k).max()
    K=100*(cl-lk)/(hk-lk).replace(0,np.nan); D=K.rolling(d).mean()
    kv,dv=round(float(K.iloc[-1]),1),round(float(D.iloc[-1]),1)
    return {"k":kv,"d":dv,"signal":"OVERBOUGHT" if kv>80 else "OVERSOLD" if kv<20 else "NEUTRAL"}

def _adx(hi,lo,cl,p=14):
    pc=cl.shift(1)
    tr=pd.concat([hi-lo,(hi-pc).abs(),(lo-pc).abs()],axis=1).max(axis=1)
    up=hi-hi.shift(1); dn=lo.shift(1)-lo
    pdm=pd.Series(np.where((up>dn)&(up>0),up,0),index=cl.index)
    ndm=pd.Series(np.where((dn>up)&(dn>0),dn,0),index=cl.index)
    atr_e=tr.ewm(alpha=1/p,adjust=False).mean()
    pdi=100*pdm.ewm(alpha=1/p,adjust=False).mean()/atr_e.replace(0,np.nan)
    ndi=100*ndm.ewm(alpha=1/p,adjust=False).mean()/atr_e.replace(0,np.nan)
    dx=100*(pdi-ndi).abs()/(pdi+ndi).replace(0,np.nan)
    adx_v=float(dx.ewm(alpha=1/p,adjust=False).mean().iloc[-1])
    return {"adx":round(adx_v,2),"pdi":round(float(pdi.iloc[-1]),2),
            "ndi":round(float(ndi.iloc[-1]),2),"trending":adx_v>25}

def _atr_regime(hi,lo,cl,p=14):
    tr=pd.concat([hi-lo,(hi-cl.shift()).abs(),(lo-cl.shift()).abs()],axis=1).max(axis=1)
    atr_s=tr.rolling(p).mean().dropna()
    if len(atr_s)<20: return {"atr":None,"pct_rank":None,"regime":"UNKNOWN"}
    cur=float(atr_s.iloc[-1]); win=atr_s.iloc[-90:]
    rank=round(float((win<cur).mean())*100,1)
    return {"atr":round(cur,4),"pct_rank":rank,
            "regime":"HIGH_VOL" if rank>75 else "LOW_VOL" if rank<25 else "NORMAL_VOL"}

def _obv(cl,vol):
    d=cl.diff().apply(lambda x:1 if x>0 else(-1 if x<0 else 0))
    obv=(d*vol).cumsum(); ma=obv.rolling(20).mean()
    ov,om=float(obv.iloc[-1]),float(ma.iloc[-1])
    return {"obv":round(ov,0),"obv_ma20":round(om,0),"trend":"bullish" if ov>om else "bearish"}

def _roc(cl,p=10):
    return round(float((cl.iloc[-1]-cl.iloc[-1-p])/cl.iloc[-1-p]*100),2) if len(cl)>p else 0.0

def _donchian(hi,lo,p=20):
    dh=hi.rolling(p).max(); dl=lo.rolling(p).min()
    return {"high":round(float(dh.iloc[-1]),4),"low":round(float(dl.iloc[-1]),4),
            "mid":round(float((dh.iloc[-1]+dl.iloc[-1])/2),4)}

def _sr(hi,lo,p=20):
    sup=round(float(lo.iloc[-p:].min()),4); res=round(float(hi.iloc[-p:].max()),4)
    return {"support":sup,"resistance":res,
            "range_pct":round((res-sup)/sup*100,2) if sup else 0}

def _volume(vol):
    avg20=float(vol.rolling(20).mean().iloc[-1])
    last5=float(vol.iloc[-5:].mean())
    ratio=round(last5/avg20*100,1) if avg20 else 100
    return {"avg20":int(avg20),"last5_avg":int(last5),"ratio_pct":ratio,
            "signal":"HIGH" if ratio>120 else "LOW" if ratio<80 else "NORMAL"}

def _perf(cl):
    last=float(cl.iloc[-1])
    def p(n): return round((last-float(cl.iloc[-(n+1)]))/float(cl.iloc[-(n+1)])*100,2) if len(cl)>n else None
    return {"1d":p(1),"5d":p(5),"20d":p(20),"60d":p(60)}


def _rsi_divergence(cl, p=14, lookback=20):
    """
    Detect RSI divergence over last `lookback` bars.
    Bullish: price makes lower low but RSI makes higher low (hidden strength).
    Bearish: price makes higher high but RSI makes lower high (hidden weakness).
    Returns: 'bullish_divergence' | 'bearish_divergence' | 'none'
    """
    if len(cl) < lookback + p + 5:
        return "none"
    try:
        d = cl.diff()
        g = d.clip(lower=0).ewm(com=p-1, min_periods=p).mean()
        l = (-d.clip(upper=0)).ewm(com=p-1, min_periods=p).mean()
        rsi_series = 100 - 100 / (1 + g / l.replace(0, np.nan))

        price_win = cl.iloc[-lookback:]
        rsi_win   = rsi_series.iloc[-lookback:]

        mid = len(price_win) // 2
        p1, p2 = price_win.iloc[:mid], price_win.iloc[mid:]
        r1, r2 = rsi_win.iloc[:mid],   rsi_win.iloc[mid:]

        p_lo1, p_lo2 = float(p1.min()), float(p2.min())
        r_lo1, r_lo2 = float(r1.min()), float(r2.min())
        p_hi1, p_hi2 = float(p1.max()), float(p2.max())
        r_hi1, r_hi2 = float(r1.max()), float(r2.max())

        # Bullish div: price lower low + RSI higher low + RSI still below 55
        if p_lo2 < p_lo1 * 0.99 and r_lo2 > r_lo1 + 2 and r_lo2 < 55:
            return "bullish_divergence"
        # Bearish div: price higher high + RSI lower high + RSI above 45
        if p_hi2 > p_hi1 * 1.01 and r_hi2 < r_hi1 - 2 and r_hi2 > 45:
            return "bearish_divergence"
    except Exception:
        pass
    return "none"


def _bb_squeeze(cl, p=20, lookback=100):
    """
    Detect Bollinger Band squeeze: bandwidth at historical low → breakout imminent.
    Returns dict: squeeze (bool), percentile, breakout direction.
    """
    try:
        mid = cl.rolling(p).mean()
        std = cl.rolling(p).std()
        bw  = ((mid + 2*std) - (mid - 2*std)) / mid.replace(0, np.nan) * 100
        bw_clean = bw.dropna()
        if len(bw_clean) < 20:
            return {"squeeze": False, "bw_percentile": 50, "breakout": "none"}

        window = bw_clean.iloc[-min(lookback, len(bw_clean)):]
        cur    = float(bw_clean.iloc[-1])
        pct    = round(float((window < cur).mean()) * 100, 1)
        squeeze = pct < 20

        last   = float(cl.iloc[-1])
        upper  = float((mid + 2*std).iloc[-1])
        lower  = float((mid - 2*std).iloc[-1])
        breakout = "none"
        if squeeze:
            if last > upper:   breakout = "bullish_breakout"
            elif last < lower: breakout = "bearish_breakout"

        return {"squeeze": squeeze, "bw_percentile": pct, "breakout": breakout}
    except Exception:
        return {"squeeze": False, "bw_percentile": 50, "breakout": "none"}


def _bounce_probability(rsi, bb_pos, vs_ma200, vol_ratio, adx_val):
    """
    Estimates bounce probability (0–100%) for oversold conditions.
    High score = more likely mean-reversion / bounce.
    Only meaningful on stocks near lows (RSI < 50).
    """
    score = 50
    # RSI: the more oversold, the higher the bounce chance
    if   rsi < 20: score += 25
    elif rsi < 25: score += 20
    elif rsi < 30: score += 15
    elif rsi < 35: score += 10
    elif rsi < 40: score +=  5
    elif rsi > 65: score -= 10
    elif rsi > 70: score -= 15

    # Bollinger position: near lower band = support zone
    if   bb_pos < 5:  score += 15
    elif bb_pos < 15: score += 10
    elif bb_pos < 25: score +=  5
    elif bb_pos > 75: score -=  8
    elif bb_pos > 85: score -= 12

    # Distance from MA200: deeply oversold = reversion more likely
    if vs_ma200 is not None:
        if   vs_ma200 < -25: score += 10   # extreme overextension
        elif vs_ma200 < -15: score +=  5
        elif vs_ma200 > 20:  score -=  5

    # Volume spike = capitulation → contrarian signal
    if   vol_ratio > 200: score += 8
    elif vol_ratio > 150: score += 5
    elif vol_ratio <  60: score -= 5

    # Strong trend (ADX > 30) = momentum may continue, lower bounce chance
    if   adx_val > 40: score -= 12
    elif adx_val > 30: score -=  6

    return max(5, min(95, score))


# ── Entry point pubblico ───────────────────────────────────────────────────────

def fetch_indicators(symbol: str, period: str = "1y") -> Optional[Dict]:
    """
    Scarica dati reali e calcola tutti gli indicatori.
    Restituisce None (NO_DATA) se dati insufficienti o fetch fallisce.
    Log dettagliato visibile nel Registro HA.
    """
    log.info(f"[MARKET] → {symbol}: avvio fetch (period={period})")

    # Mappa simboli speciali (indici)
    # FTSEMIB.MI: Yahoo non supporta ^FTSEMIB via API diretta
    # Tentativo 1: FTSEMIB.MI, fallback FTSEMIB.MI con parametro diverso
    yf_symbol = symbol

    raw = _yahoo_fetch_raw(yf_symbol, range_=period)
    if raw is None:
        log.error(f"[MARKET] {symbol}: NO_DATA (fetch fallito)")
        return None

    df = _raw_to_dataframe(raw, symbol)
    if df is None or len(df) < MIN_BARS:
        n = len(df) if df is not None else 0
        log.warning(f"[MARKET] {symbol}: NO_DATA — solo {n} barre valide (minimo {MIN_BARS})")
        return None

    try:
        cl,hi,lo,vol = df["Close"],df["High"],df["Low"],df["Volume"]
        last = round(float(cl.iloc[-1]), 4)
        prev = round(float(cl.iloc[-2]), 4)

        rsi_val  = _rsi(cl)
        bb_data  = _bollinger(cl)
        ma_data  = _ma(cl)
        adx_data = _adx(hi, lo, cl)
        vol_data = _volume(vol)
        squeeze  = _bb_squeeze(cl)

        bounce_prob = _bounce_probability(
            rsi       = rsi_val,
            bb_pos    = bb_data.get("position", 50),
            vs_ma200  = ma_data.get("vs_ma200"),
            vol_ratio = vol_data.get("ratio_pct", 100),
            adx_val   = adx_data.get("adx", 0),
        )

        # ── Risk metrics (Sharpe, Sortino, MaxDD, VaR, Beta) ─────────────────
        risk_metrics = {}
        try:
            ret_s   = cl.pct_change().dropna()
            rf_d    = 0.04 / 252  # risk-free giornaliero (4% annuo)
            excess  = ret_s - rf_d
            std_all = float(ret_s.std())

            if std_all > 0:
                sharpe = round(float(excess.mean() / std_all * np.sqrt(252)), 2)
            else:
                sharpe = 0.0

            down = ret_s[ret_s < 0]
            if len(down) > 1:
                sortino = round(float(excess.mean() / down.std() * np.sqrt(252)), 2)
            else:
                sortino = 0.0

            max_dd  = round(float((cl / cl.cummax() - 1).min() * 100), 1)
            var_95  = round(float(np.percentile(ret_s, 5) * 100), 2)

            # Beta vs ^GSPC
            beta = None
            bench = _get_benchmark_returns()
            if bench is not None:
                aligned_a, aligned_b = ret_s.align(bench, join="inner")
                if len(aligned_a) >= 30 and float(aligned_b.var()) > 0:
                    beta = round(float(aligned_a.cov(aligned_b) / aligned_b.var()), 2)

            risk_metrics = {
                "sharpe_1y":          sharpe,
                "sortino_1y":         sortino,
                "max_drawdown_1y_pct":max_dd,
                "var_95_1d_pct":      var_95,
                "beta":               beta,
            }
        except Exception as re:
            log.debug(f"[MARKET] {symbol}: risk metrics error — {re}")

        ind = {
            "symbol":           symbol,
            "last_price":       last,
            "prev_close":       prev,
            "change_pct":       round((last-prev)/prev*100, 2),
            "last_date":        str(cl.index[-1].date()),
            "bars":             len(cl),
            "rsi":              rsi_val,
            "bollinger":        bb_data,
            "ma":               ma_data,
            "macd":             _macd(cl),
            "stochastic":       _stoch(hi, lo, cl),
            "adx":              adx_data,
            "atr_regime":       _atr_regime(hi, lo, cl),
            "obv":              _obv(cl, vol),
            "roc10":            _roc(cl, 10),
            "donchian20":       _donchian(hi, lo, 20),
            "support_res":      _sr(hi, lo, 20),
            "volume":           vol_data,
            "performance":      _perf(cl),
            "rsi_divergence":   _rsi_divergence(cl),
            "bb_squeeze_data":  squeeze,
            "bounce_probability": bounce_prob,
            "risk_metrics":     risk_metrics,
            "source":           "yahoo_direct",
        }
        log.info(
            f"[MARKET] ✓ {symbol}: price={last} Δ={ind['change_pct']:+.2f}% "
            f"RSI={ind['rsi']} ADX={ind['adx']['adx']} "
            f"cross={ind['ma']['cross']} OBV={ind['obv']['trend']} "
            f"vol={ind['volume']['signal']}"
        )
        return ind

    except Exception as e:
        log.error(f"[MARKET] {symbol}: errore calcolo indicatori — {type(e).__name__}: {e}")
        return None


def fetch_all(symbols: List[str], period: str = "1y") -> Dict[str, Optional[Dict]]:
    log.info(f"[MARKET] Fetch {len(symbols)} simboli: {' '.join(symbols)}")
    results = {}
    ok = 0
    for i, sym in enumerate(symbols, 1):
        log.info(f"[MARKET] [{i}/{len(symbols)}] {sym}...")
        results[sym] = fetch_indicators(sym, period)
        if results[sym]:
            ok += 1
        # Piccola pausa tra fetch per evitare rate limit Yahoo
        if i < len(symbols):
            time.sleep(0.3)

    failed = [s for s,v in results.items() if v is None]
    log.info(f"[MARKET] Completato: {ok}/{len(symbols)} OK"
             + (f" | FAILED: {' '.join(failed)}" if failed else ""))
    return results


# ── ISIN Lookup ───────────────────────────────────────────────────────────────

# OpenFIGI exchCode → (Yahoo suffix, market, default_currency, exchange_label)
_EXCH_MAP: Dict[str, tuple] = {
    "IM": (".MI",  "IT", "EUR", "BIT"),       # Italy Borsa
    "GY": (".DE",  "EU", "EUR", "XETRA"),     # Germany Xetra
    "GF": (".F",   "EU", "EUR", "FRA"),       # Frankfurt
    "FP": (".PA",  "EU", "EUR", "EPA"),       # France Euronext Paris
    "NA": (".AS",  "EU", "EUR", "AMS"),       # Netherlands
    "SM": (".MC",  "EU", "EUR", "BME"),       # Spain Madrid
    "LN": (".L",   "EU", "GBP", "LSE"),       # UK London
    "SW": (".SW",  "EU", "CHF", "SIX"),       # Switzerland
    "SS": (".ST",  "EU", "SEK", "STO"),       # Sweden
    "NO": (".OL",  "EU", "NOK", "OSL"),       # Norway
    "DC": (".CO",  "EU", "DKK", "CPH"),       # Denmark
    "HE": (".HE",  "EU", "EUR", "HSE"),       # Finland
    "BB": (".BR",  "EU", "EUR", "EBR"),       # Belgium
    "PL": (".LS",  "EU", "EUR", "ELI"),       # Portugal
    "AT": (".VI",  "EU", "EUR", "VIE"),       # Austria
    "UQ": ("",     "US", "USD", "NASDAQ"),    # NASDAQ
    "UN": ("",     "US", "USD", "NYSE"),      # NYSE
    "UP": ("",     "US", "USD", "NYSEARCA"),  # NYSE Arca (ETF)
    "UR": ("",     "US", "USD", "BATS"),      # BATS
    "US": ("",     "US", "USD", ""),          # US generic
    "JT": (".T",   "ASIA","JPY","TSE"),       # Japan
    "AX": (".AX",  "ASIA","AUD","ASX"),       # Australia
}

_SECTYPE_MAP = {
    "Common Stock":          "stock",
    "Preferred Stock":       "stock",
    "Depositary Receipt":    "stock",
    "Open-End Fund":         "etf",
    "Exchange Traded Fund":  "etf",
    "ETP":                   "etf",
    "ETF":                   "etf",
    "Index":                 "index",
    "Index Basket":          "index",
}

_OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"


def _openfigi_lookup(isin: str) -> Optional[List[Dict]]:
    """
    Query OpenFIGI to resolve ISIN → list of matching instruments.
    Free tier: 25 req/min, no API key required.
    """
    try:
        session = _get_session()
        resp = session.post(
            _OPENFIGI_URL,
            json=[{"idType": "ID_ISIN", "idValue": isin.strip().upper()}],
            headers={"Content-Type": "application/json"},
            timeout=12,
        )
        log.info(f"[ISIN] OpenFIGI {isin}: HTTP {resp.status_code}")
        if resp.status_code == 200:
            results = resp.json()
            if results and isinstance(results, list) and results[0].get("data"):
                return results[0]["data"]
        elif resp.status_code == 429:
            log.warning("[ISIN] OpenFIGI rate limit — riprova tra qualche secondo")
        else:
            log.warning(f"[ISIN] OpenFIGI errore: {resp.status_code} {resp.text[:120]}")
    except Exception as e:
        log.error(f"[ISIN] OpenFIGI exception: {e}")
    return None


def _yahoo_quote(symbol: str) -> Optional[Dict]:
    """
    Fetch Yahoo Finance quote metadata for a symbol.
    Returns: longName, shortName, currency, quoteType, exchangeName
    """
    try:
        session = _get_session()
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        r = session.get(url, params={"interval": "1d", "range": "5d"}, timeout=12)
        if r.status_code == 200:
            data = r.json()
            result = data.get("chart", {}).get("result")
            if result and len(result) > 0:
                meta = result[0].get("meta", {})
                log.info(f"[ISIN] Yahoo quote {symbol}: {meta.get('longName', meta.get('shortName', '?'))}")
                return meta
        log.warning(f"[ISIN] Yahoo quote {symbol}: HTTP {r.status_code}")
    except Exception as e:
        log.error(f"[ISIN] Yahoo quote {symbol}: {e}")
    return None


def lookup_isin(isin: str) -> Optional[Dict]:
    """
    Lookup all asset metadata from ISIN only.

    Flow:
    1. OpenFIGI → ticker + exchCode + security type + name
    2. Build Yahoo Finance symbol (ticker + exchange suffix)
    3. Validate with Yahoo Finance quote → get full name, currency confirmed
    4. Return complete asset dict ready to save.

    Returns None if ISIN cannot be resolved.
    """
    isin = isin.strip().upper()
    if len(isin) != 12:
        return {"error": f"ISIN non valido: deve essere 12 caratteri (ricevuto: {len(isin)})"}

    log.info(f"[ISIN] Lookup {isin}...")

    # Step 1: OpenFIGI
    figi_results = _openfigi_lookup(isin)
    if not figi_results:
        return {"error": f"ISIN {isin} non trovato su OpenFIGI. Verifica che sia corretto."}

    # Priorità: equity / common stock su listino principale
    def _rank(item):
        sec  = item.get("securityType", "")
        exch = item.get("exchCode", "")
        # Preferisci azioni comuni su borse principali
        score = 0
        if "Common Stock" in sec:    score += 10
        if exch in _EXCH_MAP:        score += 5
        if exch in ("IM", "UQ", "UN", "GY", "FP", "NA", "LN"): score += 3
        return score

    best = sorted(figi_results, key=_rank, reverse=True)[0]
    log.info(f"[ISIN] OpenFIGI best match: ticker={best.get('ticker')} "
             f"exch={best.get('exchCode')} type={best.get('securityType')} "
             f"name={best.get('name')}")

    ticker    = best.get("ticker", "").strip()
    exch_code = best.get("exchCode", "").strip()
    figi_name = best.get("name", "").strip().title()
    sec_type  = best.get("securityType", "Common Stock")

    if not ticker:
        return {"error": f"OpenFIGI non ha restituito un ticker per {isin}"}

    # Step 2: Build Yahoo Finance symbol
    exch_info = _EXCH_MAP.get(exch_code, ("", "?", "USD", ""))
    yf_suffix, market, currency, exchange_label = exch_info
    yf_symbol = ticker + yf_suffix

    log.info(f"[ISIN] Yahoo symbol: {yf_symbol} (suffix={yf_suffix})")

    # Step 3: Validate with Yahoo Finance
    meta    = _yahoo_quote(yf_symbol)
    if meta is None:
        # Try without suffix as fallback
        if yf_suffix:
            meta = _yahoo_quote(ticker)
            if meta:
                yf_symbol = ticker
                log.info(f"[ISIN] Fallback symbol (no suffix): {yf_symbol}")

    # Build final asset dict
    full_name  = ""
    short_name = figi_name
    if meta:
        full_name  = meta.get("longName", "") or meta.get("shortName", "")
        short_name = meta.get("shortName", "") or figi_name
        # Prefer Yahoo currency over OpenFIGI default
        if meta.get("currency"):
            currency = meta["currency"].upper()
        # Refine asset_type from Yahoo quoteType
        qt = meta.get("quoteType", "").upper()
        if qt == "ETF":
            sec_type = "ETF"
        elif qt == "EQUITY":
            sec_type = "Common Stock"
        elif qt == "INDEX":
            sec_type = "Index"
        elif qt == "MUTUALFUND":
            sec_type = "Open-End Fund"
        # Refine exchange label
        if meta.get("exchangeName"):
            exchange_label = meta["exchangeName"]

    asset_type = _SECTYPE_MAP.get(sec_type, "stock")

    result = {
        "symbol":     yf_symbol,
        "name":       short_name or figi_name,
        "full_name":  full_name or short_name or figi_name,
        "isin":       isin,
        "market":     market,
        "country":    market,
        "asset_type": asset_type,
        "currency":   currency,
        "exchange":   exchange_label,
        "enabled":    True,
        "note":       "",
        # Extra debug info
        "_figi_name":  figi_name,
        "_exch_code":  exch_code,
        "_yf_validated": meta is not None,
    }

    log.info(f"[ISIN] ✓ {isin} → {yf_symbol} | {result['name']} | "
             f"{market} {currency} {exchange_label} | type={asset_type} | "
             f"yahoo={'OK' if meta else 'NOT VALIDATED'}")
    return result


# ── Percorso condiviso assets.json (UNICA fonte di verità) ─────────────────

def assets_json_path() -> Path:
    """Restituisce il path di assets.json. Stessa logica per lettura e scrittura."""
    # Docker: /data è la directory persistente montata
    for p in [Path("/data/assets.json"), Path("/app/assets.json")]:
        if p.exists():
            return p
    # Se nessun file esiste, default a /data (persistente in Docker)
    return Path("/data/assets.json")


def _save_assets(assets_list: list) -> None:
    """Salva la lista completa assets.json nel path condiviso."""
    p = assets_json_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(assets_list, f, indent=2, ensure_ascii=False)
    log.info(f"[ASSETS] Salvato {len(assets_list)} asset in {p}")


def load_assets(path: str = None) -> List[Dict]:
    """Carica gli asset abilitati dal path condiviso."""
    p = assets_json_path()
    if p.exists():
        assets = json.load(open(p))
        enabled = [a for a in assets if a.get("enabled", True)]
        log.info(f"[MARKET] Caricati {len(enabled)} asset da {p}")
        return enabled
    log.warning(f"[MARKET] {p} non trovato, ritorno lista vuota")
    return []
