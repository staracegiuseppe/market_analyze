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


def _yahoo_fetch_raw(symbol: str, range_: str = "1y") -> Optional[Dict]:
    """
    Chiama Yahoo Finance Chart API direttamente.
    Prova query1 poi query2. Retry 3 volte con backoff.
    Log dettagliato ogni passo.
    """
    session = _get_session()
    params  = {"interval": "1d", "range": range_}

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


# ── Entry point pubblico ───────────────────────────────────────────────────────

def fetch_indicators(symbol: str, period: str = "1y") -> Optional[Dict]:
    """
    Scarica dati reali e calcola tutti gli indicatori.
    Restituisce None (NO_DATA) se dati insufficienti o fetch fallisce.
    Log dettagliato visibile nel Registro HA.
    """
    log.info(f"[MARKET] → {symbol}: avvio fetch (period={period})")

    # Mappa simboli speciali (indici)
    yf_symbol = symbol
    if symbol == "FTSEMIB.MI":
        yf_symbol = "^FTSEMIB"
        log.info(f"[MARKET] {symbol}: remappato → {yf_symbol}")

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

        ind = {
            "symbol":      symbol,
            "last_price":  last,
            "prev_close":  prev,
            "change_pct":  round((last-prev)/prev*100, 2),
            "last_date":   str(cl.index[-1].date()),
            "bars":        len(cl),
            "rsi":         _rsi(cl),
            "bollinger":   _bollinger(cl),
            "ma":          _ma(cl),
            "macd":        _macd(cl),
            "stochastic":  _stoch(hi, lo, cl),
            "adx":         _adx(hi, lo, cl),
            "atr_regime":  _atr_regime(hi, lo, cl),
            "obv":         _obv(cl, vol),
            "roc10":       _roc(cl, 10),
            "donchian20":  _donchian(hi, lo, 20),
            "support_res": _sr(hi, lo, 20),
            "volume":      _volume(vol),
            "performance": _perf(cl),
            "source":      "yahoo_direct",
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


def load_assets(path: str = "assets.json") -> List[Dict]:
    candidates = [
        Path(path),
        Path(__file__).parent / path,
        Path("/app") / path,
        Path("/data") / path,
    ]
    for p in candidates:
        if p.exists():
            assets  = json.load(open(p))
            enabled = [a for a in assets if a.get("enabled", True)]
            log.info(f"[MARKET] Caricati {len(enabled)} asset da {p}")
            return enabled
    log.error(f"[MARKET] assets.json non trovato in: {[str(c) for c in candidates]}")
    return []