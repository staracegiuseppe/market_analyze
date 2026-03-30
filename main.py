# main.py v2.0 - Multi-Market Scanner
import os, json, logging, threading, time
from pathlib    import Path
from datetime   import datetime, timedelta
from fastapi    import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses       import HTMLResponse
from pydantic   import BaseModel
from typing     import Optional, List
import uvicorn

from market_data    import fetch_all, load_assets
from signal_engine  import run_scanner, is_trading_hours
from ai_validation  import apply_ai_enrichment
from backtest_engine import backtest_symbol, backtest_batch, BacktestConfig
from mailer         import send_report

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("main")


# ── Config ─────────────────────────────────────────────────────────────────────
def load_options() -> dict:
    p = Path("/data/options.json")
    if p.exists():
        try:
            opts = json.load(open(p))
            log.info("[CONFIG] /data/options.json")
            return opts
        except Exception as e:
            log.warning(f"[CONFIG] error: {e}")
    return {
        "anthropic_api_key":          os.getenv("ANTHROPIC_API_KEY",""),
        "perplexity_api_key":         os.getenv("PERPLEXITY_API_KEY",""),
        "score_threshold":            int(os.getenv("SCORE_THRESHOLD","25")),
        "scheduler_interval_minutes": int(os.getenv("SCHEDULER_MINUTES","60")),
        "scheduler_enabled":          True,
        "email_enabled":              False,
        "email_to":   os.getenv("EMAIL_TO",""),    "email_from": os.getenv("EMAIL_FROM",""),
        "smtp_host":  os.getenv("SMTP_HOST","smtp.gmail.com"),
        "smtp_port":  int(os.getenv("SMTP_PORT","587")),
        "smtp_user":  os.getenv("SMTP_USER",""),   "smtp_password": os.getenv("SMTP_PASSWORD",""),
        "smtp_tls":   True,                        "email_min_score": 40,
    }

OPTIONS = load_options()
if OPTIONS.get("anthropic_api_key"):  os.environ["ANTHROPIC_API_KEY"]  = OPTIONS["anthropic_api_key"]
if OPTIONS.get("perplexity_api_key"): os.environ["PERPLEXITY_API_KEY"] = OPTIONS["perplexity_api_key"]

CLAUDE_KEY        = os.getenv("ANTHROPIC_API_KEY","")
PPLX_KEY          = os.getenv("PERPLEXITY_API_KEY","")
SCHEDULER_MINUTES = int(OPTIONS.get("scheduler_interval_minutes",60))
SCHEDULER_ENABLED = bool(OPTIONS.get("scheduler_enabled",True))
BIND_HOST         = os.getenv("BIND_HOST","0.0.0.0")
PORT              = int(os.getenv("INGRESS_PORT","8099"))

ASSETS = load_assets()
log.info(f"[STARTUP] {len(ASSETS)} assets loaded")

# ── State ──────────────────────────────────────────────────────────────────────
state = {
    "last_run":  None,
    "next_run":  None,
    "running":   False,
    "signals":   [],
    "tech_data": {},
    "email_last":None,
    "email_ok":  None,
}

_backtest_cache: dict = {}  # {symbol: result}


# ── Scheduler ──────────────────────────────────────────────────────────────────
def run_scan():
    if state["running"]:
        log.warning("[SCHEDULER] already running, skip")
        return
    state["running"] = True
    symbols = [a["symbol"] for a in ASSETS]
    log.info(f"[SCHEDULER] START {datetime.now().strftime('%H:%M:%S')} "
             f"| {len(symbols)} assets | claude={'OK' if CLAUDE_KEY else 'NO'} "
             f"| pplx={'OK' if PPLX_KEY else 'NO'} "
             f"| window={'OPEN' if is_trading_hours() else 'CLOSED'}")

    # Step 1: fetch real market data
    log.info(f"[STEP 1/4 MARKET] fetching {len(symbols)} symbols via yfinance...")
    tech = fetch_all(symbols)
    real_count = sum(1 for v in tech.values() if v is not None)
    log.info(f"[STEP 1/4 MARKET] {real_count}/{len(symbols)} OK | "
             + " ".join(f"{s}{'✓' if tech.get(s) else '✗'}" for s in symbols[:10]))

    # Step 2: quantitative signals
    log.info(f"[STEP 2/4 QUANT] running signal scanner...")
    signals = run_scanner(ASSETS, tech)
    active  = [s for s in signals if s["action"] in ("BUY","SELL","WATCHLIST")]
    log.info(f"[STEP 2/4 QUANT] {len(active)} active signals | "
             f"BUY={sum(1 for s in signals if s['action']=='BUY')} "
             f"SELL={sum(1 for s in signals if s['action']=='SELL')} "
             f"WATCHLIST={sum(1 for s in signals if s['action']=='WATCHLIST')}")
    for s in [x for x in signals if x['action'] in ('BUY','SELL')][:5]:
        log.info(f"[STEP 2/4 QUANT] {s['symbol']:10} {s['action']:8} "
                 f"score={s['score']:+d} conf={s['confidence']}% "
                 f"entry={s['entry']} SL={s['stop_loss']} TP={s['take_profit']} RR=1:{s['risk_reward']}")

    # Step 3: AI enrichment (top 3 only)
    log.info(f"[STEP 3/4 AI] enriching top-{min(3,len(active))} signals...")
    if active and (CLAUDE_KEY or PPLX_KEY):
        signals = apply_ai_enrichment(signals, CLAUDE_KEY, PPLX_KEY)
        enriched = sum(1 for s in signals if s.get("ai_enriched"))
        log.info(f"[STEP 3/4 AI] {enriched} signals enriched")
    else:
        log.info("[STEP 3/4 AI] skipped (no active signals or no API keys)")

    run_ts  = datetime.utcnow().isoformat() + "Z"
    next_ts = (datetime.utcnow() + timedelta(minutes=SCHEDULER_MINUTES)).isoformat() + "Z"
    state.update({"signals": signals, "tech_data": tech,
                  "last_run": run_ts, "next_run": next_ts, "running": False})

    # Step 4: email
    log.info(f"[STEP 4/4 EMAIL] enabled={OPTIONS.get('email_enabled')}")
    if OPTIONS.get("email_enabled"):
        try:
            # Adapt signals list for mailer (expects 'results' format)
            ok = send_report(signals, run_ts, next_ts, OPTIONS)
            state["email_last"] = run_ts; state["email_ok"] = ok
            log.info(f"[STEP 4/4 EMAIL] {'OK' if ok else 'FAILED'}")
        except Exception as e:
            log.error(f"[STEP 4/4 EMAIL] {e}"); state["email_ok"] = False

    log.info(f"[SCHEDULER] DONE | active={len(active)} | "
             f"top={active[0]['symbol'] + ' ' + active[0]['action'] if active else 'none'}")


def _scheduler_loop():
    log.info(f"[SCHEDULER] thread started, interval={SCHEDULER_MINUTES}min")
    run_scan()
    while True:
        time.sleep(SCHEDULER_MINUTES * 60)
        if SCHEDULER_ENABLED:
            run_scan()


# ── FastAPI ────────────────────────────────────────────────────────────────────
app = FastAPI(title="Multi-Market Scanner", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


async def _html():
    for p in [Path("/app/index.html"), Path(__file__).parent/"index.html"]:
        if p.exists(): return HTMLResponse(content=p.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>index.html not found</h1>", 404)

@app.get("/",           response_class=HTMLResponse)
async def root():  return await _html()
@app.get("/index.html", response_class=HTMLResponse)
async def idx():   return await _html()

@app.get("/health")
async def health():
    return {"status":"ok","version":"2.0.0",
            "assets":len(ASSETS),"anthropic":bool(CLAUDE_KEY),"perplexity":bool(PPLX_KEY),
            "trading_window":is_trading_hours(),"bind":BIND_HOST,
            "email_enabled":bool(OPTIONS.get("email_enabled"))}

@app.get("/api/config")
async def config():
    return {"scheduler_minutes":SCHEDULER_MINUTES,"scheduler_enabled":SCHEDULER_ENABLED,
            "has_anthropic":bool(CLAUDE_KEY),"has_perplexity":bool(PPLX_KEY),
            "email_enabled":bool(OPTIONS.get("email_enabled")),
            "email_to":OPTIONS.get("email_to",""),
            "smtp_host":OPTIONS.get("smtp_host",""),
            "smtp_port":OPTIONS.get("smtp_port",587),
            "email_min_score":OPTIONS.get("email_min_score",40),
            "trading_window":is_trading_hours()}

@app.get("/api/assets")
async def get_assets():
    return {"count": len(ASSETS), "assets": ASSETS}

@app.get("/api/signals")
async def get_signals(market: Optional[str] = None,
                      asset_type: Optional[str] = None,
                      action: Optional[str] = None):
    sigs = state.get("signals", [])
    if market:     sigs = [s for s in sigs if s.get("market","").upper() == market.upper()]
    if asset_type: sigs = [s for s in sigs if s.get("asset_type","").lower() == asset_type.lower()]
    if action:     sigs = [s for s in sigs if s.get("action","").upper() == action.upper()]
    return {"last_run":state["last_run"],"next_run":state["next_run"],
            "trading_window":is_trading_hours(),
            "count":len(sigs),"signals":sigs}

@app.get("/api/signals/{symbol}")
async def get_signal(symbol: str):
    sym = symbol.upper()
    for s in state.get("signals",[]):
        if s["symbol"].upper() == sym: return s
    raise HTTPException(404, f"{symbol} not found")

@app.post("/api/scanner/refresh")
async def scanner_refresh(background_tasks: BackgroundTasks):
    if state["running"]: return {"status":"already_running"}
    background_tasks.add_task(run_scan)
    return {"status":"started"}

@app.post("/api/backtest")
async def run_backtest(symbol: str, period: str = "3y"):
    sym = symbol.upper()
    if sym in _backtest_cache:
        return _backtest_cache[sym]
    asset = next((a for a in ASSETS if a["symbol"].upper() == sym), {"symbol":sym,"name":sym,"market":"?"})
    result = backtest_symbol(sym, asset, period, BacktestConfig())
    _backtest_cache[sym] = result
    return result

@app.get("/api/backtest/{symbol}")
async def get_backtest(symbol: str):
    sym = symbol.upper()
    if sym in _backtest_cache: return _backtest_cache[sym]
    raise HTTPException(404, f"No backtest for {symbol}. POST /api/backtest?symbol={symbol}")

@app.post("/api/email/test")
async def email_test():
    if not OPTIONS.get("email_enabled"): raise HTTPException(400,"Set email_enabled: true")
    if not state.get("signals"):         raise HTTPException(400,"Run scanner first")
    try:
        ok = send_report(state["signals"],
                         state["last_run"] or datetime.utcnow().isoformat()+"Z",
                         state["next_run"] or "", OPTIONS)
        return {"status":"sent" if ok else "failed"}
    except Exception as e: raise HTTPException(500,str(e))


if __name__ == "__main__":
    threading.Thread(target=_scheduler_loop, daemon=True).start()
    log.info(f"[STARTUP] Multi-Market Scanner v2.0 | {BIND_HOST}:{PORT}")
    log.info(f"[STARTUP] {len(ASSETS)} assets | Claude={'OK' if CLAUDE_KEY else 'NO'} | "
             f"Perplexity={'OK' if PPLX_KEY else 'NO'}")
    uvicorn.run("main:app", host=BIND_HOST, port=PORT, log_level="warning")