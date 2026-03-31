# main.py v2.0 - Multi-Market Scanner
import os, json, logging, threading, time
from pathlib    import Path
from datetime   import datetime, timedelta
from fastapi    import FastAPI, HTTPException, BackgroundTasks, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses       import HTMLResponse
from pydantic   import BaseModel
from typing     import Optional, List
import uvicorn

from market_data       import fetch_all, load_assets
from smart_money       import run_smart_money_analysis, build_email_section
from macro_layer       import fetch_macro_context
from fundamental_layer import fetch_all_fundamentals
from scoring_engine    import run_composite_scanner
from signal_engine  import run_scanner, is_trading_hours
from ai_validation  import apply_ai_enrichment
from backtest_engine import backtest_symbol, backtest_batch, BacktestConfig
from mailer         import send_report

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("main")


# â”€â”€ Config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
FRED_KEY          = os.getenv("FRED_API_KEY","") or OPTIONS.get("fred_api_key","")
FMP_KEY           = os.getenv("FMP_API_KEY","")  or OPTIONS.get("fmp_api_key","")
EIA_KEY           = os.getenv("EIA_API_KEY","")  or OPTIONS.get("eia_api_key","")
INST_ENABLED      = bool(FMP_KEY)   # institutional_layer richiede FMP
SECTOR_ENABLED    = True             # sector_rotation usa solo Yahoo
MACRO_ENABLED     = bool(OPTIONS.get("enable_macro_layer", True))
FUND_ENABLED      = bool(OPTIONS.get("enable_fundamental_layer", False))
MACRO_UPDATE_H    = int(OPTIONS.get("macro_update_hours", 4))
SCHEDULER_MINUTES = int(OPTIONS.get("scheduler_interval_minutes",60))
SCHEDULER_ENABLED = bool(OPTIONS.get("scheduler_enabled",True))
BIND_HOST         = os.getenv("BIND_HOST","0.0.0.0")
PORT              = int(os.getenv("INGRESS_PORT","8099"))

ASSETS = load_assets()
log.info(f"[STARTUP] {len(ASSETS)} assets loaded")

# Percorso assets.json scrivibile â€” prioritÃ  /app, poi /data
def _assets_path() -> Path:
    for p in [Path("/app/assets.json"), Path(__file__).parent/"assets.json",
              Path("assets.json"), Path("/data/assets.json")]:
        if p.exists():
            return p
    return Path("/app/assets.json")  # fallback scrittura

def _save_assets(assets_list: list) -> None:
    """Salva la lista completa assets.json (inclusi disabled)."""
    p = _assets_path()
    with open(p, "w", encoding="utf-8") as f:
        json.dump(assets_list, f, indent=2, ensure_ascii=False)
    log.info(f"[ASSETS] Salvato {len(assets_list)} asset in {p}")

def _load_all_assets() -> list:
    """Carica TUTTI gli asset (inclusi disabled) per la gestione CRUD."""
    p = _assets_path()
    if p.exists():
        return json.load(open(p, encoding="utf-8"))
    return []

# Modello Pydantic per asset
class AssetModel(BaseModel):
    symbol:     str
    name:       str
    full_name:  str = ""
    isin:       str = ""
    market:     str = "US"     # IT | EU | US
    country:    str = "US"
    asset_type: str = "stock"  # stock | etf | index
    currency:   str = "USD"
    exchange:   str = ""
    enabled:    bool = True
    note:       str = ""

# â”€â”€ State â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
state = {
    "last_run":    None,
    "next_run":    None,
    "running":     False,
    "signals":     [],
    "tech_data":   {},
    "email_last":  None,
    "email_ok":    None,
    "smart_money":    None,
    "macro_context":  None,
    "fund_data":      {},
}

_backtest_cache: dict = {}  # {symbol: result}


# â”€â”€ Scheduler â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
             + " ".join(f"{s}{'âœ“' if tech.get(s) else 'âœ—'}" for s in symbols[:10]))

    # Step 2a: Macro layer (FRED + ECB + EIA + Yahoo)
    macro_ctx = None
    if MACRO_ENABLED:
        log.info("[STEP 2a/5 MACRO] Fetch contesto macro (FRED/ECB/EIA)...")
        try:
            macro_ctx = fetch_macro_context(
                fred_key=FRED_KEY, ecb_enabled=True, eia_key=EIA_KEY
            )
            state["macro_context"] = macro_ctx
            log.info(f"[STEP 2a/5 MACRO] score={macro_ctx.get('macro_score',0):+d} "
                     f"regime={macro_ctx.get('regime')} "
                     f"VIX={macro_ctx.get('data',{}).get('vix')} "
                     f"Fed={macro_ctx.get('data',{}).get('fed_funds')} "
                     f"10Y={macro_ctx.get('data',{}).get('treasury_10y')}")
        except Exception as e:
            log.error(f"[STEP 2a/5 MACRO] errore: {e}")

    # Step 2b: Fundamental layer (FMP)
    fund_db = {}
    if FUND_ENABLED and FMP_KEY:
        log.info("[STEP 2b/5 FUND] Fetch fondamentali FMP...")
        try:
            fund_db = fetch_all_fundamentals(ASSETS, FMP_KEY)
            state["fund_data"] = fund_db
            ok_fund = sum(1 for v in fund_db.values() if v.get("fundamental_score",0) != 0)
            log.info(f"[STEP 2b/5 FUND] {ok_fund}/{len(fund_db)} asset con dati fondamentali")
        except Exception as e:
            log.error(f"[STEP 2b/5 FUND] errore: {e}")
    else:
        log.info("[STEP 2b/5 FUND] Disabilitato â€” configura fmp_api_key per attivare")

    # Step 2c: Sector rotation (dati reali ETF settoriali)
    sector_ctx = {}
    if SECTOR_ENABLED:
        try:
            log.info("[STEP 2c/5 SECTOR] Fetch rotazione settoriale (11 ETF vs SPY)...")
            sector_ctx = fetch_sector_rotation()
            state["sector_rotation"] = sector_ctx
            if sector_ctx.get("available"):
                leaders  = sector_ctx.get("leaders", [])
                lagging  = sector_ctx.get("lagging", [])
                regime   = sector_ctx.get("rotation_regime", "?")
                log.info(f"[STEP 2c/5 SECTOR] regime={regime} leaders={leaders} lagging={lagging}")
            else:
                log.warning("[STEP 2c/5 SECTOR] dati non disponibili")
        except Exception as e:
            log.error(f"[STEP 2c/5 SECTOR] errore: {e}")

    # Step 2d: Institutional layer (FMP 13F + insider)
    inst_db = {}
    if INST_ENABLED and FMP_KEY and is_trading_hours():
        try:
            log.info("[STEP 2d/5 INST] Fetch dati istituzionali FMP (13F + insider)...")
            inst_db = fetch_all_institutional(ASSETS, FMP_KEY)
            state["institutional_db"] = inst_db
            ok_inst = sum(1 for v in inst_db.values() if v.get("institutional_score",0) != 0)
            log.info(f"[STEP 2d/5 INST] {ok_inst}/{len(inst_db)} asset con dati istituzionali")
        except Exception as e:
            log.error(f"[STEP 2d/5 INST] errore: {e}")
    elif not FMP_KEY:
        log.info("[STEP 2d/5 INST] Disabilitato â€” configura fmp_api_key")

    # Step 2e: Technical signals (base layer)
    log.info(f"[STEP 2e/5 QUANT] running signal scanner...")
    from signal_engine import run_scanner
    tech_signals_raw = {a["symbol"]: __import__("signal_engine").build_quant_signal(
        tech.get(a["symbol"]), a) for a in ASSETS}

    # Step 2f: Composite scoring multi-layer (6 layer)
    log.info(f"[STEP 2f/5 COMPOSITE] Composite scoring (tech+macro+regime+sector_rt+inst+fund)...")
    signals = run_composite_scanner(
        ASSETS, tech_signals_raw, macro_ctx,
        fund_db if fund_db else None,
        inst_db if inst_db else None,
        sector_ctx if sector_ctx else None,
    )
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
    # â”€â”€ Step 3: AI enrichment â€” SOLO in finestra 08:00-23:30 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if not is_trading_hours():
        log.info("[STEP 3/4 AI] SKIPPED â€” fuori finestra operativa (08:00-23:30) â€” nessun credito AI consumato")
    elif active and (CLAUDE_KEY or PPLX_KEY):
        log.info(f"[STEP 3/4 AI] enriching top-{min(3,len(active))} signals...")
        signals = apply_ai_enrichment(signals, CLAUDE_KEY, PPLX_KEY)
        enriched = sum(1 for s in signals if s.get("ai_enriched"))
        log.info(f"[STEP 3/4 AI] {enriched} signals enriched")
    else:
        log.info("[STEP 3/4 AI] skipped (no active signals or no API keys)")

    run_ts  = datetime.utcnow().isoformat() + "Z"
    next_ts = (datetime.utcnow() + timedelta(minutes=SCHEDULER_MINUTES)).isoformat() + "Z"
    state.update({"signals": signals, "tech_data": tech,
                  "last_run": run_ts, "next_run": next_ts, "running": False})

    # â”€â”€ Step 3.5: Smart Money â€” SOLO in finestra â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if not is_trading_hours():
        log.info("[STEP 3.5/4 SMART_MONEY] SKIPPED â€” fuori finestra operativa")
    elif CLAUDE_KEY:
        try:
            log.info("[STEP 3.5/4 SMART_MONEY] analisi istituzionale...")
            sm = run_smart_money_analysis([a["symbol"] for a in ASSETS], CLAUDE_KEY, PPLX_KEY)
            state["smart_money"] = sm
            log.info(f"[STEP 3.5/4 SMART_MONEY] {len(sm.get('opportunities',[]))} opp | qualita={sm.get('data_quality','?')}")
        except Exception as e:
            log.error(f"[STEP 3.5/4 SMART_MONEY] {e}")

    # Step 4: email
    log.info(f"[STEP 4/4 EMAIL] enabled={OPTIONS.get('email_enabled')}")
    # â”€â”€ Step 4: Email â€” SOLO in finestra â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if not is_trading_hours():
        log.info("[STEP 4/4 EMAIL] SKIPPED â€” fuori finestra operativa (nessuna email notturna)")
    elif OPTIONS.get("email_enabled"):
        try:
            # Adapt signals list for mailer (expects 'results' format)
            ok = send_report(signals, run_ts, next_ts, OPTIONS, state.get('smart_money'))
            state["email_last"] = run_ts; state["email_ok"] = ok
            log.info(f"[STEP 4/4 EMAIL] {'OK' if ok else 'FAILED'}")
        except Exception as e:
            log.error(f"[STEP 4/4 EMAIL] {e}"); state["email_ok"] = False

    log.info(f"[SCHEDULER] DONE | active={len(active)} | "
             f"top={active[0]['symbol'] + ' ' + active[0]['action'] if active else 'none'}")


def _scheduler_loop():
    """
    Scheduler che rispetta la finestra operativa 08:00-23:30.
    Fuori finestra: nessuna scansione, nessuna chiamata AI, nessuna email.
    Controlla ogni minuto se Ã¨ il momento di girare.
    """
    log.info(f"[SCHEDULER] thread avviato | intervallo={SCHEDULER_MINUTES}min | finestra=08:00-23:30")
    last_run_at = None  # timestamp ultimo run completato

    # Prima scansione solo se siamo dentro la finestra
    if is_trading_hours():
        log.info("[SCHEDULER] Prima scansione avviata (siamo in finestra)")
        run_scan()
        last_run_at = datetime.utcnow()
    else:
        log.info("[SCHEDULER] Fuori finestra operativa â€” prima scansione posticipata alle 08:00")

    while True:
        time.sleep(60)  # controlla ogni minuto

        if not SCHEDULER_ENABLED:
            continue

        # Blocco fuori finestra: nessuna operazione
        if not is_trading_hours():
            continue

        # Siamo in finestra: controlla se Ã¨ ora di girare
        now = datetime.utcnow()
        if last_run_at is None or (now - last_run_at).total_seconds() >= SCHEDULER_MINUTES * 60:
            log.info(f"[SCHEDULER] Avvio scansione (ultima: {last_run_at.strftime('%H:%M') if last_run_at else 'mai'})")
            run_scan()
            last_run_at = datetime.utcnow()


# â”€â”€ FastAPI â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

@app.exception_handler(404)
async def not_found_handler(request, exc):
    """Catch-all: qualsiasi path sconosciuto serve index.html (SPA pattern)."""
    # Le API restituiscono 404 JSON normale â€” solo le route non-API servono il frontend
    if request.url.path.startswith("/api/"):
        from fastapi.responses import JSONResponse
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    return await _html()

@app.get("/health")
async def health():
    return {"status":"ok","version":"2.0.0",
            "assets":len(ASSETS),"anthropic":bool(CLAUDE_KEY),"perplexity":bool(PPLX_KEY),
            "trading_window":is_trading_hours(),"bind":BIND_HOST,
            "email_enabled":bool(OPTIONS.get("email_enabled"))}

@app.get("/api/config")
async def config():
    return {"scheduler_minutes":SCHEDULER_MINUTES,"scheduler_enabled":SCHEDULER_ENABLED,
            "has_anthropic":bool(CLAUDE_KEY),"has_perplexity":bool(PPLX_KEY),"has_fmp":bool(FMP_KEY),"has_eia":bool(EIA_KEY),"has_fred":bool(FRED_KEY),
            "has_fred":bool(FRED_KEY),"has_fmp":bool(FMP_KEY),"has_eia":bool(EIA_KEY),
            "macro_enabled":MACRO_ENABLED,"fundamental_enabled":FUND_ENABLED,
            "email_enabled":bool(OPTIONS.get("email_enabled")),
            "email_to":OPTIONS.get("email_to",""),
            "smtp_host":OPTIONS.get("smtp_host",""),
            "smtp_port":OPTIONS.get("smtp_port",587),
            "email_min_score":OPTIONS.get("email_min_score",40),
            "trading_window":is_trading_hours()}

@app.get("/api/assets")
async def get_assets():
    """Restituisce tutti gli asset (inclusi disabled) per la gestione watchlist."""
    all_assets = _load_all_assets()
    return {"count": len(all_assets), "assets": all_assets,
            "active": sum(1 for a in all_assets if a.get("enabled",True))}

@app.post("/api/assets")
async def add_asset(asset: AssetModel):
    """Aggiunge un nuovo asset alla watchlist."""
    global ASSETS
    all_assets = _load_all_assets()
    sym = asset.symbol.strip().upper()
    # Verifica duplicati
    if any(a["symbol"].upper() == sym for a in all_assets):
        raise HTTPException(400, f"Simbolo {sym} giÃ  presente in watchlist")
    new_asset = {**asset.model_dump(), "symbol": sym}
    if not new_asset.get("full_name"):
        new_asset["full_name"] = new_asset["name"]
    all_assets.append(new_asset)
    _save_assets(all_assets)
    ASSETS = [a for a in all_assets if a.get("enabled", True)]
    log.info(f"[ASSETS] Aggiunto: {sym} ({new_asset["name"]})")
    return {"status": "added", "asset": new_asset, "total": len(all_assets)}

@app.put("/api/assets/{symbol}")
async def update_asset(symbol: str, asset: AssetModel):
    """Modifica un asset esistente (identificato dal simbolo nell'URL)."""
    global ASSETS
    all_assets = _load_all_assets()
    sym = symbol.strip().upper()
    idx = next((i for i,a in enumerate(all_assets) if a["symbol"].upper()==sym), None)
    if idx is None:
        raise HTTPException(404, f"Simbolo {sym} non trovato")
    updated = {**asset.model_dump(), "symbol": sym}
    if not updated.get("full_name"):
        updated["full_name"] = updated["name"]
    all_assets[idx] = updated
    _save_assets(all_assets)
    ASSETS = [a for a in all_assets if a.get("enabled", True)]
    log.info(f"[ASSETS] Modificato: {sym}")
    return {"status": "updated", "asset": updated}

@app.patch("/api/assets/{symbol}/toggle")
async def toggle_asset(symbol: str):
    """Abilita/disabilita un asset senza eliminarlo."""
    global ASSETS
    all_assets = _load_all_assets()
    sym = symbol.strip().upper()
    idx = next((i for i,a in enumerate(all_assets) if a["symbol"].upper()==sym), None)
    if idx is None:
        raise HTTPException(404, f"Simbolo {sym} non trovato")
    all_assets[idx]["enabled"] = not all_assets[idx].get("enabled", True)
    status = "enabled" if all_assets[idx]["enabled"] else "disabled"
    _save_assets(all_assets)
    ASSETS = [a for a in all_assets if a.get("enabled", True)]
    log.info(f"[ASSETS] Toggle {sym}: {status}")
    return {"status": status, "symbol": sym, "enabled": all_assets[idx]["enabled"]}

@app.delete("/api/assets/{symbol}")
async def delete_asset(symbol: str):
    """Elimina definitivamente un asset dalla watchlist."""
    global ASSETS
    all_assets = _load_all_assets()
    sym = symbol.strip().upper()
    before = len(all_assets)
    all_assets = [a for a in all_assets if a["symbol"].upper() != sym]
    if len(all_assets) == before:
        raise HTTPException(404, f"Simbolo {sym} non trovato")
    _save_assets(all_assets)
    ASSETS = [a for a in all_assets if a.get("enabled", True)]
    log.info(f"[ASSETS] Eliminato: {sym}")
    return {"status": "deleted", "symbol": sym, "remaining": len(all_assets)}

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

@app.get("/api/sector-rotation")
async def get_sector_rotation():
    """Restituisce la rotazione settoriale corrente con ranking e classificazione."""
    if state.get("sector_rotation"):
        return state["sector_rotation"]
    # Fetch sincrono se non ancora disponibile
    try:
        from sector_rotation_layer import fetch_sector_rotation
        result = fetch_sector_rotation()
        state["sector_rotation"] = result
        return result
    except Exception as e:
        return {"available": False, "error": str(e)}

@app.get("/api/institutional/{symbol}")
async def get_institutional(symbol: str):
    """Restituisce i dati istituzionali per un singolo asset."""
    sym = symbol.upper()
    inst_db = state.get("institutional_db", {})
    if sym in inst_db:
        return inst_db[sym]
    if not FMP_KEY:
        raise HTTPException(400, "FMP API key non configurata â€” configura fmp_api_key nel config add-on")
    # Fetch puntuale
    asset = next((a for a in ASSETS if a["symbol"].upper() == sym), {"symbol": sym, "asset_type": "stock"})
    from institutional_layer import fetch_institutional_score
    result = fetch_institutional_score(sym, asset.get("asset_type","stock"), FMP_KEY)
    return result

@app.get("/api/smart-money")
async def get_smart_money():
    """Restituisce l'ultima analisi istituzionale. Usa cache 6h."""
    if state.get("smart_money"):
        return state["smart_money"]
    return {"error": "Analisi non ancora eseguita", "opportunities": []}

@app.post("/api/smart-money/refresh")
async def refresh_smart_money(background_tasks: BackgroundTasks):
    """Forza aggiornamento analisi istituzionale."""
    symbols = [a["symbol"] for a in ASSETS]
    def _run():
        result = run_smart_money_analysis(symbols, CLAUDE_KEY, PPLX_KEY, force_refresh=True)
        state["smart_money"] = result
        log.info("[SMART_MONEY] Analisi aggiornata via API")
    background_tasks.add_task(_run)
    return {"status": "started"}

@app.get("/api/macro")
async def get_macro():
    """Restituisce l'ultimo contesto macro (FRED, ECB, EIA, Yahoo)."""
    m = state.get("macro_context")
    if m: return m
    raise HTTPException(404, "Macro context non ancora disponibile")

@app.get("/api/fundamentals/{symbol}")
async def get_fundamentals(symbol: str):
    """Restituisce dati fondamentali FMP per un simbolo."""
    sym = symbol.upper()
    fd  = state.get("fund_data", {}).get(sym)
    if fd: return fd
    raise HTTPException(404, f"Fondamentali non disponibili per {sym}")

@app.get("/api/chart/{symbol}")
async def get_chart(symbol: str, days: int = 60):
    """Restituisce storico prezzi per grafici frontend."""
    sym = symbol.upper()
    ind = state.get("tech_data", {}).get(sym)
    if ind is None:
        raise HTTPException(404, f"Nessun dato per {sym}")
    # Recupera dati OHLCV freschi (giÃ  cached da yfinance)
    try:
        import yfinance as yf
        df = yf.Ticker(sym).history(period="3mo", interval="1d", auto_adjust=True)
        if df is None or len(df) == 0:
            raise HTTPException(404, "Dati non disponibili")
        df = df.tail(days)
        closes = [round(float(v),4) for v in df["Close"].tolist()]
        volumes= [int(v) for v in df["Volume"].tolist()]
        dates  = [str(d.date()) for d in df.index.tolist()]
        # Media mobile 20gg
        import pandas as pd
        ma20 = df["Close"].rolling(20).mean().tail(days)
        ma20_vals = [round(float(v),4) if not pd.isna(v) else None for v in ma20.tolist()]
        return {
            "symbol": sym,
            "dates":  dates,
            "closes": closes,
            "volumes":volumes,
            "ma20":   ma20_vals,
            "last":   closes[-1] if closes else None,
            "min":    min(closes) if closes else None,
            "max":    max(closes) if closes else None,
        }
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(500, str(e))

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