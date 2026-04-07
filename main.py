# main.py v2.1 - Multi-Market Scanner + MariaDB
import os, json, logging, threading, time
from pathlib    import Path
from datetime   import datetime, timedelta
from fastapi    import FastAPI, HTTPException, BackgroundTasks, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses       import HTMLResponse
from pydantic   import BaseModel
from typing     import Optional, List
import uvicorn
import requests

from market_data            import fetch_all, load_assets, lookup_isin, _save_assets, assets_json_path
from smart_money            import run_smart_money_analysis, build_email_section, validate_signals_with_perplexity
from macro_layer            import fetch_macro_context
from fundamental_layer      import fetch_all_fundamentals
from scoring_engine         import run_composite_scanner, enrich_with_smart_money
from signal_engine          import run_scanner, build_quant_signal, is_trading_hours
from ai_validation          import apply_ai_enrichment
from backtest_engine        import backtest_symbol, backtest_batch, BacktestConfig
from mailer                 import send_report, send_wallet_alert, send_crypto_alert
from sector_rotation_layer  import fetch_sector_rotation, get_sector_score
from institutional_layer    import fetch_institutional_score, fetch_all_institutional
import db

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("main")


# â"€â"€ Config â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
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
        "wallet_scheduler_minutes":   int(os.getenv("WALLET_SCHEDULER_MINUTES","15")),
        "income_scheduler_minutes":   int(os.getenv("INCOME_SCHEDULER_MINUTES","30")),
        "scheduler_enabled":          True,
        "email_enabled":              False,
        "wallet_email_alerts_enabled": True,
        "email_to":   os.getenv("EMAIL_TO",""),    "email_from": os.getenv("EMAIL_FROM",""),
        "smtp_host":  os.getenv("SMTP_HOST","smtp.gmail.com"),
        "smtp_port":  int(os.getenv("SMTP_PORT","587")),
        "smtp_user":  os.getenv("SMTP_USER",""),   "smtp_password": os.getenv("SMTP_PASSWORD",""),
        "smtp_tls":   True,                        "email_min_score": 40,
    }

OPTIONS = load_options()
if OPTIONS.get("anthropic_api_key"):  os.environ["ANTHROPIC_API_KEY"]  = OPTIONS["anthropic_api_key"]
if OPTIONS.get("perplexity_api_key"): os.environ["PERPLEXITY_API_KEY"] = OPTIONS["perplexity_api_key"]

# ── MariaDB init ──────────────────────────────────────────────────────────────
DB_ENABLED = db.init_db(
    host     = OPTIONS.get("db_host", ""),
    port     = int(OPTIONS.get("db_port", 3306)),
    user     = OPTIONS.get("db_user", ""),
    password = OPTIONS.get("db_password", ""),
    database = OPTIONS.get("db_name", "market_analyze"),
)

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
WALLET_SCHEDULER_MINUTES = int(OPTIONS.get("wallet_scheduler_minutes",15))
INCOME_SCHEDULER_MINUTES = int(OPTIONS.get("income_scheduler_minutes",30))
SCHEDULER_ENABLED = bool(OPTIONS.get("scheduler_enabled",True))
WALLET_ALERTS_ENABLED = bool(OPTIONS.get("wallet_email_alerts_enabled", OPTIONS.get("email_enabled", False)))
BIND_HOST         = os.getenv("BIND_HOST","0.0.0.0")
PORT              = int(os.getenv("INGRESS_PORT","8099"))

ASSETS = load_assets()
CRYPTO_ASSETS_PATH = Path(__file__).parent / "crypto_assets.json"
CRYPTO_SCHEDULER_MINUTES = 10
CRYPTO_LIVE_REFRESH_SECONDS = 10
COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"


def load_crypto_assets() -> list:
    try:
        if CRYPTO_ASSETS_PATH.exists():
            return [a for a in json.load(open(CRYPTO_ASSETS_PATH, encoding="utf-8")) if a.get("enabled", True)]
    except Exception as e:
        log.error(f"[CRYPTO] load_crypto_assets: {e}")
    return []


CRYPTO_ASSETS = load_crypto_assets()
log.info(f"[STARTUP] {len(ASSETS)} assets loaded")

# Migra asset da JSON a DB alla prima avvio (solo se tabella vuota)
if DB_ENABLED:
    _json_path = assets_json_path()
    _json_assets = json.load(open(_json_path, encoding="utf-8")) if _json_path.exists() else []
    db.migrate_assets_from_json(_json_assets)


def _load_all_assets() -> list:
    """Carica TUTTI gli asset (inclusi disabled). DB se disponibile, altrimenti JSON."""
    if db.is_enabled():
        return db.load_assets_from_db()
    p = assets_json_path()
    if p.exists():
        return json.load(open(p, encoding="utf-8"))
    return []


def _load_wallet_holdings() -> list:
    if db.is_enabled():
        return db.load_wallet_holdings()
    return []


def _load_wallet_history() -> list:
    if db.is_enabled():
        return [h for h in db.load_wallet_holdings(include_closed=True) if (h.get("position_status") or "ACTIVE") != "ACTIVE"]
    return []


def _find_asset_metadata(symbol: str) -> dict:
    sym = symbol.strip().upper()
    for asset in _load_all_assets():
        if asset.get("symbol", "").upper() == sym:
            return asset
    return {"symbol": sym, "name": sym, "full_name": sym}


def _normalize_wallet_holding(data: dict) -> dict:
    base = _find_asset_metadata(data.get("symbol", ""))
    merged = {**base, **data}
    merged["symbol"] = merged.get("symbol", "").strip().upper()
    merged["name"] = merged.get("name") or merged["symbol"]
    merged["full_name"] = merged.get("full_name") or merged["name"]
    merged["quantity"] = float(merged.get("quantity") or 0)
    merged["avg_price"] = float(merged.get("avg_price") or 0)
    merged["horizon_days"] = max(1, int(merged.get("horizon_days") or 30))
    return merged


_fx_cache = {"rates": {"EUR": 1.0}, "updated_at": 0.0}


def _get_fx_rate_to_eur(currency: str) -> float:
    cur = (currency or "EUR").upper()
    if cur == "EUR":
        return 1.0
    now = time.time()
    cached = _fx_cache["rates"].get(cur)
    if cached and (_fx_cache["updated_at"] + 3600) > now:
        return cached
    try:
        resp = requests.get(f"https://api.frankfurter.app/latest?from={cur}&to=EUR", timeout=8)
        resp.raise_for_status()
        data = resp.json()
        rate = float((data.get("rates") or {}).get("EUR") or 0)
        if rate > 0:
            _fx_cache["rates"][cur] = rate
            _fx_cache["updated_at"] = now
            return rate
    except Exception as e:
        log.warning(f"[FX] cambio {cur}->EUR non disponibile: {e}")
    return cached or 1.0


def _to_eur(value, currency: str):
    try:
        if value in (None, "", "N/A"):
            return None
        return round(float(value) * _get_fx_rate_to_eur(currency), 4)
    except Exception:
        return None


def _signal_to_eur(signal: dict) -> dict:
    s = dict(signal)
    if s.get("display_currency") == "EUR":
        return s
    cur = (s.get("currency") or "EUR").upper()
    s["original_currency"] = cur
    s["fx_rate_to_eur"] = _get_fx_rate_to_eur(cur)
    for key in ("price", "entry", "stop_loss", "take_profit"):
        if s.get(key) not in (None, "", "N/A"):
            s[f"original_{key}"] = s.get(key)
            s[key] = _to_eur(s.get(key), cur)
    indicators = dict(s.get("indicators") or {})
    for key in ("support", "resistance", "ma20", "ma50", "ma200"):
        if indicators.get(key) not in (None, "", "N/A"):
            indicators[f"original_{key}"] = indicators.get(key)
            indicators[key] = _to_eur(indicators.get(key), cur)
    s["indicators"] = indicators
    s["currency"] = "EUR"
    s["display_currency"] = "EUR"
    return s


def _upsert_wallet_from_asset(asset: dict, quantity: Optional[float] = None, avg_price: Optional[float] = None,
                              target_price: Optional[float] = None, stop_loss: Optional[float] = None,
                              horizon_days: int = 30) -> None:
    if not db.is_enabled():
        return
    existing = next((h for h in _load_wallet_holdings() if h.get("symbol", "").upper() == asset["symbol"].upper()), {})
    holding = {
        **existing,
        "symbol": asset["symbol"],
        "name": asset.get("name", asset["symbol"]),
        "full_name": asset.get("full_name", asset.get("name", asset["symbol"])),
        "isin": asset.get("isin", ""),
        "market": asset.get("market", "US"),
        "country": asset.get("country", asset.get("market", "US")),
        "asset_type": asset.get("asset_type", "stock"),
        "currency": asset.get("currency", "USD"),
        "exchange": asset.get("exchange", ""),
        "quantity": quantity if quantity is not None else existing.get("quantity", 0),
        "avg_price": avg_price if avg_price is not None else existing.get("avg_price", 0),
        "target_price": target_price if target_price is not None else existing.get("target_price"),
        "stop_loss": stop_loss if stop_loss is not None else existing.get("stop_loss"),
        "horizon_days": horizon_days or existing.get("horizon_days", 30),
        "alert_enabled": existing.get("alert_enabled", True),
        "enabled": True,
        "note": existing.get("note", ""),
    }
    db.save_wallet_holding(_normalize_wallet_holding(holding))


def _recommendation_from_signal(signal: dict, holding: dict, current_price: Optional[float]) -> dict:
    action = (signal or {}).get("action", "HOLD")
    confidence = int((signal or {}).get("confidence") or 0)
    quantity = float(holding.get("quantity") or 0)
    avg_price = float(holding.get("avg_price") or 0)
    if quantity <= 0 or avg_price <= 0:
        return {
            "signal_action": action,
            "recommendation": "SETUP_REQUIRED",
            "confidence": confidence,
            "holding_days_estimate": int(holding.get("horizon_days") or 30),
            "alert_type": None,
            "is_relevant": False,
            "rationale": [
                "L'asset e' gia' collegato al wallet ma mancano quantita' e/o prezzo medio di carico.",
                "Completa i dati della posizione per ottenere stima puntuale, P/L e alert operativi.",
            ],
            "pnl_pct": 0.0,
        }
    pnl_pct = 0.0
    if current_price and avg_price:
        pnl_pct = round(((current_price - avg_price) / avg_price) * 100, 2)

    recommendation = "HOLD"
    alert_type = None
    relevant = False
    if action == "SELL":
        recommendation = "SELL"
        alert_type = "SELL_SIGNAL"
        relevant = True
    elif action == "BUY":
        recommendation = "ACCUMULATE" if quantity > 0 else "BUY"
        alert_type = "BUY_SIGNAL"
        relevant = True
    elif action == "WATCHLIST":
        recommendation = "HOLD_WATCH"
        if confidence >= 75:
            alert_type = "WATCH_ESCALATION"
            relevant = True

    if holding.get("stop_loss") and current_price and current_price <= float(holding["stop_loss"]):
        recommendation = "RISK_EXIT"
        alert_type = "STOP_LOSS"
        relevant = True

    target_price = holding.get("target_price")
    if target_price and current_price and current_price >= float(target_price):
        recommendation = "TAKE_PROFIT"
        alert_type = "TARGET_REACHED"
        relevant = True

    if pnl_pct <= -8 and recommendation not in ("SELL", "RISK_EXIT"):
        recommendation = "REDUCE"
        alert_type = alert_type or "DRAWDOWN"
        relevant = True

    hold_days = holding.get("horizon_days") or 30
    if recommendation in ("SELL", "RISK_EXIT", "TAKE_PROFIT", "REDUCE"):
        hold_days = min(hold_days, 3)
    elif recommendation in ("BUY", "ACCUMULATE"):
        hold_days = max(10, hold_days)
    elif recommendation == "HOLD_WATCH":
        hold_days = min(max(5, hold_days // 2), 15)

    rationale = []
    if action == "BUY":
        rationale.append("Il motore segnali vede forza rialzista sul titolo.")
    elif action == "SELL":
        rationale.append("Il motore segnali rileva deterioramento tecnico o rischio di inversione.")
    elif action == "WATCHLIST":
        rationale.append("Il titolo va monitorato per conferme prima di aumentare o alleggerire la posizione.")
    else:
        rationale.append("Non ci sono segnali operativi forti, quindi la posizione resta in osservazione.")
    if pnl_pct:
        rationale.append(f"Performance non realizzata: {pnl_pct:+.2f}% rispetto al prezzo medio.")
    if holding.get("target_price"):
        rationale.append(f"Target impostato a {holding['target_price']}.")
    if holding.get("stop_loss"):
        rationale.append(f"Stop loss impostato a {holding['stop_loss']}.")

    return {
        "signal_action": action,
        "recommendation": recommendation,
        "confidence": confidence,
        "holding_days_estimate": int(hold_days),
        "alert_type": alert_type,
        "is_relevant": relevant,
        "rationale": rationale,
        "pnl_pct": pnl_pct,
    }


def run_wallet_review(force_email: bool = False) -> dict:
    if state["wallet_running"]:
        return state.get("wallet") or {"summary": {}, "holdings": [], "alerts": []}

    holdings = [h for h in _load_wallet_holdings() if h.get("enabled", True)]
    if not holdings:
        result = {"summary": {"holdings_count": 0}, "holdings": [], "alerts": [], "history": []}
        now = datetime.utcnow()
        state.update({
            "wallet": result,
            "wallet_last_run": now.isoformat() + "Z",
            "wallet_next_run": (now + timedelta(minutes=WALLET_SCHEDULER_MINUTES)).isoformat() + "Z",
        })
        return result

    state["wallet_running"] = True
    try:
        symbols = [h["symbol"] for h in holdings]
        tech = fetch_all(symbols)
        signal_index = {s["symbol"]: s for s in state.get("signals", [])}
        analyzed = []
        alerts = []
        invested_total = market_value_total = pnl_total = 0.0

        for holding in holdings:
            sym = holding["symbol"]
            signal = signal_index.get(sym)
            if signal is None:
                signal = build_quant_signal(tech.get(sym), holding)
                signal["symbol"] = sym
                signal["name"] = holding.get("name", sym)
                signal = _signal_to_eur(signal)
            current_price = (signal or {}).get("price")
            quantity = float(holding.get("quantity") or 0)
            avg_price = float(holding.get("avg_price") or 0)
            invested_amount = round(quantity * avg_price, 2)
            market_value = round(quantity * float(current_price or 0), 2)
            pnl_amount = round(market_value - invested_amount, 2)

            rec = _recommendation_from_signal(signal, holding, current_price)
            holding_view = {
                **holding,
                "current_price": current_price,
                "invested_amount": invested_amount,
                "market_value": market_value,
                "pnl_amount": pnl_amount,
                "pnl_pct": rec["pnl_pct"],
                "signal_action": rec["signal_action"],
                "recommendation": rec["recommendation"],
                "confidence": rec["confidence"],
                "holding_days_estimate": rec["holding_days_estimate"],
                "reasons": rec["rationale"],
                "signal": signal,
            }
            analyzed.append(holding_view)
            invested_total += invested_amount
            market_value_total += market_value
            pnl_total += pnl_amount

            if rec["is_relevant"] and holding.get("alert_enabled", True):
                prior = db.get_recent_wallet_alert(sym, rec["alert_type"], within_hours=12) if db.is_enabled() and rec["alert_type"] else None
                should_alert = force_email or not prior or prior.get("recommendation") != rec["recommendation"]
                if should_alert:
                    alert_payload = {
                        "symbol": sym,
                        "name": holding.get("name", sym),
                        "recommendation": rec["recommendation"],
                        "signal_action": rec["signal_action"],
                        "confidence": rec["confidence"],
                        "current_price": current_price,
                        "pnl_pct": rec["pnl_pct"],
                        "holding_days_estimate": rec["holding_days_estimate"],
                        "reasons": rec["rationale"],
                    }
                    alerts.append(alert_payload)
                    if db.is_enabled() and rec["alert_type"]:
                        db.save_wallet_alert(sym, rec["alert_type"], rec["recommendation"], rec["confidence"], alert_payload)

        pnl_pct_total = round((pnl_total / invested_total) * 100, 2) if invested_total else 0.0
        analyzed.sort(key=lambda item: (item["recommendation"] in ("SELL", "RISK_EXIT", "TAKE_PROFIT", "REDUCE"), item["confidence"]), reverse=True)
        now = datetime.utcnow()
        result = {
            "summary": {
                "holdings_count": len(analyzed),
                "invested_total": round(invested_total, 2),
                "market_value_total": round(market_value_total, 2),
                "pnl_total": round(pnl_total, 2),
                "pnl_pct_total": pnl_pct_total,
                "alert_count": len(alerts),
            },
            "holdings": analyzed,
            "alerts": alerts,
            "run_at": now.isoformat() + "Z",
            "next_run": (now + timedelta(minutes=WALLET_SCHEDULER_MINUTES)).isoformat() + "Z",
        }

        if db.is_enabled():
            try:
                db.save_wallet_analysis_run(result)
            except Exception as e:
                log.error(f"[DB] save_wallet_analysis_run: {e}")

        if alerts and OPTIONS.get("email_enabled") and WALLET_ALERTS_ENABLED:
            try:
                send_wallet_alert(result, OPTIONS)
            except Exception as e:
                log.error(f"[WALLET] send_wallet_alert: {e}")

        state.update({
            "wallet": result,
            "wallet_last_run": result["run_at"],
            "wallet_next_run": result["next_run"],
        })
        return result
    finally:
        state["wallet_running"] = False


def _income_weighted_yield(role: str, asset_type: str, recommendation: str) -> float:
    if asset_type == "etf":
        base = {"income": 0.034, "growth": 0.011, "opportunistico": 0.006}.get(role, 0.02)
    else:
        base = {"income": 0.041, "growth": 0.008, "opportunistico": 0.004}.get(role, 0.015)
    if recommendation in ("SELL", "REDUCE", "RISK_EXIT"):
        base *= 0.4
    return round(base, 4)


def _market_income_profile() -> dict:
    macro = (state.get("macro_context") or {})
    regime = macro.get("regime", "NEUTRAL")
    rotation = (state.get("sector_rotation") or {}).get("rotation_regime", "MIXED")
    if regime == "BULLISH" and rotation == "RISK_ON":
        return {"scenario": "Mercato rialzista con leadership ciclica", "risk": "Medio", "income": 0.40, "growth": 0.40, "opportunistic": 0.20}
    if regime == "BEARISH" or rotation == "RISK_OFF":
        return {"scenario": "Mercato difensivo o ribassista", "risk": "Medio-Alto", "income": 0.65, "growth": 0.20, "opportunistic": 0.15}
    if regime == "CAUTIOUS":
        return {"scenario": "Mercato incerto con volatilita' elevata", "risk": "Medio", "income": 0.55, "growth": 0.25, "opportunistic": 0.20}
    return {"scenario": "Mercato bilanciato senza trend dominante", "risk": "Medio", "income": 0.50, "growth": 0.30, "opportunistic": 0.20}


def run_income_plan() -> dict:
    signals = state.get("signals", []) or []
    wallet = state.get("wallet") or run_wallet_review()
    profile = _market_income_profile()
    holdings = wallet.get("holdings", []) or []
    invested_capital = float(wallet.get("summary", {}).get("market_value_total", 0) or 0)
    capital_tier = "Capitale basso" if invested_capital < 25000 else "Capitale medio" if invested_capital < 100000 else "Capitale alto"

    sorted_signals = sorted(signals, key=lambda s: (s.get("action") == "BUY", s.get("confidence", 0), s.get("score", 0)), reverse=True)
    etfs = [s for s in sorted_signals if s.get("asset_type") == "etf"]
    stocks = [s for s in sorted_signals if s.get("asset_type") == "stock"]

    instruments = []
    seen = set()

    def pick(items, role: str, limit: int, max_weight: float):
        for sig in items:
            sym = sig.get("symbol")
            if not sym or sym in seen:
                continue
            recommendation = "comprare" if sig.get("action") == "BUY" else "mantenere" if sig.get("action") in ("WATCHLIST", "HOLD") else "ridurre"
            item = {
                "symbol": sym,
                "name": sig.get("name") or sig.get("full_name") or sym,
                "asset_type": sig.get("asset_type", "stock"),
                "role": role,
                "recommendation": recommendation,
                "max_weight": max_weight,
                "confidence": int(sig.get("confidence") or 0),
                "reason": "; ".join((sig.get("reasons") or [])[:2]) or "Selezione coerente con il regime di mercato attuale.",
                "estimated_annual_yield": _income_weighted_yield(role, sig.get("asset_type", "stock"), recommendation.upper()),
            }
            instruments.append(item)
            seen.add(sym)
            if sum(1 for x in instruments if x["role"] == role) >= limit:
                break

    pick(etfs, "income", 3, 0.20)
    pick(stocks, "income", 2, 0.10)
    pick(etfs, "growth", 2, 0.15)
    pick(stocks, "growth", 3, 0.10)
    pick(stocks, "opportunistico", 2, 0.08)

    if not instruments:
        instruments = [{
            "symbol": "N/D",
            "name": "Nessuno strumento disponibile",
            "asset_type": "mixed",
            "role": "income",
            "recommendation": "mantenere",
            "max_weight": 0.0,
            "confidence": 0,
            "reason": "Servono piu' ETF e azioni analizzati nella watchlist per costruire il piano rendita.",
            "estimated_annual_yield": 0.0,
        }]

    annual_yield = 0.0
    weights = {"income": profile["income"], "growth": profile["growth"], "opportunistico": profile["opportunistic"]}
    grouped = {"income": [], "growth": [], "opportunistico": []}
    for item in instruments:
        grouped[item["role"]].append(item)
    for role, items in grouped.items():
        if not items:
            continue
        role_yield = sum(x["estimated_annual_yield"] for x in items) / len(items)
        annual_yield += role_yield * weights[role]

    annual_yield = round(max(annual_yield * 0.9, 0.01), 4)
    estimated_monthly_income = round((invested_capital * annual_yield) / 12, 2)
    monthly_goal = 1000.0
    monthly_gap = round(max(monthly_goal - estimated_monthly_income, 0), 2)
    target_capital = round(12000 / annual_yield, 2) if annual_yield > 0 else 0.0
    gap_to_target = round(max(target_capital - invested_capital, 0), 2)

    etf_value = sum(float(h.get("market_value") or 0) for h in holdings if h.get("asset_type") == "etf")
    stock_value = sum(float(h.get("market_value") or 0) for h in holdings if h.get("asset_type") == "stock")
    total_value = etf_value + stock_value
    current_mix = {
        "etf_pct": round((etf_value / total_value) * 100, 1) if total_value else round(profile["income"] * 100, 1),
        "stock_pct": round((stock_value / total_value) * 100, 1) if total_value else round((1 - profile["income"]) * 100, 1),
    }

    actions = []
    for item in instruments[:8]:
        actions.append({
            "symbol": item["symbol"],
            "action": item["recommendation"],
            "role": item["role"],
            "note": item["reason"],
        })

    buy_candidates = [x for x in instruments if x["recommendation"] in ("comprare", "mantenere")]
    buy_plan = []
    if monthly_gap > 0 and buy_candidates:
        plan_capital = min(gap_to_target if gap_to_target > 0 else target_capital, max(monthly_gap * 12 / annual_yield if annual_yield else 0, 0))
        plan_capital = round(plan_capital if plan_capital > 0 else gap_to_target, 2)
        weighted = []
        for item in buy_candidates[:5]:
            role_mul = {"income": 1.0, "growth": 0.75, "opportunistico": 0.45}.get(item["role"], 0.6)
            weighted.append((item, max(item["estimated_annual_yield"] * role_mul, 0.001)))
        total_weight = sum(w for _, w in weighted) or 1
        for item, weight in weighted:
            alloc_eur = round(plan_capital * weight / total_weight, 2)
            monthly_contribution = round((alloc_eur * item["estimated_annual_yield"]) / 12, 2)
            buy_plan.append({
                "symbol": item["symbol"],
                "role": item["role"],
                "buy_amount_eur": alloc_eur,
                "estimated_monthly_income_add": monthly_contribution,
                "reason": item["reason"],
            })

    plan = {
        "market_scenario": profile["scenario"],
        "risk_level": profile["risk"],
        "capital_profile": capital_tier,
        "allocation": {
            "etf_vs_stocks": current_mix,
            "income_pct": round(profile["income"] * 100, 1),
            "growth_pct": round(profile["growth"] * 100, 1),
            "opportunistic_pct": round(profile["opportunistic"] * 100, 1),
        },
        "summary": {
            "estimated_monthly_income": estimated_monthly_income,
            "monthly_goal": monthly_goal,
            "monthly_gap": monthly_gap,
            "portfolio_avg_yield_pct": round(annual_yield * 100, 2),
            "target_capital_for_goal": target_capital,
            "gap_to_target": gap_to_target,
            "current_capital": round(invested_capital, 2),
        },
        "instruments": instruments,
        "actions": actions,
        "buy_plan": buy_plan,
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "next_run": (datetime.utcnow() + timedelta(minutes=INCOME_SCHEDULER_MINUTES)).isoformat() + "Z",
    }
    state["income_plan"] = plan
    state["income_last_run"] = plan["updated_at"]
    state["income_next_run"] = plan["next_run"]
    if db.is_enabled():
        try:
            db.save_income_plan(plan)
        except Exception as e:
            log.error(f"[DB] save_income_plan: {e}")
    return plan


def run_crypto_scan() -> dict:
    if state["crypto_running"]:
        return {"signals": state.get("crypto_signals", [])}
    state["crypto_running"] = True
    try:
        assets = CRYPTO_ASSETS
        symbols = [a["symbol"] for a in assets]
        tech = fetch_all(symbols)
        signals = []
        for asset in assets:
            sig = build_quant_signal(tech.get(asset["symbol"]), asset)
            sig["trading_window"] = True
            sig["scan_type"] = "crypto"
            signals.append(_signal_to_eur(sig))
        signals.sort(key=lambda s: (s.get("action") in ("BUY", "SELL", "WATCHLIST"), s.get("confidence", 0), s.get("score", 0)), reverse=True)
        now = datetime.utcnow()
        current_snapshot = {s["symbol"]: s.get("action") for s in signals if s.get("action") in ("BUY", "SELL")}
        previous_snapshot = state.get("crypto_alert_snapshot", {}) or {}
        changed_alerts = [s for s in signals if s.get("action") in ("BUY", "SELL") and previous_snapshot.get(s["symbol"]) != s.get("action")]
        state["crypto_signals"] = signals
        state["crypto_last_run"] = now.isoformat() + "Z"
        state["crypto_next_run"] = (now + timedelta(minutes=CRYPTO_SCHEDULER_MINUTES)).isoformat() + "Z"
        state["crypto_alert_snapshot"] = current_snapshot
        if changed_alerts and OPTIONS.get("email_enabled"):
            try:
                ok = send_crypto_alert(changed_alerts, OPTIONS)
                state["crypto_email_last"] = now.isoformat() + "Z" if ok else state.get("crypto_email_last")
            except Exception as e:
                log.error(f"[CRYPTO] send_crypto_alert: {e}")
        return {
            "signals": signals,
            "last_run": state["crypto_last_run"],
            "next_run": state["crypto_next_run"],
            "count": len(signals),
            "email_last": state.get("crypto_email_last"),
        }
    finally:
        state["crypto_running"] = False


def fetch_crypto_live_prices() -> dict:
    assets = CRYPTO_ASSETS
    ids = [a.get("coingecko_id") for a in assets if a.get("coingecko_id")]
    if not ids:
        return {"prices": {}, "updated_at": None}
    prices = {}
    try:
        r = requests.get(
            COINGECKO_SIMPLE_PRICE_URL,
            params={
                "ids": ",".join(ids),
                "vs_currencies": "eur",
                "include_24hr_change": "true",
                "include_last_updated_at": "true",
            },
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        for asset in assets:
            coin_id = asset.get("coingecko_id")
            if not coin_id or coin_id not in data:
                continue
            item = data.get(coin_id, {})
            prices[asset["symbol"]] = {
                "price": item.get("eur"),
                "change_24h_pct": item.get("eur_24h_change"),
                "updated_at": item.get("last_updated_at"),
                "name": asset.get("name", asset["symbol"]),
            }
        state["crypto_live"] = prices
        state["crypto_live_last"] = datetime.utcnow().isoformat() + "Z"
        return {"prices": prices, "updated_at": state["crypto_live_last"], "source": "coingecko_simple_price"}
    except Exception as e:
        log.error(f"[CRYPTO] fetch_crypto_live_prices: {e}")
        return {"prices": state.get("crypto_live", {}), "updated_at": state.get("crypto_live_last"), "source": "cache"}

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
    add_to_wallet: bool = False
    wallet_quantity: Optional[float] = None
    wallet_avg_price: Optional[float] = None
    wallet_target_price: Optional[float] = None
    wallet_stop_loss: Optional[float] = None
    wallet_horizon_days: int = 30


class WalletHoldingModel(BaseModel):
    symbol:        str
    quantity:      float
    avg_price:     float
    name:          str = ""
    full_name:     str = ""
    isin:          str = ""
    market:        str = "US"
    country:       str = "US"
    asset_type:    str = "stock"
    currency:      str = "USD"
    exchange:      str = ""
    target_price:  Optional[float] = None
    stop_loss:     Optional[float] = None
    horizon_days:  int = 30
    alert_enabled: bool = True
    enabled:       bool = True
    note:          str = ""

# â"€â"€ State â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
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
    "wallet":         None,
    "wallet_last_run": None,
    "wallet_next_run": None,
    "wallet_running": False,
    "income_plan": None,
    "income_last_run": None,
    "income_next_run": None,
    "crypto_signals": [],
    "crypto_last_run": None,
    "crypto_next_run": None,
    "crypto_running": False,
    "crypto_email_last": None,
    "crypto_alert_snapshot": {},
    "crypto_live": {},
    "crypto_live_last": None,
}

_backtest_cache: dict = {}  # {symbol: result}


# â"€â"€ Scheduler â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
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

    # Lookback storico: carica ultimi N segnali per asset dal DB (usati per momentum e validazione)
    history_by_symbol = {}
    if db.is_enabled():
        try:
            history_by_symbol = db.get_history_compact(symbols, limit_per_symbol=5)
            n_with_history = sum(1 for v in history_by_symbol.values() if len(v) > 0)
            log.info(f"[SCHEDULER] History lookback: {n_with_history}/{len(symbols)} simboli con storico")
        except Exception as e:
            log.warning(f"[SCHEDULER] History lookback fallito: {e}")

    # Step 1: fetch real market data
    log.info(f"[STEP 1/4 MARKET] fetching {len(symbols)} symbols via yfinance...")
    tech = fetch_all(symbols)
    real_count = sum(1 for v in tech.values() if v is not None)
    log.info(f"[STEP 1/4 MARKET] {real_count}/{len(symbols)} OK | "
             + " ".join(f"{s}{'OK' if tech.get(s) else 'NO'}" for s in symbols[:10]))

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
        log.info("[STEP 2b/5 FUND] Disabilitato - configura fmp_api_key per attivare")

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
        log.info("[STEP 2d/5 INST] Disabilitato - configura fmp_api_key")

    # Step 2e: Technical signals (base layer)
    log.info(f"[STEP 2e/5 QUANT] running signal scanner...")
    tech_signals_raw = {a["symbol"]: build_quant_signal(tech.get(a["symbol"]), a) for a in ASSETS}

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

    # Signal momentum: aggiusta composite_score (+/-3 max) in base alla coerenza storica
    if history_by_symbol:
        for sig in signals:
            hist = history_by_symbol.get(sig["symbol"].upper(), [])
            if len(hist) < 2:
                continue
            cs_hist = [h.get("composite_score") or 0 for h in hist[:5]]
            trend   = cs_hist[0] - cs_hist[-1]   # positivo = score in crescita
            recent_actions = [h["action"] for h in hist[:3]]
            n_confirm = sum(1 for a in recent_actions if a == sig["action"])
            mom_adj = 0
            if trend > 15 and n_confirm >= 2:
                mom_adj = +3
            elif trend < -15 and n_confirm >= 2:
                mom_adj = -3
            if mom_adj != 0:
                sig["composite_score"] = max(-100, min(100, sig.get("composite_score", 0) + mom_adj))
            sig["signal_momentum"] = {
                "score_trend":  round(trend, 1),
                "adj":          mom_adj,
                "n_confirms":   n_confirm,
                "last_actions": recent_actions[:3],
                "history_runs": len(hist),
            }

    # Step 3: AI enrichment (top 3 only)
    # â"€â"€ Step 3: AI enrichment - SOLO in finestra 08:00-23:30 â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
    if not is_trading_hours():
        log.info("[STEP 3/4 AI] SKIPPED - fuori finestra operativa (08:00-23:30) - nessun credito AI consumato")
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

    # â"€â"€ Step 3.5: Smart Money - SOLO in finestra â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
    if not is_trading_hours():
        log.info("[STEP 3.5/4 SMART_MONEY] SKIPPED - fuori finestra operativa")
    elif CLAUDE_KEY:
        try:
            log.info("[STEP 3.5/4 SMART_MONEY] analisi istituzionale...")
            sm = run_smart_money_analysis([a["symbol"] for a in ASSETS], CLAUDE_KEY, PPLX_KEY)
            state["smart_money"] = sm
            n_opp = len(sm.get('opportunities', []))
            log.info(f"[STEP 3.5/4 SMART_MONEY] {n_opp} opp | qualita={sm.get('data_quality','?')}")
            if n_opp and state.get("signals"):
                state["signals"] = enrich_with_smart_money(state["signals"], sm)
                signals = state["signals"]
                log.info("[STEP 3.5/4 SMART_MONEY] Segnali asset arricchiti con overlay Smart Money")
            if db.is_enabled() and not sm.get("error"):
                try:
                    db.save_smart_money(sm)
                except Exception as _e:
                    log.error(f"[DB] save_smart_money: {_e}")
        except Exception as e:
            log.error(f"[STEP 3.5/4 SMART_MONEY] {e}")

    # Persistenza su MariaDB — DOPO SM enrichment (signals_json include overlay smart money)
    if db.is_enabled():
        try:
            db.save_analysis_run(signals, state.get("macro_context"))
        except Exception as e:
            log.error(f"[DB] save_analysis_run: {e}")

    # â"€â"€ Step 3.6: Perplexity validation finale (compact batch) â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
    if is_trading_hours() and PPLX_KEY:
        try:
            log.info("[STEP 3.6 PPLX_VALIDATE] Validazione finale segnali via Perplexity...")
            pplx_val = validate_signals_with_perplexity(signals, history_by_symbol, PPLX_KEY)
            if pplx_val:
                for sig in signals:
                    v = pplx_val.get(sig["symbol"])
                    if v:
                        sig["perplexity_validation"] = v
                state["signals"] = signals
                log.info(f"[STEP 3.6 PPLX_VALIDATE] {len(pplx_val)} segnali validati")
        except Exception as e:
            log.error(f"[STEP 3.6 PPLX_VALIDATE] {e}")

    signals = [_signal_to_eur(sig) for sig in signals]
    state["signals"] = signals

    # Step 4: email
    log.info(f"[STEP 4/4 EMAIL] enabled={OPTIONS.get('email_enabled')}")
    # â"€â"€ Step 4: Email - SOLO in finestra â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
    if not is_trading_hours():
        log.info("[STEP 4/4 EMAIL] SKIPPED - fuori finestra operativa (nessuna email notturna)")
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
    last_wallet_run_at = None
    last_income_run_at = None
    last_crypto_run_at = None

    # Prima scansione solo se siamo dentro la finestra
    if is_trading_hours():
        log.info("[SCHEDULER] Prima scansione avviata (siamo in finestra)")
        run_scan()
        last_run_at = datetime.utcnow()
        if _load_wallet_holdings():
            run_wallet_review()
            last_wallet_run_at = datetime.utcnow()
        run_income_plan()
        last_income_run_at = datetime.utcnow()
    else:
        log.info("[SCHEDULER] Fuori finestra operativa - prima scansione posticipata alle 08:00")
    if CRYPTO_ASSETS:
        run_crypto_scan()
        last_crypto_run_at = datetime.utcnow()

    while True:
        time.sleep(60)  # controlla ogni minuto

        now = datetime.utcnow()
        if CRYPTO_ASSETS and (
            last_crypto_run_at is None or (now - last_crypto_run_at).total_seconds() >= CRYPTO_SCHEDULER_MINUTES * 60
        ):
            log.info(f"[CRYPTO] Avvio scansione crypto (ultima: {last_crypto_run_at.strftime('%H:%M') if last_crypto_run_at else 'mai'})")
            run_crypto_scan()
            last_crypto_run_at = datetime.utcnow()

        if not SCHEDULER_ENABLED:
            continue

        # Blocco fuori finestra: nessuna operazione
        if not is_trading_hours():
            continue

        # Siamo in finestra: controlla se Ã¨ ora di girare
        if last_run_at is None or (now - last_run_at).total_seconds() >= SCHEDULER_MINUTES * 60:
            log.info(f"[SCHEDULER] Avvio scansione (ultima: {last_run_at.strftime('%H:%M') if last_run_at else 'mai'})")
            run_scan()
            last_run_at = datetime.utcnow()
        if _load_wallet_holdings() and (
            last_wallet_run_at is None or (now - last_wallet_run_at).total_seconds() >= WALLET_SCHEDULER_MINUTES * 60
        ):
            log.info(f"[WALLET] Avvio controllo portafoglio (ultima: {last_wallet_run_at.strftime('%H:%M') if last_wallet_run_at else 'mai'})")
            run_wallet_review()
            last_wallet_run_at = datetime.utcnow()
        if last_income_run_at is None or (now - last_income_run_at).total_seconds() >= INCOME_SCHEDULER_MINUTES * 60:
            log.info(f"[INCOME] Avvio piano rendita (ultima: {last_income_run_at.strftime('%H:%M') if last_income_run_at else 'mai'})")
            run_income_plan()
            last_income_run_at = datetime.utcnow()


# â"€â"€ FastAPI â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
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
    # Le API restituiscono 404 JSON normale - solo le route non-API servono il frontend
    if request.url.path.startswith("/api/"):
        from fastapi.responses import JSONResponse
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    return await _html()

@app.get("/health")
async def health():
    return {"status":"ok","version":"2.0.0",
            "assets":len(ASSETS),"anthropic":bool(CLAUDE_KEY),"perplexity":bool(PPLX_KEY),
            "trading_window":is_trading_hours(),"bind":BIND_HOST,
            "email_enabled":bool(OPTIONS.get("email_enabled")),
            "wallet_scheduler_minutes": WALLET_SCHEDULER_MINUTES,
            "crypto_scheduler_minutes": CRYPTO_SCHEDULER_MINUTES,
            "crypto_assets": len(CRYPTO_ASSETS)}

@app.get("/api/config")
async def config():
    return {"scheduler_minutes":SCHEDULER_MINUTES,"scheduler_enabled":SCHEDULER_ENABLED,
            "wallet_scheduler_minutes": WALLET_SCHEDULER_MINUTES,
            "income_scheduler_minutes": INCOME_SCHEDULER_MINUTES,
            "crypto_scheduler_minutes": CRYPTO_SCHEDULER_MINUTES,
            "crypto_live_refresh_seconds": CRYPTO_LIVE_REFRESH_SECONDS,
            "crypto_live_endpoint": COINGECKO_SIMPLE_PRICE_URL,
            "crypto_signals_endpoint": "/api/crypto/signals",
            "crypto_methodology_endpoint": "/api/crypto/methodology",
            "has_anthropic":bool(CLAUDE_KEY),"has_perplexity":bool(PPLX_KEY),
            "has_fmp":bool(FMP_KEY),"has_eia":bool(EIA_KEY),"has_fred":bool(FRED_KEY),
            "macro_enabled":MACRO_ENABLED,"fundamental_enabled":FUND_ENABLED,
            "email_enabled":bool(OPTIONS.get("email_enabled")),
            "wallet_email_alerts_enabled": WALLET_ALERTS_ENABLED,
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
    if any(a["symbol"].upper() == sym for a in all_assets):
        raise HTTPException(400, f"Simbolo {sym} già presente in watchlist")
    new_asset = {**asset.model_dump(), "symbol": sym}
    wallet_opts = {
        "add_to_wallet": new_asset.pop("add_to_wallet", False),
        "wallet_quantity": new_asset.pop("wallet_quantity", None),
        "wallet_avg_price": new_asset.pop("wallet_avg_price", None),
        "wallet_target_price": new_asset.pop("wallet_target_price", None),
        "wallet_stop_loss": new_asset.pop("wallet_stop_loss", None),
        "wallet_horizon_days": new_asset.pop("wallet_horizon_days", 30),
    }
    if not new_asset.get("full_name"):
        new_asset["full_name"] = new_asset["name"]
    if db.is_enabled():
        db.save_asset_to_db(new_asset)
    else:
        all_assets.append(new_asset)
        _save_assets(all_assets)
    all_assets = _load_all_assets()
    ASSETS = [a for a in all_assets if a.get("enabled", True)]
    if wallet_opts["add_to_wallet"] and db.is_enabled():
        _upsert_wallet_from_asset(
            new_asset,
            quantity=wallet_opts["wallet_quantity"],
            avg_price=wallet_opts["wallet_avg_price"],
            target_price=wallet_opts["wallet_target_price"],
            stop_loss=wallet_opts["wallet_stop_loss"],
            horizon_days=wallet_opts["wallet_horizon_days"],
        )
    log.info(f"[ASSETS] Aggiunto: {sym} ({new_asset['name']})")
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
    wallet_opts = {
        "add_to_wallet": updated.pop("add_to_wallet", False),
        "wallet_quantity": updated.pop("wallet_quantity", None),
        "wallet_avg_price": updated.pop("wallet_avg_price", None),
        "wallet_target_price": updated.pop("wallet_target_price", None),
        "wallet_stop_loss": updated.pop("wallet_stop_loss", None),
        "wallet_horizon_days": updated.pop("wallet_horizon_days", 30),
    }
    if not updated.get("full_name"):
        updated["full_name"] = updated["name"]
    if db.is_enabled():
        db.save_asset_to_db(updated)
    else:
        all_assets[idx] = updated
        _save_assets(all_assets)
    all_assets = _load_all_assets()
    ASSETS = [a for a in all_assets if a.get("enabled", True)]
    if wallet_opts["add_to_wallet"] and db.is_enabled():
        _upsert_wallet_from_asset(
            updated,
            quantity=wallet_opts["wallet_quantity"],
            avg_price=wallet_opts["wallet_avg_price"],
            target_price=wallet_opts["wallet_target_price"],
            stop_loss=wallet_opts["wallet_stop_loss"],
            horizon_days=wallet_opts["wallet_horizon_days"],
        )
    log.info(f"[ASSETS] Modificato: {sym}")
    return {"status": "updated", "asset": updated}

@app.patch("/api/assets/{symbol}/toggle")
async def toggle_asset(symbol: str):
    """Abilita/disabilita un asset senza eliminarlo."""
    global ASSETS
    sym = symbol.strip().upper()
    if db.is_enabled():
        new_state = db.toggle_asset_in_db(sym)
        if new_state is None:
            raise HTTPException(404, f"Simbolo {sym} non trovato")
        status = "enabled" if new_state else "disabled"
        all_assets = _load_all_assets()
        ASSETS = [a for a in all_assets if a.get("enabled", True)]
        log.info(f"[ASSETS] Toggle {sym}: {status}")
        return {"status": status, "symbol": sym, "enabled": new_state}
    all_assets = _load_all_assets()
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
    sym = symbol.strip().upper()
    if db.is_enabled():
        if not db.delete_asset_from_db(sym):
            raise HTTPException(404, f"Simbolo {sym} non trovato")
        all_assets = _load_all_assets()
        ASSETS = [a for a in all_assets if a.get("enabled", True)]
        log.info(f"[ASSETS] Eliminato: {sym}")
        return {"status": "deleted", "symbol": sym, "remaining": len(all_assets)}
    all_assets = _load_all_assets()
    before = len(all_assets)
    all_assets = [a for a in all_assets if a["symbol"].upper() != sym]
    if len(all_assets) == before:
        raise HTTPException(404, f"Simbolo {sym} non trovato")
    _save_assets(all_assets)
    ASSETS = [a for a in all_assets if a.get("enabled", True)]
    log.info(f"[ASSETS] Eliminato: {sym}")
    return {"status": "deleted", "symbol": sym, "remaining": len(all_assets)}


@app.get("/api/wallet")
async def get_wallet():
    wallet = state.get("wallet")
    if not wallet:
        wallet = run_wallet_review()
    return {
        "holdings_count": len(_load_wallet_holdings()),
        "closed_count": len(_load_wallet_history()),
        "last_run": state.get("wallet_last_run"),
        "next_run": state.get("wallet_next_run"),
        **wallet,
    }


@app.get("/api/wallet/holdings")
async def get_wallet_holdings(include_closed: bool = False):
    holdings = db.load_wallet_holdings(include_closed=include_closed) if db.is_enabled() else []
    return {"count": len(holdings), "holdings": holdings}


@app.post("/api/wallet/holdings")
async def add_wallet_holding(holding: WalletHoldingModel):
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato: il wallet richiede DB attivo")
    normalized = _normalize_wallet_holding(holding.model_dump())
    if normalized["quantity"] < 0:
        raise HTTPException(400, "quantity non puo' essere < 0")
    if normalized["avg_price"] < 0:
        raise HTTPException(400, "avg_price non puo' essere < 0")
    db.save_wallet_holding(normalized)
    wallet = run_wallet_review()
    return {"status": "added", "holding": normalized, "wallet": wallet}


@app.put("/api/wallet/holdings/{symbol}")
async def update_wallet_holding(symbol: str, holding: WalletHoldingModel):
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato: il wallet richiede DB attivo")
    normalized = _normalize_wallet_holding({**holding.model_dump(), "symbol": symbol.upper()})
    if normalized["quantity"] < 0:
        raise HTTPException(400, "quantity non puo' essere < 0")
    if normalized["avg_price"] < 0:
        raise HTTPException(400, "avg_price non puo' essere < 0")
    db.save_wallet_holding(normalized)
    wallet = run_wallet_review()
    return {"status": "updated", "holding": normalized, "wallet": wallet}


@app.delete("/api/wallet/holdings/{symbol}")
async def remove_wallet_holding(symbol: str):
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato: il wallet richiede DB attivo")
    if not db.delete_wallet_holding(symbol.upper()):
        raise HTTPException(404, f"Posizione {symbol.upper()} non trovata")
    wallet = run_wallet_review()
    return {"status": "deleted", "symbol": symbol.upper(), "wallet": wallet}


@app.post("/api/wallet/holdings/{symbol}/close")
async def close_wallet_position(symbol: str, payload: Optional[dict] = Body(default=None)):
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato: il wallet richiede DB attivo")
    payload = payload or {}
    exit_price = payload.get("exit_price")
    exit_note = payload.get("exit_note", "")
    if exit_price is not None:
        try:
            exit_price = float(exit_price)
        except Exception:
            raise HTTPException(400, "exit_price non valido")
    if not db.close_wallet_holding(symbol.upper(), exit_price=exit_price, exit_note=exit_note):
        raise HTTPException(404, f"Posizione {symbol.upper()} non trovata")
    wallet = run_wallet_review()
    return {"status": "closed", "symbol": symbol.upper(), "wallet": wallet}


@app.post("/api/wallet/holdings/{symbol}/reopen")
async def reopen_wallet_position(symbol: str):
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato: il wallet richiede DB attivo")
    if not db.reopen_wallet_holding(symbol.upper()):
        raise HTTPException(404, f"Posizione {symbol.upper()} non trovata")
    wallet = run_wallet_review()
    return {"status": "reopened", "symbol": symbol.upper(), "wallet": wallet}


@app.post("/api/wallet/refresh")
async def refresh_wallet(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_wallet_review, True)
    return {"status": "started"}


@app.get("/api/income-plan")
async def get_income_plan():
    plan = state.get("income_plan")
    if not plan:
        plan = run_income_plan()
    return plan


@app.post("/api/income-plan/refresh")
async def refresh_income_plan(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_income_plan)
    return {"status": "started"}


@app.get("/api/income-plan/history")
async def get_income_plan_history(days: int = 7):
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato")
    return {"days": days, "runs": db.get_income_history(days)}


@app.get("/api/wallet/history")
async def get_wallet_history(days: int = 7):
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato")
    return {"days": days, "runs": db.get_wallet_history(days)}


@app.get("/api/wallet/history/{symbol}")
async def get_wallet_position_history(symbol: str, days: int = 30):
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato")
    return {"symbol": symbol.upper(), "days": days, "history": db.get_wallet_position_history(symbol, days)}

@app.get("/api/assets/lookup")
async def lookup_asset_by_isin(isin: str):
    """
    Risolve un ISIN → dati asset completi (symbol Yahoo, nome, mercato, valuta, borsa).
    Usa OpenFIGI (gratuito) + Yahoo Finance per validare il simbolo.
    """
    if not isin or len(isin.strip()) != 12:
        raise HTTPException(400, "ISIN non valido - deve essere esattamente 12 caratteri")
    result = lookup_isin(isin.strip().upper())
    if result is None:
        raise HTTPException(404, f"Impossibile risolvere {isin}")
    if result.get("error"):
        raise HTTPException(422, result["error"])
    return result


@app.get("/api/signals")
async def get_signals(market: Optional[str] = None,
                      asset_type: Optional[str] = None,
                      action: Optional[str] = None):
    sigs = [_signal_to_eur(s) for s in state.get("signals", [])]
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
        if s["symbol"].upper() == sym: return _signal_to_eur(s)
    raise HTTPException(404, f"{symbol} not found")


@app.get("/api/crypto/signals")
async def get_crypto_signals(action: Optional[str] = None):
    sigs = state.get("crypto_signals", [])
    if not sigs:
        result = run_crypto_scan()
        sigs = result.get("signals", [])
    if action:
        sigs = [s for s in sigs if s.get("action", "").upper() == action.upper()]
    return {
        "last_run": state.get("crypto_last_run"),
        "next_run": state.get("crypto_next_run"),
        "count": len(sigs),
        "signals": sigs,
        "assets": CRYPTO_ASSETS,
    }


@app.get("/api/crypto/live")
async def get_crypto_live():
    return fetch_crypto_live_prices()


@app.post("/api/crypto/refresh")
async def refresh_crypto(background_tasks: BackgroundTasks):
    if state.get("crypto_running"):
        return {"status": "already_running"}
    background_tasks.add_task(run_crypto_scan)
    return {"status": "started"}


@app.get("/api/crypto/methodology")
async def get_crypto_methodology():
    return {
        "current_pipeline": [
            {
                "step": 1,
                "label": "Market data REST",
                "api": "Yahoo Finance Chart API",
                "endpoint_pattern": "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                "purpose": "Scaricare OHLCV giornaliero per BTC-EUR, ETH-EUR, SOL-EUR, XRP-EUR, ADA-EUR, DOT-EUR, MATIC-EUR, DOGE-EUR, SHIB-EUR.",
            },
            {
                "step": 2,
                "label": "Indicatori tecnici",
                "api": "Calcolo interno Market Analyze",
                "purpose": "RSI, medie mobili, MACD, ADX, OBV, Bollinger, ATR, supporti/resistenze, ROC, squeeze, divergenze RSI.",
            },
            {
                "step": 3,
                "label": "Scoring quantitativo",
                "api": "Signal engine interno",
                "purpose": "Trasformare gli indicatori in score, confidence e azione BUY / SELL / WATCHLIST / HOLD.",
            },
            {
                "step": 4,
                "label": "Normalizzazione EUR",
                "api": "FX conversion",
                "endpoint_pattern": "https://api.frankfurter.app/latest?from={currency}&to=EUR",
                "purpose": "Uniformare tutti i valori mostrati in euro.",
            },
            {
                "step": 5,
                "label": "Alert email",
                "api": "SMTP/OAuth2 configurato nell'addon",
                "purpose": "Inviare mail quando un segnale crypto cambia verso BUY o SELL.",
            },
        ],
        "reasoning_logic": [
            "Trend: prezzo sopra MA20 e MA50, o sotto di esse.",
            "Momentum: MACD histogram e incroci MACD.",
            "Forza: ADX e direzione +DI / -DI.",
            "Flussi: OBV e conferma volume.",
            "Estremi: RSI, Bollinger, Stocastico.",
            "Timing: supporti, resistenze, ATR, risk/reward.",
            "Output: score numerico, confidence percentuale e livelli operativi.",
        ],
        "verified_realtime_providers": [
            {
                "type": "REST",
                "provider": "CoinGecko",
                "endpoints": [
                    "https://api.coingecko.com/api/v3/simple/price",
                    "https://api.coingecko.com/api/v3/coins/markets",
                    "https://api.coingecko.com/api/v3/coins/{id}/market_chart/range",
                    "https://api.coingecko.com/api/v3/coins/{id}/ohlc/range",
                ],
                "notes": "Usato per snapshot live prezzo EUR; molto utile anche per mercati e storico OHLC crypto.",
            },
            {
                "type": "REST",
                "provider": "Coinbase Advanced Trade",
                "endpoints": ["/api/v3/brokerage/products", "/api/v3/brokerage/products/{product_id}/candles"],
                "notes": "Utile per metadata e candele di mercato crypto.",
            },
            {
                "type": "REST",
                "provider": "Kraken",
                "endpoints": ["/public/OHLC", "/public/Ticker"],
                "notes": "Utile per OHLC e ticker pubblici su coppie crypto.",
            },
            {
                "type": "AI",
                "provider": "Perplexity Sonar",
                "notes": "Adatto a sintesi news e contesto realtime web-grounded; non sostituisce il feed prezzi.",
            },
        ],
    }

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
        raise HTTPException(400, "FMP API key non configurata - configura fmp_api_key nel config add-on")
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
        if result.get("opportunities") and state.get("signals"):
            state["signals"] = enrich_with_smart_money(state["signals"], result)
        log.info("[SMART_MONEY] Analisi aggiornata e segnali arricchiti via API")
    background_tasks.add_task(_run)
    return {"status": "started"}

@app.get("/api/macro")
async def get_macro():
    """Restituisce l'ultimo contesto macro (FRED, ECB, EIA, Yahoo)."""
    m = state.get("macro_context")
    if m: return m
    raise HTTPException(404, "Macro context non ancora disponibile")

# ── Trend / storico DB ────────────────────────────────────────────────────────
@app.get("/api/trends/{symbol}")
async def get_trends(symbol: str, days: int = 30):
    """
    Storico segnali per un asset (da MariaDB).
    Utile per costruire grafici di trend score/action nel tempo.
    """
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato")
    data = db.get_signal_trend(symbol.upper(), days)
    return {"symbol": symbol.upper(), "days": days, "count": len(data), "history": data}

@app.get("/api/trends")
async def get_trends_summary(days: int = 7):
    """Riepilogo degli ultimi scan con contatori BUY/SELL/WATCHLIST."""
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato")
    runs = db.get_analysis_summary(days)
    top  = db.get_top_signals_trend(days)
    return {"days": days, "runs": runs, "top_signals": top}

@app.get("/api/trends/smart-money")
async def get_smart_money_trends(limit: int = 10):
    """Storico delle analisi Smart Money (solo metadati, no JSON completo)."""
    if not db.is_enabled():
        raise HTTPException(503, "MariaDB non configurato")
    return {"history": db.get_smart_money_history(limit)}

@app.get("/api/db/status")
async def db_status():
    """Stato connessione MariaDB."""
    return {
        "enabled":  db.is_enabled(),
        "host":     OPTIONS.get("db_host", ""),
        "database": OPTIONS.get("db_name", ""),
    }

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
        currency = "EUR"
        for s in state.get("signals", []):
            if s.get("symbol", "").upper() == sym:
                currency = s.get("original_currency") or s.get("currency") or "EUR"
                break
        rate = _get_fx_rate_to_eur(currency)
        closes = [round(v * rate, 4) for v in closes]
        # Media mobile 20gg
        import pandas as pd
        ma20 = df["Close"].rolling(20).mean().tail(days)
        ma20_vals = [round(float(v) * rate,4) if not pd.isna(v) else None for v in ma20.tolist()]
        return {
            "symbol": sym,
            "dates":  dates,
            "closes": closes,
            "volumes":volumes,
            "ma20":   ma20_vals,
            "currency": "EUR",
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
