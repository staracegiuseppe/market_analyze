# mailer.py v5.0 - Email report completo in italiano
# Include: segnali tecnici + Smart Money + scoperte fuori watchlist
# OAuth2 (primario) + App Password (fallback)

import smtplib, base64, logging, time, json
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from datetime             import datetime
from typing               import List, Dict, Optional
import requests as _req

log = logging.getLogger("mailer")

_LAST_EMAIL_DIAG: Dict = {
    "status": "idle",
    "reason": None,
    "detail": None,
    "transport": None,
    "attempts": [],
    "updated_at": None,
}
_LAST_OAUTH_ERROR: Dict = {}
_LAST_SMTP_ERROR: Dict = {}

try:
    from smart_money import build_email_section as _sm_section
    _HAS_SM = True
except ImportError:
    _HAS_SM = False

# ── OAuth2 ────────────────────────────────────────────────────────────────────
_tok: Dict = {"access_token": None, "expires_at": 0}


def _set_email_diag(**kwargs):
    global _LAST_EMAIL_DIAG
    _LAST_EMAIL_DIAG = {
        **_LAST_EMAIL_DIAG,
        **kwargs,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }


def get_email_diagnostic() -> Dict:
    return dict(_LAST_EMAIL_DIAG)

def _access_token(cid, csecret, rtoken):
    global _LAST_OAUTH_ERROR
    if _tok["access_token"] and _tok["expires_at"] > time.time() + 60:
        return _tok["access_token"]
    try:
        r = _req.post("https://oauth2.googleapis.com/token", data={
            "client_id": cid, "client_secret": csecret,
            "refresh_token": rtoken, "grant_type": "refresh_token",
        }, timeout=10)
        if r.status_code == 200:
            d = r.json()
            _tok["access_token"] = d["access_token"]
            _tok["expires_at"]   = time.time() + int(d.get("expires_in", 3600))
            log.info("[OAUTH2] access_token OK")
            _LAST_OAUTH_ERROR = {}
            return _tok["access_token"]
        try:
            err = r.json()
        except Exception:
            err = {"raw": r.text[:300]}
        _LAST_OAUTH_ERROR = {
            "status": "failed",
            "transport": "oauth2",
            "reason": err.get("error") or "oauth2_token_error",
            "detail": err.get("error_description") or err.get("raw") or r.text[:300],
            "http_status": r.status_code,
        }
        log.error(f"[OAUTH2] {r.status_code}: {r.text[:150]}")
    except Exception as e:
        _LAST_OAUTH_ERROR = {
            "status": "failed",
            "transport": "oauth2",
            "reason": "oauth2_exception",
            "detail": str(e),
        }
        log.error(f"[OAUTH2] {e}")
    return None

def _send_oauth2(msg, sender, recipient, cid, csecret, rtoken):
    global _LAST_OAUTH_ERROR
    token = _access_token(cid, csecret, rtoken)
    if not token: return False
    auth = base64.b64encode(f"user={sender}\x01auth=Bearer {token}\x01\x01".encode()).decode()
    try:
        s = smtplib.SMTP("smtp.gmail.com", 587, timeout=15)
        s.ehlo(); s.starttls(); s.ehlo()
        code, _ = s.docmd("AUTH", "XOAUTH2 " + auth)
        if code == 334: s.docmd("")
        s.sendmail(sender, recipient, msg.as_string()); s.quit()
        _LAST_OAUTH_ERROR = {}
        log.info("[OAUTH2] ✓"); return True
    except Exception as e:
        _LAST_OAUTH_ERROR = {
            "status": "failed",
            "transport": "oauth2",
            "reason": "oauth2_send_failed",
            "detail": str(e),
        }
        log.error(f"[OAUTH2] {e}"); return False

def _send_apppassword(msg, sender, recipient, host, port, user, pw, tls):
    global _LAST_SMTP_ERROR
    try:
        s = smtplib.SMTP(host, port, timeout=15) if tls else smtplib.SMTP_SSL(host, port, timeout=15)
        if tls: s.ehlo(); s.starttls()
        s.login(user, pw)
        s.sendmail(sender, recipient, msg.as_string()); s.quit()
        _LAST_SMTP_ERROR = {}
        log.info("[SMTP] ✓"); return True
    except smtplib.SMTPAuthenticationError:
        _LAST_SMTP_ERROR = {
            "status": "failed",
            "transport": "smtp",
            "reason": "smtp_auth_failed",
            "detail": "Autenticazione SMTP fallita",
        }
        log.error("[SMTP] Auth fallita"); return False
    except Exception as e:
        _LAST_SMTP_ERROR = {
            "status": "failed",
            "transport": "smtp",
            "reason": "smtp_exception",
            "detail": str(e),
        }
        log.error(f"[SMTP] {e}"); return False


def _resolve_sender_recipient(opts):
    sender = opts.get("email_from") or opts.get("smtp_user") or opts.get("email_to")
    recipient = opts.get("email_to")
    return sender, recipient


def _dispatch_email(msg, opts):
    sender, recipient = _resolve_sender_recipient(opts)
    if not sender or not recipient:
        _set_email_diag(status="failed", reason="missing_sender_or_recipient", detail="Sender o recipient mancanti", transport=None, attempts=[])
        log.error("[EMAIL] sender/recipient mancanti")
        return False
    attempts = []
    cid     = opts.get("oauth2_client_id", "")
    secret  = opts.get("oauth2_client_secret", "")
    refresh = opts.get("oauth2_refresh_token", "")
    if cid and secret and refresh and sender.endswith("@gmail.com"):
        if _send_oauth2(msg, sender, recipient, cid, secret, refresh):
            _set_email_diag(status="sent", reason="ok", detail="Invio riuscito via OAuth2", transport="oauth2", attempts=attempts + [{"transport":"oauth2","status":"sent"}])
            return True
        if _LAST_OAUTH_ERROR:
            attempts.append(dict(_LAST_OAUTH_ERROR))
    ok = _send_apppassword(
        msg, sender, recipient,
        opts.get("smtp_host", "smtp.gmail.com"),
        int(opts.get("smtp_port", 587)),
        opts.get("smtp_user", ""),
        opts.get("smtp_password", ""),
        bool(opts.get("smtp_tls", True)),
    )
    if ok:
        _set_email_diag(status="sent", reason="ok", detail="Invio riuscito via SMTP", transport="smtp", attempts=attempts + [{"transport":"smtp","status":"sent"}])
        return True
    if _LAST_SMTP_ERROR:
        attempts.append(dict(_LAST_SMTP_ERROR))
    primary = attempts[0] if attempts else {"reason": "dispatch_failed", "detail": "Invio email fallito"}
    _set_email_diag(
        status="failed",
        reason=primary.get("reason", "dispatch_failed"),
        detail=primary.get("detail", "Invio email fallito"),
        transport=primary.get("transport"),
        attempts=attempts,
    )
    return False


# ── Traduzione motivazioni ────────────────────────────────────────────────────
_TR = {
    "price>MA20>MA50":        "Prezzo sopra media 20 e 50 giorni → trend rialzista",
    "price<MA20<MA50":        "Prezzo sotto media 20 e 50 giorni → trend ribassista",
    "golden_cross":           "Golden Cross: media 20 ha superato media 50 → segnale rialzista forte",
    "death_cross":            "Death Cross: media 20 ha incrociato al ribasso media 50 → segnale ribassista",
    "above MA200":            "Prezzo sopra media 200 giorni → trend di lungo periodo positivo",
    "below MA200":            "Prezzo sotto media 200 giorni → trend di lungo periodo negativo",
    "MACD hist>0":            "MACD positivo → momentum in crescita",
    "MACD hist<0":            "MACD negativo → momentum in calo",
    "MACD bullish crossover": "Incrocio MACD rialzista → cambio direzione verso l'alto",
    "MACD bearish crossover": "Incrocio MACD ribassista → cambio direzione verso il basso",
    "RSI constructive":       "RSI in zona costruttiva (40–65) → forza senza eccessi",
    "RSI recovering":         "RSI in recupero → uscita dalla zona di debolezza",
    "RSI overbought":         "RSI in ipercomprato (>70) → attenzione a possibile correzione",
    "RSI oversold":           "RSI in ipervenduto (<30) → possibile rimbalzo tecnico",
    "RSI weak":               "RSI debole (<35) → pressione ribassista persistente",
    "ADX":                    "ADX elevato → trend forte e direzionale",
    "OBV bullish":            "Volume in accumulo (OBV crescente) → acquisti istituzionali in corso",
    "OBV bearish":            "Volume in distribuzione (OBV calante) → vendite istituzionali in corso",
    "BB oversold zone":       "Prezzo nella banda bassa di Bollinger → zona di supporto tecnico",
    "BB overbought":          "Prezzo nella banda alta di Bollinger → zona di resistenza tecnica",
    "high volume confirms":   "Volume elevato confirma la direzione del movimento",
    "ROC10":                  "Momentum positivo a 10 giorni",
}

def _tr(reason: str) -> str:
    import re
    for key, trad in _TR.items():
        if key in reason:
            nums = re.findall(r'[-+]?\d+\.?\d*', reason)
            return trad + (f" ({nums[0]})" if nums else "")
    return reason


# ── Score breakdown in italiano ───────────────────────────────────────────────
_BD_NOMI = {
    "ma_align":  "Allineamento medie mobili",
    "ma_cross":  "Incrocio medie mobili",
    "vs_ma200":  "Posizione vs MA200 (lungo periodo)",
    "macd":      "MACD",
    "macd_cross":"Incrocio MACD",
    "rsi":       "RSI",
    "adx":       "ADX — Forza del trend",
    "obv":       "OBV — Volume istituzionale",
    "volume":    "Conferma del volume",
    "bb":        "Bande di Bollinger",
    "roc":       "Momentum ROC 10 giorni",
}

def _score_breakdown_html(bd: dict) -> str:
    if not bd: return ""
    rows = ""
    for k, v in bd.items():
        if v == 0: continue
        col = "#16A34A" if v > 0 else "#DC2626"
        w   = min(100, abs(v) * 5)
        s   = ("+" if v > 0 else "") + str(v)
        nm  = _BD_NOMI.get(k, k)
        rows += (
            f'<tr><td style="padding:3px 8px;color:#9CA3AF;font-size:11px;white-space:nowrap">{nm}</td>'
            f'<td style="padding:3px 8px;width:100px">'
            f'<div style="height:5px;background:#1F2937;border-radius:3px">'
            f'<div style="height:5px;width:{w}%;background:{col};border-radius:3px"></div>'
            f'</div></td>'
            f'<td style="padding:3px 8px;color:{col};font-family:monospace;font-size:11px;text-align:right">{s}</td>'
            f'</tr>'
        )
    return (
        '<div style="margin-top:12px">'
        '<div style="font-size:9px;color:#6B7280;letter-spacing:.1em;text-transform:uppercase;margin-bottom:6px">CONTRIBUTO INDICATORI AL PUNTEGGIO</div>'
        f'<table style="width:100%;border-collapse:collapse">{rows}</table>'
        '</div>'
    ) if rows else ""


# ── Indicatori tecnici ────────────────────────────────────────────────────────
def _ind_section(ind: dict) -> str:
    if not ind: return ""
    defs = [
        ("rsi",        "RSI (14)",             lambda v: f"{v}",    lambda v: "Ipercomprato ⚠" if v>70 else "Ipervenduto ⚠" if v<30 else "Zona neutrale"),
        ("adx",        "ADX — Forza trend",    lambda v: f"{v}",    lambda v: "Trend forte ✓" if v>25 else "Trend debole"),
        ("macd_hist",  "MACD Istogramma",       lambda v: f"{v:+.4f}", lambda v: "Momentum positivo" if v>0 else "Momentum negativo"),
        ("obv_trend",  "OBV Trend",            lambda v: "Rialzista" if v=="bullish" else "Ribassista", lambda v: "Accumulo istituzionale" if v=="bullish" else "Distribuzione"),
        ("bb_pos",     "Posizione Bollinger",   lambda v: f"{v:.0f}%", lambda v: "Zona alta (resistenza)" if v>70 else "Zona bassa (supporto)" if v<30 else "Zona centrale"),
        ("stoch_k",    "Stocastico K",          lambda v: f"{v}",    lambda v: "Ipercomprato" if v>80 else "Ipervenduto" if v<20 else "Neutrale"),
        ("vol_signal", "Volume",                lambda v: {"HIGH":"Alto","LOW":"Basso","NORMAL":"Normale"}.get(v,v), lambda v: ""),
        ("roc10",      "ROC 10 giorni",         lambda v: f"{v:+.1f}%", lambda v: "Momentum positivo" if v>0 else "Momentum negativo"),
        ("atr_regime", "Volatilità",            lambda v: {"HIGH_VOL":"Alta","LOW_VOL":"Bassa","NORMAL_VOL":"Normale"}.get(v,v), lambda v: ""),
        ("ma_cross",   "Incrocio medie",        lambda v: {"golden_cross":"Golden Cross ↑","death_cross":"Death Cross ↓","ma20_above_ma50":"MA20 > MA50","ma20_below_ma50":"MA20 < MA50"}.get(v,v), lambda v: ""),
        ("support",    "Supporto",              lambda v: f"{v}",    lambda v: "Livello di acquisto atteso"),
        ("resistance", "Resistenza",            lambda v: f"{v}",    lambda v: "Livello di vendita atteso"),
        ("ma20",       "Media mobile 20gg",     lambda v: f"{v}",    lambda v: ""),
        ("ma50",       "Media mobile 50gg",     lambda v: f"{v}",    lambda v: ""),
        ("ma200",      "Media mobile 200gg",    lambda v: f"{v}",    lambda v: ""),
    ]
    rows = ""
    for key, label, fmt, comment in defs:
        v = ind.get(key)
        if v is None: continue
        try:
            vstr = fmt(v)
            cstr = comment(v)
        except Exception:
            vstr, cstr = str(v), ""
        rows += (
            f'<tr style="border-bottom:1px solid #1F293718">'
            f'<td style="padding:4px 8px;color:#9CA3AF;font-size:11px">{label}</td>'
            f'<td style="padding:4px 8px;color:#F9FAFB;font-size:11px;font-family:monospace">{vstr}</td>'
            f'<td style="padding:4px 8px;color:#6B7280;font-size:10px">{cstr}</td>'
            f'</tr>'
        )
    return (
        '<div style="margin-top:14px">'
        '<div style="font-size:9px;color:#6B7280;letter-spacing:.1em;text-transform:uppercase;margin-bottom:6px">INDICATORI TECNICI COMPLETI</div>'
        f'<table style="width:100%;border-collapse:collapse;background:#0A0F1A;border-radius:6px">{rows}</table>'
        '</div>'
    ) if rows else ""


# ── Performance table (grafico alternativo per email) ────────────────────────
def _perf_section(ind: dict, curr: str) -> str:
    """Tabella performance e minigrafico a barre ASCII per email."""
    perf = ind.get("performance", {})
    if not perf: return ""

    bars = {"1d": "█", "5d": "████", "20d": "████████", "60d": "████████████"}
    rows = ""
    for period, label, days_label in [
        ("1d",  "1 giorno",   "1g"),
        ("5d",  "1 settimana","1s"),
        ("20d", "1 mese",     "1m"),
        ("60d", "3 mesi",     "3m"),
    ]:
        v = perf.get(period)
        if v is None: continue
        col   = "#16A34A" if v >= 0 else "#DC2626"
        sign  = "+" if v >= 0 else ""
        bar_w = min(80, max(3, int(abs(v) * 4)))
        rows += (
            f'<tr style="border-bottom:1px solid #1F293718">'
            f'<td style="padding:4px 8px;color:#9CA3AF;font-size:11px;width:80px">{label}</td>'
            f'<td style="padding:4px 8px">'
            f'<div style="height:8px;background:#1F2937;border-radius:2px;position:relative">'
            f'<div style="height:8px;width:{bar_w}%;max-width:100%;background:{col};border-radius:2px"></div>'
            f'</div></td>'
            f'<td style="padding:4px 8px;color:{col};font-size:12px;font-weight:700;font-family:monospace;text-align:right;white-space:nowrap">{sign}{v}%</td>'
            f'</tr>'
        )
    return (
        '<div style="margin-top:14px">'
        '<div style="font-size:9px;color:#6B7280;letter-spacing:.1em;text-transform:uppercase;margin-bottom:6px">PERFORMANCE STORICA</div>'
        f'<table style="width:100%;border-collapse:collapse">{rows}</table>'
        '</div>'
    ) if rows else ""


def send_wallet_alert(wallet_result: Dict, opts: Dict) -> bool:
    alerts = wallet_result.get("alerts", []) or []
    if not alerts:
        return True

    sender, recipient = _resolve_sender_recipient(opts)
    if not sender or not recipient:
        log.error("[WALLET EMAIL] sender/recipient mancanti")
        return False

    summary = wallet_result.get("summary", {}) or {}
    cards = ""
    for alert in alerts:
        rec = alert.get("recommendation", "HOLD")
        action = alert.get("signal_action", "HOLD")
        col = "#DC2626" if rec in ("SELL", "RISK_EXIT", "REDUCE", "TAKE_PROFIT") else "#16A34A" if rec in ("BUY", "ACCUMULATE") else "#F59E0B"
        reasons = "".join(
            f'<li style="margin-bottom:4px">{reason}</li>'
            for reason in (alert.get("reasons") or [])[:4]
        )
        pnl = alert.get("pnl_pct")
        pnl_html = ""
        if pnl is not None:
            pnl_html = f'<div style="font-size:11px;color:{"#16A34A" if pnl >= 0 else "#DC2626"};margin-top:6px">P/L non realizzato: {pnl:+.2f}%</div>'
        cards += (
            f'<div style="border:1px solid {col}33;border-left:4px solid {col};border-radius:8px;padding:14px 16px;margin-bottom:12px;background:#0B1220">'
            f'<div style="display:flex;justify-content:space-between;gap:10px;align-items:flex-start">'
            f'<div><div style="font-size:18px;font-weight:800;color:#F9FAFB">{alert.get("symbol","?")}</div>'
            f'<div style="font-size:11px;color:#9CA3AF">{alert.get("name","")}</div></div>'
            f'<div style="text-align:right"><div style="font-size:11px;color:{col};font-weight:800">{rec}</div>'
            f'<div style="font-size:10px;color:#9CA3AF">Segnale: {action} · conf. {alert.get("confidence",0)}%</div></div></div>'
            f'<div style="font-size:12px;color:#D1D5DB;margin-top:10px">Prezzo attuale: <b>{alert.get("current_price","—")}</b> · Orizzonte stimato: <b>{alert.get("holding_days_estimate","—")} giorni</b></div>'
            f'{pnl_html}'
            f'<ul style="margin:10px 0 0 18px;padding:0;color:#C7D2FE;font-size:11px;line-height:1.6">{reasons}</ul>'
            f'</div>'
        )

    html = (
        '<html><body style="background:#07090D;color:#D0DFF8;font-family:Segoe UI,Arial,sans-serif;padding:20px">'
        '<div style="max-width:760px;margin:0 auto">'
        '<div style="font-size:22px;font-weight:900;color:#F9FAFB;margin-bottom:6px">Alert Wallet</div>'
        '<div style="font-size:12px;color:#94A3B8;margin-bottom:18px">Sono stati rilevati segnali rilevanti sul tuo portafoglio.</div>'
        '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:18px">'
        f'<div style="background:#0B1220;border:1px solid #1F2937;border-radius:8px;padding:10px 12px">Posizioni: <b>{summary.get("holdings_count",0)}</b></div>'
        f'<div style="background:#0B1220;border:1px solid #1F2937;border-radius:8px;padding:10px 12px">Valore: <b>{summary.get("market_value_total",0):,.2f}</b></div>'
        f'<div style="background:#0B1220;border:1px solid #1F2937;border-radius:8px;padding:10px 12px">P/L: <b>{summary.get("pnl_total",0):+,.2f}</b></div>'
        '</div>'
        f'{cards}'
        '<div style="margin-top:16px;font-size:10px;color:#64748B">Messaggio generato automaticamente da Market Analyze. Valuta sempre rischio, liquidità e conferme operative.</div>'
        '</div></body></html>'
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Market Analyze] Alert wallet: {len(alerts)} segnale/i rilevanti"
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html, "html", "utf-8"))
    return _dispatch_email(msg, opts)


def send_crypto_alert(signals: List[Dict], opts: Dict) -> bool:
    alerts = [s for s in signals if s.get("action") in ("BUY", "SELL")]
    if not alerts:
        return True

    sender, recipient = _resolve_sender_recipient(opts)
    if not sender or not recipient:
        log.error("[CRYPTO EMAIL] sender/recipient mancanti")
        return False

    rows = ""
    for sig in alerts:
        action = sig.get("action", "HOLD")
        col = "#16A34A" if action == "BUY" else "#DC2626"
        reasons = "".join(f"<li>{_tr(r)}</li>" for r in (sig.get("reasons") or [])[:4])
        rows += (
            f'<div style="border:1px solid {col}33;border-left:4px solid {col};border-radius:8px;padding:14px 16px;margin-bottom:12px;background:#0B1220">'
            f'<div style="display:flex;justify-content:space-between;gap:10px">'
            f'<div><div style="font-size:18px;font-weight:800;color:#F9FAFB">{sig.get("name", sig.get("symbol","?"))}</div>'
            f'<div style="font-size:11px;color:#9CA3AF">{sig.get("symbol","?")} · {sig.get("exchange","Crypto")}</div></div>'
            f'<div style="text-align:right"><div style="font-size:12px;font-weight:800;color:{col}">{action}</div>'
            f'<div style="font-size:10px;color:#9CA3AF">conf. {sig.get("confidence",0)}% · score {sig.get("score",0)}</div></div></div>'
            f'<div style="font-size:12px;color:#D1D5DB;margin-top:8px">Prezzo: <b>{sig.get("currency","EUR")} {sig.get("price","—")}</b></div>'
            f'<ul style="margin:10px 0 0 18px;padding:0;color:#C7D2FE;font-size:11px;line-height:1.6">{reasons}</ul>'
            f'</div>'
        )

    html = (
        '<html><body style="background:#07090D;color:#D0DFF8;font-family:Segoe UI,Arial,sans-serif;padding:20px">'
        '<div style="max-width:760px;margin:0 auto">'
        '<div style="font-size:22px;font-weight:900;color:#F9FAFB;margin-bottom:6px">Alert Trading Crypto</div>'
        '<div style="font-size:12px;color:#94A3B8;margin-bottom:18px">Sono stati rilevati segnali BUY/SELL sulle crypto monitorate.</div>'
        f'{rows}'
        '<div style="margin-top:16px;font-size:10px;color:#64748B">Messaggio automatico di Market Analyze. Le crypto sono altamente volatili: valida sempre liquidita\' e gestione del rischio.</div>'
        '</div></body></html>'
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Market Analyze] Alert crypto: {len(alerts)} segnale/i"
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html, "html", "utf-8"))
    return _dispatch_email(msg, opts)


# ── Card segnale completa ─────────────────────────────────────────────────────
def _card(r: dict) -> str:
    action    = r.get("action", "HOLD")
    col       = {"BUY":"#16A34A","SELL":"#DC2626","WATCHLIST":"#2563EB"}.get(action,"#6B7280")
    etiq      = {"BUY":"🟢 ACQUISTO","SELL":"🔴 VENDITA","WATCHLIST":"🔵 DA OSSERVARE","HOLD":"⚪ NEUTRO"}.get(action, action)
    sym       = r.get("symbol","?")
    full_name = r.get("full_name") or r.get("name", sym)
    isin      = r.get("isin","")
    exchange  = r.get("exchange","")
    market    = r.get("market","?")
    mkt_label = {"IT":"Italia 🇮🇹","EU":"Europa 🇪🇺","US":"USA 🇺🇸"}.get(market, market)
    atype     = {"stock":"Azione","etf":"ETF","index":"Indice"}.get(r.get("asset_type",""),"?")
    curr      = r.get("currency","")
    price     = r.get("price")
    conf      = r.get("confidence",0)
    score     = r.get("score",0)
    entry     = r.get("entry"); sl = r.get("stop_loss"); tp = r.get("take_profit"); rr = r.get("risk_reward")
    reasons   = r.get("reasons",[])
    ind       = r.get("indicators",{})
    bd        = r.get("score_breakdown",{})
    ai_sum    = r.get("ai_summary","")
    news      = r.get("news",[])

    conf_col  = "#16A34A" if conf>=70 else "#F59E0B" if conf>=50 else "#DC2626"
    conf_lbl  = "Alta" if conf>=70 else "Media" if conf>=50 else "Bassa"
    sc_str    = ("+" if score>=0 else "")+str(score)
    isin_str  = " · ISIN: "+isin if isin else ""
    exch_str  = " · "+exchange if exchange else ""

    # Livelli operativi
    livelli = ""
    if entry and sl and tp:
        rischio  = abs(entry - sl)
        guadagno = abs(tp - entry)
        livelli = (
            '<table style="width:100%;border-collapse:collapse;background:#0A0F1A;border-radius:6px;margin-top:12px">'
            '<tr>'
            '<td style="padding:10px 12px;text-align:center;border-right:1px solid #1F2937">'
            '<div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">PREZZO ENTRATA</div>'
            '<div style="font-size:16px;font-weight:700;color:#F9FAFB;font-family:monospace">'+curr+" "+str(entry)+'</div>'
            '</td>'
            '<td style="padding:10px 12px;text-align:center;border-right:1px solid #1F2937">'
            '<div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">STOP LOSS</div>'
            '<div style="font-size:16px;font-weight:700;color:#DC2626;font-family:monospace">'+curr+" "+str(sl)+'</div>'
            '<div style="font-size:10px;color:#6B7280;margin-top:2px">Rischio: '+f"{rischio:.2f}"+'</div>'
            '</td>'
            '<td style="padding:10px 12px;text-align:center;border-right:1px solid #1F2937">'
            '<div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">OBIETTIVO</div>'
            '<div style="font-size:16px;font-weight:700;color:#16A34A;font-family:monospace">'+curr+" "+str(tp)+'</div>'
            '<div style="font-size:10px;color:#6B7280;margin-top:2px">Guadagno: '+f"{guadagno:.2f}"+'</div>'
            '</td>'
            '<td style="padding:10px 12px;text-align:center">'
            '<div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">RISCHIO/RENDIMENTO</div>'
            '<div style="font-size:16px;font-weight:700;color:#F59E0B;font-family:monospace">1 : '+str(rr)+'</div>'
            '</td>'
            '</tr></table>'
        )

    motiv_lbl = "Perché ACQUISTARE" if action=="BUY" else "Perché VENDERE" if action=="SELL" else "Motivi del segnale"
    motiv_items = "".join('<li style="margin-bottom:6px;line-height:1.6">'+_tr(r2)+'</li>' for r2 in reasons)
    motiv = (
        '<div style="margin-top:14px">'
        '<div style="font-size:9px;color:#6B7280;letter-spacing:.1em;text-transform:uppercase;margin-bottom:8px">'+motiv_lbl+'</div>'
        '<ul style="margin:0;padding:0 0 0 18px;color:#D1D5DB;font-size:12px">'+motiv_items+'</ul>'
        '</div>'
    ) if motiv_items else ""

    ai_html = (
        '<div style="margin-top:12px;padding:10px 14px;background:#0A0F1A;border-left:3px solid #2563EB;border-radius:0 4px 4px 0">'
        '<div style="font-size:9px;color:#2563EB;letter-spacing:.1em;margin-bottom:4px">ANALISI AI</div>'
        '<div style="font-size:11px;color:#9CA3AF;line-height:1.7">'+ai_sum+'</div>'
        '</div>'
    ) if ai_sum else ""

    news_items = "".join(
        '<div style="padding:5px 0;border-bottom:1px solid #1F293722;font-size:11px;color:#9CA3AF">'
        '▸ '+n.get("headline","")+'<span style="color:#374151"> — '+n.get("source","")+'</span></div>'
        for n in news[:3]
    )
    news_html = (
        '<div style="margin-top:12px">'
        '<div style="font-size:9px;color:#6B7280;letter-spacing:.1em;margin-bottom:6px">ULTIME NOTIZIE</div>'
        + news_items + '</div>'
    ) if news_items else ""

    price_html = (
        '<div style="font-size:18px;font-weight:700;color:#F9FAFB;font-family:monospace">'+curr+" "+str(price)+'</div>'
    ) if price else ""

    parts = [
        '<div style="background:#111827;border:1px solid #1F2937;border-left:4px solid '+col+';border-radius:10px;margin-bottom:20px;overflow:hidden">',
        '<div style="padding:14px 18px;display:flex;justify-content:space-between;align-items:flex-start;background:'+col+'0D;border-bottom:1px solid #1F2937">',
        '<div>',
        '<div style="font-size:20px;font-weight:800;color:'+col+'">'+etiq+'</div>',
        '<div style="font-size:17px;font-weight:700;color:#F9FAFB;margin-top:4px">'+sym+'<span style="font-size:12px;font-weight:400;color:#9CA3AF;margin-left:8px">'+full_name+'</span></div>',
        '<div style="font-size:10px;color:#6B7280;margin-top:3px">'+mkt_label+' · '+atype+isin_str+exch_str+'</div>',
        '</div>',
        '<div style="text-align:right;flex-shrink:0;margin-left:16px">',
        price_html,
        '<div style="font-size:11px;color:#6B7280;margin-top:3px">Score: <span style="color:'+col+';font-weight:700">'+sc_str+'/100</span></div>',
        '</div></div>',
        '<div style="padding:8px 18px;background:#0D1420;border-bottom:1px solid #1F2937">',
        '<div style="display:flex;justify-content:space-between;margin-bottom:3px">',
        '<span style="font-size:10px;color:#6B7280;letter-spacing:.08em">CONFIDENZA SEGNALE</span>',
        '<span style="font-size:10px;font-weight:700;color:'+conf_col+'">'+str(conf)+'% — '+conf_lbl+'</span></div>',
        '<div style="height:5px;background:#1F2937;border-radius:3px">',
        '<div style="height:5px;width:'+str(conf)+'%;background:'+conf_col+';border-radius:3px"></div>',
        '</div></div>',
        '<div style="padding:14px 18px">',
        livelli,
        motiv,
        _score_breakdown_html(bd),
        _perf_section(ind, curr),
        _ind_section(ind),
        ai_html,
        news_html,
        '</div></div>',
    ]
    return "".join(parts)

def _card_hold(r: dict) -> str:
    sym    = r.get("symbol","?"); nm = r.get("full_name") or r.get("name",sym)
    action = r.get("action","HOLD"); score = r.get("score",0)
    price  = r.get("price"); curr = r.get("currency",""); ind = r.get("indicators",{})
    rsi    = f"RSI {ind['rsi']}" if ind.get("rsi") else ""; adx = f"ADX {ind['adx']}" if ind.get("adx") else ""
    reasons = [_tr(x) for x in r.get("reasons",[])[:2]]
    reason_str = " · ".join(reasons) if reasons else ""
    sc_str = ("+" if score>=0 else "")+str(score)
    etiq   = {"HOLD":"⚪ Neutro","NO_DATA":"⚫ Nessun dato"}.get(action, action)
    price_str = f"{curr} {price}" if price else "—"
    return (
        f'<tr style="border-bottom:1px solid #1F2937">'
        f'<td style="padding:7px 10px;color:#F9FAFB;font-weight:600;white-space:nowrap">{sym}</td>'
        f'<td style="padding:7px 10px;color:#9CA3AF;font-size:11px">{nm[:28]}</td>'
        f'<td style="padding:7px 10px;color:#6B7280;font-size:11px">{etiq}</td>'
        f'<td style="padding:7px 10px;font-family:monospace;font-size:11px;color:{"#16A34A" if score>=0 else "#DC2626"}">{sc_str}</td>'
        f'<td style="padding:7px 10px;color:#9CA3AF;font-size:11px">{" | ".join(filter(None,[rsi,adx]))}</td>'
        f'<td style="padding:7px 10px;color:#6B7280;font-size:11px;font-family:monospace">{price_str}</td>'
        f'<td style="padding:7px 10px;color:#6B7280;font-size:10px;max-width:200px">{reason_str}</td>'
        f'</tr>'
    )


# ── Build HTML report ─────────────────────────────────────────────────────────
def build_html_report(results: List[Dict], run_ts: str, next_ts: str, smart_money_data: Dict = None) -> str:
    run_dt  = run_ts[:19].replace("T"," ") if run_ts else "---"
    next_dt = next_ts[:19].replace("T"," ") if next_ts else "---"

    buy_l   = [r for r in results if r.get("action")=="BUY"]
    sell_l  = [r for r in results if r.get("action")=="SELL"]
    watch_l = [r for r in results if r.get("action")=="WATCHLIST"]
    hold_l  = [r for r in results if r.get("action") not in ("BUY","SELL","WATCHLIST")]

    # Riepilogo
    parts_rie = []
    if buy_l:   parts_rie.append(f'<span style="color:#16A34A;font-weight:700">🟢 {len(buy_l)} Acquist{"o" if len(buy_l)==1 else "i"}</span>')
    if sell_l:  parts_rie.append(f'<span style="color:#DC2626;font-weight:700">🔴 {len(sell_l)} Vendit{"a" if len(sell_l)==1 else "e"}</span>')
    if watch_l: parts_rie.append(f'<span style="color:#2563EB;font-weight:700">🔵 {len(watch_l)} Da osservare</span>')
    if hold_l:  parts_rie.append(f'<span style="color:#6B7280">⚪ {len(hold_l)} Neutri</span>')
    riepilogo = " &nbsp;·&nbsp; ".join(parts_rie) or "Nessun segnale attivo"

    def _section(lst, col, icon, label):
        if not lst: return ""
        cards = "".join(_card(r) for r in lst)
        return (
            f'<div style="margin-bottom:28px">'
            f'<div style="font-size:13px;font-weight:700;color:{col};padding:8px 14px;'
            f'background:{col}18;border-left:4px solid {col};border-radius:0 6px 6px 0;margin-bottom:14px">'
            f'{icon} {label} ({len(lst)})</div>'
            f'{cards}</div>'
        )

    hold_rows = "".join(_card_hold(r) for r in hold_l)
    hold_section = (
        f'<div style="margin-bottom:20px">'
        f'<div style="font-size:12px;font-weight:600;color:#6B7280;margin-bottom:8px">⚪ ALTRI ASSET MONITORATI ({len(hold_l)})</div>'
        f'<div style="overflow-x:auto">'
        f'<table style="width:100%;min-width:600px;border-collapse:collapse;background:#111827;'
        f'border:1px solid #1F2937;border-radius:8px;overflow:hidden">'
        f'<tr style="background:#0D1420">'
        + "".join(f'<th style="padding:6px 10px;color:#6B7280;font-size:9px;text-align:left;letter-spacing:.1em;font-weight:600">{h}</th>'
                  for h in ["SIMBOLO","NOME","STATO","SCORE","RSI / ADX","PREZZO","MOTIVO"])
        + f'</tr>{hold_rows}</table></div></div>'
    ) if hold_l else ""

    # Smart Money section — passa assets (results contiene isin) per ISIN lookup
    sm_section = ""
    if smart_money_data and _HAS_SM:
        sm_section = _sm_section(smart_money_data, assets=results) or ""

    n_disc = 0
    if smart_money_data and not smart_money_data.get("error"):
        n_disc = sum(1 for o in smart_money_data.get("opportunities",[]) if o.get("signal_type")=="Discovery")

    sm_riepilogo = ""
    if smart_money_data and not smart_money_data.get("error"):
        n_opp  = len(smart_money_data.get("opportunities",[]))
        qual   = smart_money_data.get("data_quality","?")
        qcol   = {"high":"#16A34A","medium":"#F59E0B","low":"#DC2626"}.get(qual,"#6B7280")
        sm_riepilogo = (
            f' &nbsp;·&nbsp; <span style="color:#A78BFA;font-weight:700">🏦 Smart Money: {n_opp} opportunità'
            + (f' · 🔭 {n_disc} Scoperte' if n_disc else "")
            + f' · Qualità <span style="color:{qcol}">{qual.upper()}</span></span>'
        )

    return (
        '<!DOCTYPE html><html lang="it"><head><meta charset="UTF-8"/></head>'
        '<body style="margin:0;padding:0;background:#0D1117;font-family:\'Segoe UI\',system-ui,sans-serif">'
        '<div style="max-width:740px;margin:0 auto;padding:20px 14px">'

        # Header
        '<div style="background:linear-gradient(135deg,#111827,#1F2937);border:1px solid #374151;'
        'border-radius:12px;padding:24px;margin-bottom:20px;text-align:center">'
        '<div style="font-size:26px;font-weight:800;color:#F9FAFB">⊕ Market Analyze</div>'
        '<div style="font-size:12px;color:#6B7280;margin-top:4px">ANALISI COMPLETA · SEGNALI QUANT + SMART MONEY</div>'
        '<div style="margin-top:12px;font-size:11px;color:#9CA3AF">'
        f'📅 {run_dt} &nbsp;·&nbsp; ⏭ Prossimo: {next_dt} &nbsp;·&nbsp; 📊 {len(results)} asset analizzati</div></div>'

        # Riepilogo unificato (segnali + SM)
        '<div style="background:#111827;border:1px solid #1F2937;border-radius:10px;'
        'padding:14px 20px;margin-bottom:22px;text-align:center">'
        '<div style="font-size:10px;color:#6B7280;letter-spacing:.1em;margin-bottom:8px">RIEPILOGO COMPLETO</div>'
        f'<div style="font-size:13px;line-height:2.2">{riepilogo}{sm_riepilogo}</div></div>'

        + _section(buy_l,   "#16A34A", "🟢", "SEGNALI DI ACQUISTO")
        + _section(sell_l,  "#DC2626", "🔴", "SEGNALI DI VENDITA")
        + _section(watch_l, "#2563EB", "🔵", "DA OSSERVARE")
        + hold_section
        + sm_section

        + '<div style="text-align:center;padding:16px;font-size:10px;color:#374151;'
        'margin-top:8px;border-top:1px solid #1F2937">'
        'Market Analyze · Home Assistant Add-on · Non costituisce consulenza finanziaria</div>'
        '</div></body></html>'
    )


# ── Entry point ────────────────────────────────────────────────────────────────
def send_report(results: List[Dict], run_ts: str, next_ts: str, cfg: Dict, smart_money_data: Dict = None) -> bool:
    if not cfg.get("email_enabled"): return False
    email_from, email_to = _resolve_sender_recipient(cfg)
    if not email_to or not email_from:
        log.warning("[MAILER] email_to o email_from mancanti"); return False

    min_score = int(cfg.get("email_min_score",40))
    active    = [r for r in results if r.get("action") in ("BUY","SELL","WATCHLIST")]
    mode      = str(cfg.get("email_signal_mode", "strong_only")).lower()
    if mode == "any_active":
        selected = active
    elif mode == "buy_sell_only":
        selected = [r for r in active if r.get("action") in ("BUY", "SELL")]
    else:
        selected = [r for r in active if abs(r.get("score",0)) >= min_score]
    if not selected:
        log.info(f"[MAILER] Nessun segnale compatibile con policy={mode} e soglia={min_score}")
        return False

    html_body = build_html_report(results, run_ts, next_ts, smart_money_data)

    n_buy  = sum(1 for r in active if r.get("action")=="BUY")
    n_sell = sum(1 for r in active if r.get("action")=="SELL")
    parts_s = []
    if n_buy:  parts_s.append(f"{n_buy} ACQUIST{'O' if n_buy==1 else 'I'}")
    if n_sell: parts_s.append(f"{n_sell} VENDIT{'A' if n_sell==1 else 'E'}")
    # Aggiunge info Smart Money al soggetto
    sm_suffix = ""
    if smart_money_data and not smart_money_data.get("error"):
        n_opp  = len(smart_money_data.get("opportunities", []))
        n_disc_subj = sum(1 for o in smart_money_data.get("opportunities",[]) if o.get("signal_type")=="Discovery")
        if n_opp:
            sm_suffix = f" · 🏦 SM: {n_opp} opp"
            if n_disc_subj:
                sm_suffix += f" ({n_disc_subj} scoperte)"
    subject = f"📊 Market Analyze — {', '.join(parts_s) or 'Nessun segnale'}{sm_suffix} — {run_ts[:10] if run_ts else ''}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject; msg["From"] = email_from; msg["To"] = email_to
    msg.attach(MIMEText(html_body,"html","utf-8"))
    return _dispatch_email(msg, cfg)
