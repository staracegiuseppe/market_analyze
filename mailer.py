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

try:
    from smart_money import build_email_section as _sm_section
    _HAS_SM = True
except ImportError:
    _HAS_SM = False

# ── OAuth2 ────────────────────────────────────────────────────────────────────
_tok: Dict = {"access_token": None, "expires_at": 0}

def _access_token(cid, csecret, rtoken):
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
            return _tok["access_token"]
        log.error(f"[OAUTH2] {r.status_code}: {r.text[:150]}")
    except Exception as e:
        log.error(f"[OAUTH2] {e}")
    return None

def _send_oauth2(msg, sender, recipient, cid, csecret, rtoken):
    token = _access_token(cid, csecret, rtoken)
    if not token: return False
    auth = base64.b64encode(f"user={sender}\x01auth=Bearer {token}\x01\x01".encode()).decode()
    try:
        s = smtplib.SMTP("smtp.gmail.com", 587, timeout=15)
        s.ehlo(); s.starttls(); s.ehlo()
        code, _ = s.docmd("AUTH", "XOAUTH2 " + auth)
        if code == 334: s.docmd("")
        s.sendmail(sender, recipient, msg.as_string()); s.quit()
        log.info("[OAUTH2] ✓"); return True
    except Exception as e:
        log.error(f"[OAUTH2] {e}"); return False

def _send_apppassword(msg, sender, recipient, host, port, user, pw, tls):
    try:
        s = smtplib.SMTP(host, port, timeout=15) if tls else smtplib.SMTP_SSL(host, port, timeout=15)
        if tls: s.ehlo(); s.starttls()
        s.login(user, pw)
        s.sendmail(sender, recipient, msg.as_string()); s.quit()
        log.info("[SMTP] ✓"); return True
    except smtplib.SMTPAuthenticationError:
        log.error("[SMTP] Auth fallita"); return False
    except Exception as e:
        log.error(f"[SMTP] {e}"); return False


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
    ai_align  = r.get("ai_enriched") and r.get("ai_news_bias","")
    news      = r.get("news",[])

    conf_col  = "#16A34A" if conf>=70 else "#F59E0B" if conf>=50 else "#DC2626"
    conf_lbl  = "Alta" if conf>=70 else "Media" if conf>=50 else "Bassa"
    sc_str    = ("+" if score>=0 else "")+str(score)
    isin_str  = f' · ISIN: {isin}' if isin else ""
    exch_str  = f' · {exchange}' if exchange else ""

    # Livelli operativi
    livelli = ""
    if entry and sl and tp:
        rischio  = abs(entry - sl)
        guadagno = abs(tp - entry)
        livelli = (
            '<table style="width:100%;border-collapse:collapse;background:#0A0F1A;border-radius:6px;margin-top:12px">'
            '<tr>'
            f'<td style="padding:10px 12px;text-align:center;border-right:1px solid #1F2937">'
            f'<div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">PREZZO ENTRATA</div>'
            f'<div style="font-size:16px;font-weight:700;color:#F9FAFB;font-family:monospace">{curr} {entry}</div>'
            f'</td>'
            f'<td style="padding:10px 12px;text-align:center;border-right:1px solid #1F2937">'
            f'<div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">STOP LOSS</div>'
            f'<div style="font-size:16px;font-weight:700;color:#DC2626;font-family:monospace">{curr} {sl}</div>'
            f'<div style="font-size:10px;color:#6B7280;margin-top:2px">Rischio: {rischio:.2f}</div>'
            f'</td>'
            f'<td style="padding:10px 12px;text-align:center;border-right:1px solid #1F2937">'
            f'<div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">OBIETTIVO</div>'
            f'<div style="font-size:16px;font-weight:700;color:#16A34A;font-family:monospace">{curr} {tp}</div>'
            f'<div style="font-size:10px;color:#6B7280;margin-top:2px">Guadagno: {guadagno:.2f}</div>'
            f'</td>'
            f'<td style="padding:10px 12px;text-align:center">'
            f'<div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">RISCHIO/RENDIMENTO</div>'
            f'<div style="font-size:16px;font-weight:700;color:#F59E0B;font-family:monospace">1 : {rr}</div>'
            f'</td>'
            '</tr></table>'
        )

    # Motivazioni tradotte
    motiv_lbl = "Perché ACQUISTARE" if action=="BUY" else "Perché VENDERE" if action=="SELL" else "Motivi del segnale"
    motiv_items = "".join(f'<li style="margin-bottom:6px;line-height:1.6">{_tr(r2)}</li>' for r2 in reasons)
    motiv = (
        f'<div style="margin-top:14px">'
        f'<div style="font-size:9px;color:#6B7280;letter-spacing:.1em;text-transform:uppercase;margin-bottom:8px">{motiv_lbl}</div>'
        f'<ul style="margin:0;padding:0 0 0 18px;color:#D1D5DB;font-size:12px">{motiv_items}</ul>'
        f'</div>'
    ) if motiv_items else ""

    # AI summary
    ai_html = ""
    if ai_sum:
        ai_html = (
            f'<div style="margin-top:12px;padding:10px 14px;background:#0A0F1A;border-left:3px solid #2563EB;border-radius:0 4px 4px 0">'
            f'<div style="font-size:9px;color:#2563EB;letter-spacing:.1em;margin-bottom:4px">ANALISI AI</div>'
            f'<div style="font-size:11px;color:#9CA3AF;line-height:1.7">{ai_sum}</div>'
            f'</div>'
        )

    # News
    news_html = ""
    if news:
        items_n = "".join(
            f'<div style="padding:5px 0;border-bottom:1px solid #1F293722;font-size:11px;color:#9CA3AF">'
            f'▸ {n.get("headline","")} <span style="color:#374151">— {n.get("source","")}</span></div>'
            for n in news[:3]
        )
        news_html = (
            f'<div style="margin-top:12px">'
            f'<div style="font-size:9px;color:#6B7280;letter-spacing:.1em;margin-bottom:6px">ULTIME NOTIZIE</div>'
            f'{items_n}</div>'
        )

    return (
        f'<div style="background:#111827;border:1px solid #1F2937;border-left:4px solid {col};'
        f'border-radius:10px;margin-bottom:20px;overflow:hidden">'

        # Header
        f'<div style="padding:14px 18px;display:flex;justify-content:space-between;align-items:flex-start;'
        f'background:{col}0D;border-bottom:1px solid #1F2937">'
        f'<div>'
        f'<div style="font-size:20px;font-weight:800;color:{col}">{etiq}</div>'
        f'<div style="font-size:17px;font-weight:700;color:#F9FAFB;margin-top:4px">{sym}'
        f'<span style="font-size:12px;font-weight:400;color:#9CA3AF;margin-left:8px">{full_name}</span></div>'
        f'<div style="font-size:10px;color:#6B7280;margin-top:3px">{mkt_label} · {atype}{isin_str}{exch_str}</div>'
        f'</div>'
        f'<div style="text-align:right;flex-shrink:0;margin-left:16px">'
        f'{f"<div style=\\"font-size:18px;font-weight:700;color:#F9FAFB;font-family:monospace\\">{curr} {price}</div>" if price else ""}'
        f'<div style="font-size:11px;color:#6B7280;margin-top:3px">Score: <span style="color:{col};font-weight:700">{sc_str}/100</span></div>'
        f'</div></div>'

        # Confidenza
        f'<div style="padding:8px 18px;background:#0D1420;border-bottom:1px solid #1F2937">'
        f'<div style="display:flex;justify-content:space-between;margin-bottom:3px">'
        f'<span style="font-size:10px;color:#6B7280;letter-spacing:.08em">CONFIDENZA SEGNALE</span>'
        f'<span style="font-size:10px;font-weight:700;color:{conf_col}">{conf}% — {conf_lbl}</span></div>'
        f'<div style="height:5px;background:#1F2937;border-radius:3px">'
        f'<div style="height:5px;width:{conf}%;background:{conf_col};border-radius:3px"></div>'
        f'</div></div>'

        # Body
        f'<div style="padding:14px 18px">'
        + livelli + motiv
        + _score_breakdown_html(bd)
        + _perf_section(ind, curr)
        + _ind_section(ind)
        + ai_html + news_html
        + f'</div></div>'
    )


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
        f'<td style="padding:7px 10px;color:#6B7280;font-size:10px;max-width:200px">{reason_str}<