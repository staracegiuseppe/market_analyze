# mailer.py v3.0
# Gmail OAuth2 (XOAUTH2) come metodo primario.
# Fallback automatico a App Password se oauth2_refresh_token non configurato.
#
# OAuth2 flow:
#   client_id + client_secret + refresh_token → Google token endpoint
#   → access_token (durata 1h) → SMTP XOAUTH2 authentication
#
# Nessuna password Gmail nel config. Il refresh_token non scade mai
# (finché non viene revocato manualmente da Google Account).

import smtplib
import base64
import logging
import json
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from datetime             import datetime
from typing               import List, Dict, Any, Optional
import requests as _requests

log = logging.getLogger("mailer")

# ── Token cache in memoria ────────────────────────────────────────────────────
_token_cache: Dict[str, Any] = {"access_token": None, "expires_at": 0}


def _get_access_token(client_id: str, client_secret: str, refresh_token: str) -> Optional[str]:
    """
    Scambia il refresh_token con un access_token fresco.
    Cache in memoria: il token viene riutilizzato finché non scade (3600s).
    """
    now = time.time()
    if _token_cache["access_token"] and _token_cache["expires_at"] > now + 60:
        log.debug("[OAUTH2] access_token dalla cache (scade tra %.0fs)" %
                  (_token_cache["expires_at"] - now))
        return _token_cache["access_token"]

    log.info("[OAUTH2] Richiesta nuovo access_token a Google...")
    try:
        r = _requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id":     client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type":    "refresh_token",
            },
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            token = data["access_token"]
            expires_in = int(data.get("expires_in", 3600))
            _token_cache["access_token"] = token
            _token_cache["expires_at"]   = now + expires_in
            log.info(f"[OAUTH2] access_token OK (valido {expires_in}s)")
            return token
        else:
            log.error(f"[OAUTH2] Errore token endpoint: HTTP {r.status_code} — {r.text[:200]}")
            return None
    except Exception as e:
        log.error(f"[OAUTH2] Eccezione token endpoint: {e}")
        return None


def _xoauth2_string(user_email: str, access_token: str) -> str:
    """Costruisce la stringa XOAUTH2 per SMTP AUTH."""
    raw = f"user={user_email}\x01auth=Bearer {access_token}\x01\x01"
    return base64.b64encode(raw.encode()).decode()


def _send_oauth2(
    msg: MIMEMultipart,
    sender: str,
    recipient: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> bool:
    """Invia via SMTP con autenticazione XOAUTH2."""
    access_token = _get_access_token(client_id, client_secret, refresh_token)
    if not access_token:
        log.error("[OAUTH2] Impossibile ottenere access_token — email non inviata")
        return False

    auth_string = _xoauth2_string(sender, access_token)
    try:
        log.info(f"[OAUTH2] Connessione SMTP smtp.gmail.com:587...")
        srv = smtplib.SMTP("smtp.gmail.com", 587, timeout=15)
        srv.ehlo()
        srv.starttls()
        srv.ehlo()
        # XOAUTH2: autentica senza password
        code, response = srv.docmd("AUTH", "XOAUTH2 " + auth_string)
        if code not in (235, 334):
            # Alcuni server restituiscono 334 con challenge — rispondiamo vuoto
            if code == 334:
                srv.docmd("")
        srv.sendmail(sender, recipient, msg.as_string())
        srv.quit()
        log.info("[OAUTH2] ✓ Email inviata con OAuth2")
        return True
    except smtplib.SMTPAuthenticationError as e:
        log.error(f"[OAUTH2] Auth SMTP fallita: {e}")
        log.error("[OAUTH2] Verifica che il refresh_token sia valido e che l'account abbia Gmail API abilitata")
        return False
    except Exception as e:
        log.error(f"[OAUTH2] Errore SMTP: {e}")
        return False


def _send_app_password(
    msg: MIMEMultipart,
    sender: str,
    recipient: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    smtp_tls: bool,
) -> bool:
    """Fallback: invia via SMTP con App Password."""
    try:
        log.info(f"[SMTP] Connessione {smtp_host}:{smtp_port} (tls={smtp_tls})...")
        if smtp_tls:
            srv = smtplib.SMTP(smtp_host, smtp_port, timeout=15)
            srv.ehlo(); srv.starttls()
        else:
            srv = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=15)
        srv.login(smtp_user, smtp_password)
        srv.sendmail(sender, recipient, msg.as_string())
        srv.quit()
        log.info("[SMTP] ✓ Email inviata con App Password")
        return True
    except smtplib.SMTPAuthenticationError:
        log.error("[SMTP] Auth fallita — verifica App Password (16 caratteri senza spazi)")
        return False
    except Exception as e:
        log.error(f"[SMTP] Errore: {e}")
        return False


# ── HTML report builder ───────────────────────────────────────────────────────

def _action_color(action):
    return {"BUY": "#18B85A", "SELL": "#E03838", "WATCHLIST": "#3D8EF0"}.get(action, "#C8A020")


def _card(r):
    action  = r.get("action", "HOLD")
    col     = _action_color(action)
    score   = r.get("score", 0)
    conf    = r.get("confidence", 0)
    sym     = r.get("symbol", "?")
    nm      = r.get("full_name") or r.get("name", sym)
    isin    = r.get("isin", "")
    market  = r.get("market", "?")
    atype   = r.get("asset_type", "?")
    price   = r.get("price", "?")
    curr    = r.get("currency", "")
    rr      = r.get("risk_reward")
    entry   = r.get("entry")
    sl      = r.get("stop_loss")
    tp2     = r.get("take_profit")
    summary = r.get("ai_summary", "")
    reasons = r.get("reasons", [])
    ind     = r.get("indicators", {})
    news    = r.get("news", [])

    isin_str = ('<div style="font-size:9px;color:#555;margin-top:1px">ISIN: ' + isin + '</div>') if isin else ""

    levels = ""
    if entry and sl and tp2:
        levels = (
            '<div style="font-size:10px;color:#888;margin-top:6px;font-family:monospace">'
            "Entry=" + str(entry) + "  SL=" + str(sl) + "  TP=" + str(tp2)
            + ("  R:R=1:" + str(rr) if rr else "") + "</div>"
        )

    ind_str = ""
    if ind:
        ind_str = (
            '<div style="font-size:10px;color:#888;margin-top:6px">'
            "RSI=" + str(ind.get("rsi", "?"))
            + "  ADX=" + str(ind.get("adx", "?"))
            + "  MACD=" + str(ind.get("macd_hist", "?"))
            + "  OBV=" + str(ind.get("obv_trend", "?"))
            + "  Vol=" + str(ind.get("vol_signal", "?"))
            + "</div>"
        )

    news_str = "".join(
        '<div style="font-size:10px;color:#6880A0;margin-top:3px">▸ '
        + n.get("headline", "") + '</div>'
        for n in news[:2]
    )

    reasons_str = "".join(
        '<div style="font-size:10px;color:#7A90B0;margin-top:2px">+ ' + rr2 + '</div>'
        for rr2 in reasons[:3]
    )

    summary_str = (
        '<div style="font-size:11px;color:#B0C4DE;margin-top:8px;border-left:2px solid '
        + col + ';padding-left:8px">' + summary + '</div>'
    ) if summary else ""

    sc_str = ("+" if score >= 0 else "") + str(score)

    return (
        '<div style="background:#0F1520;border:1px solid #1E2D45;border-left:4px solid '
        + col + ';border-radius:6px;padding:14px;margin-bottom:10px">'

        '<div style="display:flex;justify-content:space-between;align-items:flex-start">'
        '<div>'
        '<b style="color:#D0DFF8;font-size:14px">' + sym + '</b>'
        '<span style="color:#6880A0;font-size:11px;margin-left:8px">' + nm + '</span>'
        + isin_str +
        '<div style="font-size:9px;color:#6880A0;margin-top:2px">'
        + market + ' · ' + atype + ' · ' + curr
        + '</div></div>'

        '<div style="text-align:right">'
        '<span style="color:' + col + ';font-size:20px;font-weight:700">' + action + '</span><br>'
        '<span style="color:' + col + ';font-size:11px">' + str(conf) + '%</span>'
        '<span style="color:#6880A0;font-size:11px;margin-left:8px">score=' + sc_str + '</span>'
        + (' <span style="color:#888;font-size:10px">' + curr + ' ' + str(price) + '</span>' if price else '')
        + '</div></div>'

        + ind_str + levels + summary_str + reasons_str + news_str
        + '</div>'
    )


def build_html_report(results: List[Dict], run_ts: str, next_ts: str) -> str:
    run_dt  = run_ts[:19].replace("T", " ") if run_ts else "---"
    next_dt = next_ts[:19].replace("T", " ") if next_ts else "---"
    active  = [r for r in results if r.get("action") in ("BUY", "SELL", "WATCHLIST")]
    passive = [r for r in results if r.get("action") not in ("BUY", "SELL", "WATCHLIST")]

    # Summary row
    summary_rows = ""
    for r in active:
        col = _action_color(r.get("action", "HOLD"))
        sc  = r.get("score", 0)
        rr  = r.get("risk_reward")
        summary_rows += (
            '<div style="display:flex;justify-content:space-between;padding:5px 0;'
            'border-bottom:1px solid #192030;font-size:11px">'
            '<span style="color:#D0DFF8">' + r.get("symbol","?")
            + ' <span style="color:#6880A0;font-size:10px">— ' + (r.get("name",""))[:22] + '</span></span>'
            '<span style="color:' + col + ';font-family:monospace">'
            + r.get("action","") + ' ' + str(r.get("confidence","")) + '%'
            + (' R:R=1:' + str(rr) if rr else '') + '</span></div>'
        )

    passive_rows = "".join(
        '<div style="display:flex;justify-content:space-between;padding:4px 0;'
        'border-bottom:1px solid #19203022;font-size:10px">'
        '<span style="color:#7A90B0">' + r.get("symbol","?") + ' — ' + r.get("name","")[:20] + '</span>'
        '<span style="color:#6880A0">' + r.get("action","HOLD") + ' ' + str(r.get("score",0)) + '</span></div>'
        for r in passive[:8]
    )

    active_cards = "".join(_card(r) for r in active)

    parts = []
    parts.append("<!DOCTYPE html><html><head><meta charset='UTF-8'/></head>")
    parts.append("<body style='margin:0;padding:0;background:#07090D;font-family:Segoe UI,sans-serif'>")
    parts.append("<div style='max-width:680px;margin:0 auto;padding:20px'>")

    # Header
    parts.append(
        "<div style='background:linear-gradient(135deg,#0B0E16,#111827);border:1px solid #1E2D45;"
        "border-radius:8px;padding:24px;margin-bottom:20px;text-align:center'>"
        "<div style='font-size:22px;font-weight:700;color:#D0DFF8'>⊕ Market Analyze</div>"
        "<div style='font-size:12px;color:#6880A0;margin-top:4px'>SIGNAL REPORT · QUANT + AI · v2.2</div>"
        "<div style='margin-top:10px;font-size:11px;color:#6880A0'>"
        "Generated: <b style='color:#D0DFF8'>" + run_dt + "</b>"
        "&nbsp;&nbsp;Next: <b style='color:#D0DFF8'>" + next_dt + "</b>"
        "&nbsp;&nbsp;Active: <b style='color:#C8A020'>" + str(len(active)) + "</b>"
        "</div></div>"
    )

    # Summary
    if summary_rows:
        parts.append(
            "<div style='background:#0B0E16;border:1px solid #1E2D45;border-radius:6px;"
            "padding:14px;margin-bottom:20px'>"
            "<div style='font-size:9px;color:#6880A0;letter-spacing:.1em;margin-bottom:8px'>ACTIVE SIGNALS</div>"
            + summary_rows + "</div>"
        )
    else:
        parts.append("<div style='padding:16px;text-align:center;color:#6880A0;font-size:12px'>No active signals</div>")

    # Active cards
    if active_cards:
        parts.append(
            "<div style='font-size:9px;color:#6880A0;letter-spacing:.1em;margin-bottom:8px'>SIGNAL DETAILS</div>"
            + active_cards
        )

    # Passive summary
    if passive_rows:
        parts.append(
            "<div style='background:#0B0E16;border:1px solid #1E2D45;border-radius:6px;"
            "padding:12px;margin-top:12px'>"
            "<div style='font-size:9px;color:#6880A0;letter-spacing:.1em;margin-bottom:8px'>OTHER ASSETS</div>"
            + passive_rows + "</div>"
        )

    # Footer
    parts.append(
        "<div style='text-align:center;padding:14px;font-size:10px;color:#444;margin-top:8px'>"
        "Market Analyze Add-on · Home Assistant · Not financial advice"
        "</div></div></body></html>"
    )

    return "".join(parts)


# ── Entry point pubblico ──────────────────────────────────────────────────────

def send_report(results: List[Dict], run_ts: str, next_ts: str, cfg: Dict) -> bool:
    """
    Invia il report email.
    Priorità: OAuth2 (client_id + client_secret + refresh_token)
    Fallback:  App Password (smtp_user + smtp_password)
    """
    if not cfg.get("email_enabled"):
        return False

    email_to   = cfg.get("email_to", "")
    email_from = cfg.get("email_from", "")
    if not email_to or not email_from:
        log.warning("[MAILER] email_to o email_from mancanti")
        return False

    min_score = int(cfg.get("email_min_score", 40))
    active    = [r for r in results if r.get("action") in ("BUY","SELL","WATCHLIST")]
    strong    = [r for r in active if abs(r.get("score", 0)) >= min_score]
    if not strong:
        log.info(f"[MAILER] Nessun segnale con |score| >= {min_score} — email non inviata")
        return False

    html_body = build_html_report(results, run_ts, next_ts)

    n_buy  = sum(1 for r in active if r.get("action") == "BUY")
    n_sell = sum(1 for r in active if r.get("action") == "SELL")
    parts  = []
    if n_buy:  parts.append(f"{n_buy} BUY")
    if n_sell: parts.append(f"{n_sell} SELL")
    subject = "Market Analyze — " + (", ".join(parts) or "No signals") + " — " + (run_ts[:10] if run_ts else "")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = email_from
    msg["To"]      = email_to
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    # ── Metodo 1: OAuth2 ──────────────────────────────────────────────────────
    client_id     = cfg.get("oauth2_client_id", "")
    client_secret = cfg.get("oauth2_client_secret", "")
    refresh_token = cfg.get("oauth2_refresh_token", "")

    if client_id and client_secret and refresh_token:
        log.info("[MAILER] Tentativo invio con OAuth2...")
        ok = _send_oauth2(msg, email_from, email_to, client_id, client_secret, refresh_token)
        if ok:
            return True
        log.warning("[MAILER] OAuth2 fallito — provo fallback App Password")

    # ── Metodo 2: App Password (fallback) ─────────────────────────────────────
    smtp_user = cfg.get("smtp_user", "")
    smtp_pass = cfg.get("smtp_password", "")
    if smtp_user and smtp_pass:
        log.info("[MAILER] Tentativo invio con App Password...")
        return _send_app_password(
            msg, email_from, email_to,
            cfg.get("smtp_host", "smtp.gmail.com"),
            int(cfg.get("smtp_port", 587)),
            smtp_user, smtp_pass,
            bool(cfg.get("smtp_tls", True)),
        )

    log.error("[MAILER] Nessun metodo di autenticazione configurato (OAuth2 o App Password)")
    return False
