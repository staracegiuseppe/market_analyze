# mailer.py v2.0 - Multi-market HTML email report. No f-string nesting.
import smtplib, logging
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from datetime             import datetime
from typing               import List, Dict, Any

log = logging.getLogger("mailer")


def _col(action):
    return {"BUY":"#18B85A","SELL":"#E03838","WATCHLIST":"#3D8EF0"}.get(action,"#C8A020")

def _card(r):
    action  = r.get("action","HOLD"); col = _col(action)
    score   = r.get("score",0); conf = r.get("confidence",0)
    sym     = r.get("symbol","?"); nm  = r.get("name",sym)
    market  = r.get("market","?"); tp  = r.get("asset_type","?")
    price   = r.get("price","?"); rr   = r.get("risk_reward")
    entry   = r.get("entry"); sl = r.get("stop_loss"); tp2 = r.get("take_profit")
    summary = r.get("ai_summary",""); reasons = r.get("reasons",[]); news = r.get("news",[])
    ind     = r.get("indicators",{})

    levels = ""
    if entry and sl and tp2:
        levels = ('<div style="font-size:10px;color:#888;margin-top:6px">'
                  "Entry=" + str(entry) + "  SL=" + str(sl) + "  TP=" + str(tp2)
                  + ("  R:R=1:" + str(rr) if rr else "") + "</div>")

    news_html = ""
    for n in news[:2]:
        news_html += ('<div style="font-size:10px;color:#6880A0;margin-top:3px">▸ '
                      + n.get("headline","") + '</div>')

    ind_html = ""
    if ind:
        ind_html = ('<div style="font-size:10px;color:#888;margin-top:6px">'
                    "RSI=" + str(ind.get("rsi","?"))
                    + "  ADX=" + str(ind.get("adx","?"))
                    + "  MACD=" + str(ind.get("macd_hist","?"))
                    + "  OBV=" + str(ind.get("obv_trend","?"))
                    + "</div>")

    return (
        '<div style="background:#0F1520;border:1px solid #1E2D45;border-left:4px solid '
        + col + ';border-radius:6px;padding:14px;margin-bottom:10px">'
        '<div style="display:flex;justify-content:space-between">'
        '<div><b style="color:#D0DFF8;font-size:14px">' + sym + '</b>'
        '<span style="color:#6880A0;font-size:11px;margin-left:8px">' + nm + '</span>'
        '<span style="background:#192030;color:#6880A0;font-size:9px;padding:2px 6px;'
        'margin-left:6px;border-radius:3px">' + market + ' · ' + tp + '</span></div>'
        '<div><span style="color:' + col + ';font-size:20px;font-weight:700;font-family:monospace">'
        + action + '</span>'
        '<span style="color:' + col + ';font-size:11px;margin-left:8px">' + str(conf) + '%</span>'
        '<span style="color:#888;font-size:11px;margin-left:8px">score=' + ("+" if score>=0 else "") + str(score) + '</span>'
        '</div></div>'
        + ind_html + levels
        + ('<div style="font-size:11px;color:#B0C4DE;margin-top:8px;border-left:2px solid '
           + col + ';padding-left:8px">' + summary + '</div>' if summary else '')
        + (''.join('<div style="font-size:10px;color:#7A90B0;margin-top:3px">+ ' + r2 + '</div>'
                   for r2 in reasons[:3]))
        + news_html
        + '</div>'
    )


def build_html_report(results, run_ts, next_ts):
    run_dt  = (run_ts[:19].replace("T"," ")) if run_ts else "---"
    next_dt = (next_ts[:19].replace("T"," ")) if next_ts else "---"
    active  = [r for r in results if r.get("action") in ("BUY","SELL","WATCHLIST")]
    inactive= [r for r in results if r.get("action") not in ("BUY","SELL","WATCHLIST")]

    cards_active = "".join(_card(r) for r in active)
    cards_hold   = ""
    for r in inactive[:5]:
        sym=r.get("symbol","?"); score=r.get("score",0); action=r.get("action","HOLD")
        cards_hold += ('<div style="display:flex;justify-content:space-between;padding:5px 0;'
                       'border-bottom:1px solid #192030;font-size:11px">'
                       '<span style="color:#D0DFF8">' + sym + ' — ' + r.get("name",sym)[:25] + '</span>'
                       '<span style="color:#6880A0">' + action
                       + ' score=' + ("+" if score>=0 else "") + str(score) + '</span></div>')

    return (
        "<!DOCTYPE html><html><head><meta charset='UTF-8'/></head>"
        "<body style='margin:0;padding:0;background:#07090D;font-family:Segoe UI,sans-serif'>"
        "<div style='max-width:680px;margin:0 auto;padding:20px'>"
        "<div style='background:#0B0E16;border:1px solid #1E2D45;border-radius:8px;padding:24px;"
        "margin-bottom:20px;text-align:center'>"
        "<div style='font-size:22px;font-weight:700;color:#D0DFF8'>Multi-Market Scanner</div>"
        "<div style='font-size:12px;color:#6880A0;margin-top:4px'>SIGNAL REPORT · QUANT + AI</div>"
        "<div style='margin-top:10px;font-size:11px;color:#6880A0'>"
        "Generated: <b style='color:#D0DFF8'>" + run_dt + "</b>"
        "&nbsp; Next: <b style='color:#D0DFF8'>" + next_dt + "</b>"
        "&nbsp; Active: <b style='color:#C8A020'>" + str(len(active)) + "</b>"
        "</div></div>"
        + ("<div style='font-size:10px;color:#6880A0;letter-spacing:.1em;margin-bottom:8px'>"
           "ACTIVE SIGNALS</div>" + cards_active if active else
           "<div style='padding:20px;text-align:center;color:#6880A0'>No active signals</div>")
        + ("<div style='background:#0B0E16;border:1px solid #1E2D45;border-radius:6px;"
           "padding:14px;margin-top:12px'>"
           "<div style='font-size:10px;color:#6880A0;margin-bottom:8px'>OTHER ASSETS</div>"
           + cards_hold + "</div>" if cards_hold else "")
        + "<div style='text-align:center;padding:12px;font-size:10px;color:#555;margin-top:8px'>"
          "Multi-Market Scanner · Home Assistant Add-on · Not financial advice</div>"
        + "</div></body></html>"
    )


def send_report(results, run_ts, next_ts, cfg) -> bool:
    if not cfg.get("email_enabled"): return False
    required = ["email_to","email_from","smtp_host","smtp_user","smtp_password"]
    if any(not cfg.get(k) for k in required):
        log.warning("Email skip: missing fields")
        return False

    active = [r for r in results if r.get("action") in ("BUY","SELL","WATCHLIST")]
    min_sc = int(cfg.get("email_min_score",40))
    strong = [r for r in active if abs(r.get("score",0)) >= min_sc]
    if not strong:
        log.info("Email skip: no signal above min_score")
        return False

    html_body = build_html_report(results, run_ts, next_ts)
    n_buy  = sum(1 for r in active if r.get("action")=="BUY")
    n_sell = sum(1 for r in active if r.get("action")=="SELL")
    parts  = []
    if n_buy:  parts.append(str(n_buy)+" BUY")
    if n_sell: parts.append(str(n_sell)+" SELL")
    subject = "Scanner — " + (", ".join(parts) or "No signals") + " — " + (run_ts[:10] if run_ts else "")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject; msg["From"] = cfg["email_from"]; msg["To"] = cfg["email_to"]
    msg.attach(MIMEText(html_body,"html","utf-8"))

    try:
        if cfg.get("smtp_tls",True):
            srv = smtplib.SMTP(cfg["smtp_host"],int(cfg.get("smtp_port",587)),timeout=15)
            srv.ehlo(); srv.starttls()
        else:
            srv = smtplib.SMTP_SSL(cfg["smtp_host"],int(cfg.get("smtp_port",465)),timeout=15)
        srv.login(cfg["smtp_user"],cfg["smtp_password"])
        srv.sendmail(cfg["email_from"],cfg["email_to"],msg.as_string())
        srv.quit()
        log.info("Email sent OK")
        return True
    except smtplib.SMTPAuthenticationError:
        log.error("SMTP auth failed"); return False
    except Exception as e:
        log.error("Email error: "+str(e)); return False
