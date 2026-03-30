# mailer.py v4.0 - Report email in italiano, layout chiaro, motivazioni tradotte
import smtplib, base64, logging, time, json
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from datetime             import datetime
from typing               import List, Dict, Any, Optional
import requests as _req

log = logging.getLogger("mailer")

# ── Token cache OAuth2 ────────────────────────────────────────────────────────
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
    auth  = base64.b64encode(f"user={sender}\x01auth=Bearer {token}\x01\x01".encode()).decode()
    try:
        s = smtplib.SMTP("smtp.gmail.com", 587, timeout=15)
        s.ehlo(); s.starttls(); s.ehlo()
        code, _ = s.docmd("AUTH", "XOAUTH2 " + auth)
        if code == 334: s.docmd("")
        s.sendmail(sender, recipient, msg.as_string()); s.quit()
        log.info("[OAUTH2] Email inviata OK"); return True
    except Exception as e:
        log.error(f"[OAUTH2] SMTP: {e}"); return False

def _send_apppassword(msg, sender, recipient, host, port, user, pw, tls):
    try:
        if tls:
            s = smtplib.SMTP(host, port, timeout=15); s.ehlo(); s.starttls()
        else:
            s = smtplib.SMTP_SSL(host, port, timeout=15)
        s.login(user, pw)
        s.sendmail(sender, recipient, msg.as_string()); s.quit()
        log.info("[SMTP] Email inviata OK"); return True
    except smtplib.SMTPAuthenticationError:
        log.error("[SMTP] Autenticazione fallita"); return False
    except Exception as e:
        log.error(f"[SMTP] {e}"); return False


# ── Traduzione motivazioni ─────────────────────────────────────────────────────
_TRADUZIONI = {
    "price>MA20>MA50":           "Prezzo sopra media 20 e 50 giorni → trend rialzista",
    "price<MA20<MA50":           "Prezzo sotto media 20 e 50 giorni → trend ribassista",
    "golden_cross":              "Golden Cross: media 20 supera media 50 → segnale rialzista forte",
    "death_cross":               "Death Cross: media 20 scende sotto media 50 → segnale ribassista forte",
    "above MA200":               "Prezzo sopra media 200 giorni → trend di lungo periodo positivo",
    "below MA200":               "Prezzo sotto media 200 giorni → trend di lungo periodo negativo",
    "MACD hist>0":               "MACD positivo → momentum in crescita",
    "MACD hist<0":               "MACD negativo → momentum in calo",
    "MACD bullish crossover":    "Incrocio MACD rialzista → cambio di direzione verso l'alto",
    "MACD bearish crossover":    "Incrocio MACD ribassista → cambio di direzione verso il basso",
    "RSI constructive":          "RSI in zona costruttiva (40-65) → forza senza eccessi",
    "RSI recovering":            "RSI in recupero → uscita dalla zona di debolezza",
    "RSI overbought":            "RSI in ipercomprato (>70) → rischio correzione a breve",
    "RSI oversold":              "RSI in ipervenduto (<30) → possibile rimbalzo tecnico",
    "RSI weak":                  "RSI debole (<35) → pressione vendita persistente",
    "ADX":                       "ADX elevato → trend forte e direzionale",
    "OBV bullish":               "Volume in accumulo (OBV crescente) → acquisti istituzionali",
    "OBV bearish":               "Volume in distribuzione (OBV calante) → vendite istituzionali",
    "BB oversold zone":          "Prezzo nella banda bassa di Bollinger → zona di supporto",
    "BB overbought":             "Prezzo nella banda alta di Bollinger → zona di resistenza",
    "high volume confirms":      "Volume elevato conferma il movimento",
    "ROC10":                     "Momentum positivo a 10 giorni",
}

def _traduci(reason: str) -> str:
    for key, trad in _TRADUZIONI.items():
        if key in reason:
            # Estrai valori numerici dal motivo originale
            import re
            nums = re.findall(r'[-+]?\d+\.?\d*', reason)
            if nums and "%" not in trad:
                return trad + f" ({nums[0]})"
            return trad
    return reason  # se non trovata, restituisce l'originale


def _indicatore_label(key: str, val) -> tuple:
    """Ritorna (etichetta_it, valore_formattato, commento_it)"""
    if key == "rsi":
        comm = "Ipercomprato" if val > 70 else "Ipervenduto" if val < 30 else "Neutrale"
        return "RSI (14)", f"{val}", comm
    if key == "adx":
        comm = "Trend forte" if val > 25 else "Trend debole"
        return "ADX (14)", f"{val}", comm
    if key == "macd_hist":
        comm = "Momentum positivo" if val > 0 else "Momentum negativo"
        return "MACD Istogramma", f"{val:+.4f}", comm
    if key == "obv_trend":
        comm = "Accumulo" if val == "bullish" else "Distribuzione"
        return "OBV Trend", "Rialzista" if val == "bullish" else "Ribassista", comm
    if key == "bb_pos":
        comm = "Zona alta" if val > 70 else "Zona bassa" if val < 30 else "Zona centrale"
        return "Posizione Bollinger", f"{val:.0f}%", comm
    if key == "stoch_k":
        comm = "Ipercomprato" if val > 80 else "Ipervenduto" if val < 20 else "Neutrale"
        return "Stocastico K", f"{val}", comm
    if key == "vol_signal":
        labels = {"HIGH": "Alto", "LOW": "Basso", "NORMAL": "Normale"}
        return "Volume", labels.get(val, val), ""
    if key == "roc10":
        comm = "Momentum positivo" if val > 0 else "Momentum negativo"
        return "ROC 10 giorni", f"{val:+.1f}%", comm
    if key == "atr_regime":
        labels = {"HIGH_VOL": "Alta volatilità", "LOW_VOL": "Bassa volatilità", "NORMAL_VOL": "Volatilità normale"}
        return "Regime volatilità", labels.get(val, val), ""
    if key == "ma_cross":
        labels = {
            "golden_cross":     "Golden Cross ↑",
            "death_cross":      "Death Cross ↓",
            "ma20_above_ma50":  "MA20 sopra MA50",
            "ma20_below_ma50":  "MA20 sotto MA50",
        }
        return "Incrocio medie", labels.get(val, val), ""
    if key == "support":
        return "Supporto", f"{val}", "Livello di acquisto atteso"
    if key == "resistance":
        return "Resistenza", f"{val}", "Livello di vendita atteso"
    if key == "atr":
        return "ATR (volatilità media)", f"{val}", ""
    if key == "ma20":
        return "Media mobile 20gg", f"{val}", ""
    if key == "ma50":
        return "Media mobile 50gg", f"{val}", ""
    if key == "ma200":
        return "Media mobile 200gg", f"{val}", ""
    return key, str(val), ""


# ── HTML builder ─────────────────────────────────────────────────────────────

def _colore_azione(action):
    return {"BUY": "#16A34A", "SELL": "#DC2626", "WATCHLIST": "#2563EB"}.get(action, "#6B7280")

def _etichetta_azione(action):
    return {"BUY": "🟢 ACQUISTO", "SELL": "🔴 VENDITA", "WATCHLIST": "🔵 OSSERVATO", "HOLD": "⚪ NEUTRO"}.get(action, action)

def _score_breakdown_it(breakdown: dict) -> str:
    nomi = {
        "ma_align":  "Allineamento medie",
        "ma_cross":  "Incrocio medie",
        "vs_ma200":  "Posizione vs MA200",
        "macd":      "MACD",
        "macd_cross":"Incrocio MACD",
        "rsi":       "RSI",
        "adx":       "ADX / Forza trend",
        "obv":       "OBV / Volume",
        "volume":    "Conferma volume",
        "bb":        "Bande di Bollinger",
        "roc":       "Momentum ROC",
    }
    rows = []
    for k, v in breakdown.items():
        if v == 0:
            continue
        nome  = nomi.get(k, k)
        color = "#16A34A" if v > 0 else "#DC2626"
        bar_w = min(100, abs(v) * 5)  # scala visiva
        sign  = "+" if v > 0 else ""
        rows.append(
            '<tr>'
            f'<td style="padding:4px 8px;color:#9CA3AF;font-size:11px;white-space:nowrap">{nome}</td>'
            f'<td style="padding:4px 8px;width:120px">'
            f'<div style="height:6px;background:#1F2937;border-radius:3px">'
            f'<div style="height:6px;width:{bar_w}%;background:{color};border-radius:3px"></div>'
            f'</div></td>'
            f'<td style="padding:4px 8px;color:{color};font-size:11px;font-family:monospace;text-align:right">{sign}{v}</td>'
            '</tr>'
        )
    if not rows:
        return ""
    return (
        '<table style="width:100%;border-collapse:collapse;margin-top:6px">'
        + "".join(rows)
        + '</table>'
    )


def _card_segnale(r: dict) -> str:
    """Genera la card HTML per un singolo segnale."""
    action    = r.get("action", "HOLD")
    col       = _colore_azione(action)
    etiq      = _etichetta_azione(action)
    sym       = r.get("symbol", "?")
    full_name = r.get("full_name") or r.get("name", sym)
    isin      = r.get("isin", "")
    market    = r.get("market", "?")
    atype     = r.get("asset_type", "?")
    curr      = r.get("currency", "")
    price     = r.get("price")
    conf      = r.get("confidence", 0)
    score     = r.get("score", 0)
    entry     = r.get("entry")
    sl        = r.get("stop_loss")
    tp        = r.get("take_profit")
    rr        = r.get("risk_reward")
    reasons   = r.get("reasons", [])
    ind       = r.get("indicators", {})
    breakdown = r.get("score_breakdown", {})
    ai_sum    = r.get("ai_summary", "")
    news      = r.get("news", [])

    # Traduci motivazioni
    motivazioni = [_traduci(r2) for r2 in reasons]

    # Tipo asset in italiano
    tipo_it = {"stock": "Azione", "etf": "ETF", "index": "Indice"}.get(atype, atype)
    mercato_it = {"IT": "Italia 🇮🇹", "EU": "Europa 🇪🇺", "US": "USA 🇺🇸"}.get(market, market)

    # Sezione livelli operativi
    livelli_html = ""
    if entry and sl and tp:
        rischio  = abs(entry - sl)
        guadagno = abs(tp - entry)
        livelli_html = f"""
        <table style="width:100%;border-collapse:collapse;margin-top:12px;background:#0A0F1A;border-radius:6px">
          <tr>
            <td style="padding:8px 12px;text-align:center;border-right:1px solid #1F2937;width:25%">
              <div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">PREZZO ENTRATA</div>
              <div style="font-size:16px;font-weight:700;color:#F9FAFB;font-family:monospace">{curr} {entry}</div>
            </td>
            <td style="padding:8px 12px;text-align:center;border-right:1px solid #1F2937;width:25%">
              <div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">STOP LOSS</div>
              <div style="font-size:16px;font-weight:700;color:#DC2626;font-family:monospace">{curr} {sl}</div>
              <div style="font-size:10px;color:#6B7280;margin-top:2px">Rischio: {curr} {rischio:.2f}</div>
            </td>
            <td style="padding:8px 12px;text-align:center;border-right:1px solid #1F2937;width:25%">
              <div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">OBIETTIVO</div>
              <div style="font-size:16px;font-weight:700;color:#16A34A;font-family:monospace">{curr} {tp}</div>
              <div style="font-size:10px;color:#6B7280;margin-top:2px">Guadagno: {curr} {guadagno:.2f}</div>
            </td>
            <td style="padding:8px 12px;text-align:center;width:25%">
              <div style="font-size:9px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:3px">RISCHIO/RENDIMENTO</div>
              <div style="font-size:16px;font-weight:700;color:#F59E0B;font-family:monospace">1 : {rr}</div>
            </td>
          </tr>
        </table>"""

    # Sezione motivazioni
    motiv_html = ""
    if motivazioni:
        items = "".join(
            f'<li style="margin-bottom:6px;padding-left:4px">{m}</li>'
            for m in motivazioni
        )
        lbl = "Perché ACQUISTARE" if action == "BUY" else "Perché VENDERE" if action == "SELL" else "Motivi del segnale"
        motiv_html = f"""
        <div style="margin-top:14px">
          <div style="font-size:10px;color:#9CA3AF;letter-spacing:.1em;text-transform:uppercase;margin-bottom:8px">{lbl}</div>
          <ul style="margin:0;padding:0 0 0 18px;color:#D1D5DB;font-size:12px;line-height:1.8">
            {items}
          </ul>
        </div>"""

    # Score breakdown
    bd_html = ""
    bd = _score_breakdown_it(breakdown)
    if bd:
        bd_html = f"""
        <div style="margin-top:14px">
          <div style="font-size:10px;color:#9CA3AF;letter-spacing:.1em;text-transform:uppercase;margin-bottom:6px">Punteggio indicatori</div>
          {bd}
        </div>"""

    # Indicatori tecnici principali
    ind_html = ""
    chiavi_mostra = ["rsi", "adx", "macd_hist", "obv_trend", "bb_pos", "stoch_k", "roc10", "vol_signal", "atr_regime", "support", "resistance"]
    righe_ind = []
    for k in chiavi_mostra:
        if k in ind and ind[k] is not None:
            lbl, valore, comm = _indicatore_label(k, ind[k])
            righe_ind.append(
                '<tr>'
                f'<td style="padding:3px 8px;color:#9CA3AF;font-size:11px">{lbl}</td>'
                f'<td style="padding:3px 8px;color:#F9FAFB;font-size:11px;font-family:monospace">{valore}</td>'
                f'<td style="padding:3px 8px;color:#6B7280;font-size:10px">{comm}</td>'
                '</tr>'
            )
    if righe_ind:
        ind_html = f"""
        <div style="margin-top:14px">
          <div style="font-size:10px;color:#9CA3AF;letter-spacing:.1em;text-transform:uppercase;margin-bottom:6px">Indicatori tecnici</div>
          <table style="width:100%;border-collapse:collapse">{"".join(righe_ind)}</table>
        </div>"""

    # AI summary
    ai_html = ""
    if ai_sum:
        ai_html = f"""
        <div style="margin-top:14px;padding:10px 14px;background:#0A0F1A;border-left:3px solid #2563EB;border-radius:0 4px 4px 0">
          <div style="font-size:9px;color:#2563EB;letter-spacing:.1em;margin-bottom:5px">ANALISI AI</div>
          <div style="font-size:11px;color:#9CA3AF;line-height:1.7">{ai_sum}</div>
        </div>"""

    # News
    news_html = ""
    if news:
        items_n = "".join(
            f'<div style="padding:5px 0;border-bottom:1px solid #1F2937;font-size:11px;color:#9CA3AF">'
            f'▸ {n.get("headline","")} <span style="color:#4B5563">{n.get("source","")}</span></div>'
            for n in news[:3]
        )
        news_html = f"""
        <div style="margin-top:12px">
          <div style="font-size:10px;color:#9CA3AF;letter-spacing:.1em;margin-bottom:6px">ULTIME NOTIZIE</div>
          {items_n}
        </div>"""

    # Barra confidenza
    conf_color = "#16A34A" if conf >= 70 else "#F59E0B" if conf >= 50 else "#DC2626"
    conf_label = "Alta" if conf >= 70 else "Media" if conf >= 50 else "Bassa"
    score_str  = ("+" if score >= 0 else "") + str(score)
    isin_str   = f'<span style="color:#4B5563;font-size:10px;margin-left:8px">ISIN: {isin}</span>' if isin else ""

    return f"""
    <div style="background:#111827;border:1px solid #1F2937;border-radius:10px;margin-bottom:20px;overflow:hidden">

      <!-- Intestazione segnale -->
      <div style="background:{col}18;border-bottom:2px solid {col};padding:16px 20px;display:flex;justify-content:space-between;align-items:flex-start">
        <div>
          <div style="font-size:22px;font-weight:800;color:{col}">{etiq}</div>
          <div style="font-size:18px;font-weight:700;color:#F9FAFB;margin-top:4px">{sym}
            <span style="font-size:13px;font-weight:400;color:#9CA3AF;margin-left:6px">{full_name}</span>
          </div>
          <div style="font-size:11px;color:#6B7280;margin-top:4px">{mercato_it} · {tipo_it}{isin_str}</div>
        </div>
        <div style="text-align:right;flex-shrink:0;margin-left:20px">
          {f'<div style="font-size:20px;font-weight:700;color:#F9FAFB;font-family:monospace">{curr} {price}</div>' if price else ""}
          <div style="margin-top:4px">
            <span style="font-size:11px;color:#9CA3AF">Punteggio: </span>
            <span style="font-size:13px;font-weight:700;color:{col};font-family:monospace">{score_str}/100</span>
          </div>
        </div>
      </div>

      <!-- Confidenza -->
      <div style="padding:10px 20px;background:#0D1420;border-bottom:1px solid #1F2937">
        <div style="display:flex;justify-content:space-between;margin-bottom:4px">
          <span style="font-size:10px;color:#9CA3AF;letter-spacing:.1em">CONFIDENZA SEGNALE</span>
          <span style="font-size:11px;font-weight:700;color:{conf_color}">{conf}% — {conf_label}</span>
        </div>
        <div style="height:6px;background:#1F2937;border-radius:3px">
          <div style="height:6px;width:{conf}%;background:{conf_color};border-radius:3px"></div>
        </div>
      </div>

      <!-- Corpo -->
      <div style="padding:16px 20px">
        {livelli_html}
        {motiv_html}
        {bd_html}
        {ind_html}
        {ai_html}
        {news_html}
      </div>
    </div>"""


def _card_hold(r: dict) -> str:
    """Card compatta per asset in HOLD/NO_DATA."""
    action = r.get("action", "HOLD")
    sym    = r.get("symbol", "?")
    nm     = r.get("name", sym)
    score  = r.get("score", 0)
    conf   = r.get("confidence", 0)
    price  = r.get("price")
    curr   = r.get("currency", "")
    ind    = r.get("indicators", {})
    rsi    = ind.get("rsi", "")
    adx    = ind.get("adx", "")

    score_str = ("+" if score >= 0 else "") + str(score)
    etiq   = {"HOLD": "⚪ Neutro", "NO_DATA": "⚫ Nessun dato"}.get(action, action)
    price_str = f" — {curr} {price}" if price else ""

    return (
        f'<tr style="border-bottom:1px solid #1F2937">'
        f'<td style="padding:7px 10px;color:#F9FAFB;font-size:12px;font-weight:600">{sym}</td>'
        f'<td style="padding:7px 10px;color:#9CA3AF;font-size:11px">{nm[:25]}</td>'
        f'<td style="padding:7px 10px;color:#6B7280;font-size:11px">{etiq}</td>'
        f'<td style="padding:7px 10px;color:#9CA3AF;font-size:11px;font-family:monospace">{score_str}</td>'
        f'<td style="padding:7px 10px;color:#9CA3AF;font-size:11px">{rsi}{" | " if rsi and adx else ""}{adx}</td>'
        f'<td style="padding:7px 10px;color:#6B7280;font-size:11px">{price_str.strip()}</td>'
        '</tr>'
    )


def build_html_report(results: List[Dict], run_ts: str, next_ts: str) -> str:
    run_dt  = run_ts[:19].replace("T", " ") if run_ts else "---"
    next_dt = next_ts[:19].replace("T", " ") if next_ts else "---"

    # Dividi segnali
    buy_list    = [r for r in results if r.get("action") == "BUY"]
    sell_list   = [r for r in results if r.get("action") == "SELL"]
    watch_list  = [r for r in results if r.get("action") == "WATCHLIST"]
    hold_list   = [r for r in results if r.get("action") not in ("BUY","SELL","WATCHLIST")]

    # Riepilogo header
    n_buy   = len(buy_list)
    n_sell  = len(sell_list)
    n_watch = len(watch_list)
    n_hold  = len(hold_list)

    # Riepilogo testuale
    riep_items = []
    if n_buy:   riep_items.append(f'<span style="color:#16A34A;font-weight:700">🟢 {n_buy} Acquisto{"i" if n_buy>1 else ""}</span>')
    if n_sell:  riep_items.append(f'<span style="color:#DC2626;font-weight:700">🔴 {n_sell} Vendita{"e" if n_sell>1 else ""}</span>')
    if n_watch: riep_items.append(f'<span style="color:#2563EB;font-weight:700">🔵 {n_watch} Da osservare</span>')
    if n_hold:  riep_items.append(f'<span style="color:#6B7280">⚪ {n_hold} Neutri</span>')
    riepilogo = " &nbsp;·&nbsp; ".join(riep_items) if riep_items else "Nessun segnale attivo"

    # Sezioni principali
    sezione_acquisti = ""
    if buy_list:
        cards = "".join(_card_segnale(r) for r in buy_list)
        sezione_acquisti = f"""
        <div style="margin-bottom:30px">
          <div style="font-size:13px;font-weight:700;color:#16A34A;letter-spacing:.05em;
               margin-bottom:14px;padding:8px 16px;background:#052E16;border-left:4px solid #16A34A;border-radius:0 6px 6px 0">
            🟢 SEGNALI DI ACQUISTO ({n_buy})
          </div>
          {cards}
        </div>"""

    sezione_vendite = ""
    if sell_list:
        cards = "".join(_card_segnale(r) for r in sell_list)
        sezione_vendite = f"""
        <div style="margin-bottom:30px">
          <div style="font-size:13px;font-weight:700;color:#DC2626;letter-spacing:.05em;
               margin-bottom:14px;padding:8px 16px;background:#450A0A;border-left:4px solid #DC2626;border-radius:0 6px 6px 0">
            🔴 SEGNALI DI VENDITA ({n_sell})
          </div>
          {cards}
        </div>"""

    sezione_watch = ""
    if watch_list:
        cards = "".join(_card_segnale(r) for r in watch_list)
        sezione_watch = f"""
        <div style="margin-bottom:30px">
          <div style="font-size:13px;font-weight:700;color:#2563EB;letter-spacing:.05em;
               margin-bottom:14px;padding:8px 16px;background:#0C1A3A;border-left:4px solid #2563EB;border-radius:0 6px 6px 0">
            🔵 DA OSSERVARE ({n_watch})
          </div>
          {cards}
        </div>"""

    sezione_hold = ""
    if hold_list:
        righe = "".join(_card_hold(r) for r in hold_list)
        sezione_hold = f"""
        <div style="margin-bottom:20px">
          <div style="font-size:12px;font-weight:600;color:#6B7280;margin-bottom:10px">
            ⚪ ALTRI ASSET MONITORATI ({n_hold})
          </div>
          <table style="width:100%;border-collapse:collapse;background:#111827;border-radius:8px;overflow:hidden;border:1px solid #1F2937">
            <tr style="background:#0D1420">
              <th style="padding:7px 10px;color:#6B7280;font-size:10px;text-align:left;letter-spacing:.1em">SIMBOLO</th>
              <th style="padding:7px 10px;color:#6B7280;font-size:10px;text-align:left;letter-spacing:.1em">NOME</th>
              <th style="padding:7px 10px;color:#6B7280;font-size:10px;text-align:left;letter-spacing:.1em">STATO</th>
              <th style="padding:7px 10px;color:#6B7280;font-size:10px;text-align:left;letter-spacing:.1em">SCORE</th>
              <th style="padding:7px 10px;color:#6B7280;font-size:10px;text-align:left;letter-spacing:.1em">RSI | ADX</th>
              <th style="padding:7px 10px;color:#6B7280;font-size:10px;text-align:left;letter-spacing:.1em">PREZZO</th>
            </tr>
            {righe}
          </table>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>Market Analyze — Report</title>
</head>
<body style="margin:0;padding:0;background:#0D1117;font-family:'Segoe UI',system-ui,sans-serif">
<div style="max-width:700px;margin:0 auto;padding:24px 16px">

  <!-- INTESTAZIONE -->
  <div style="background:linear-gradient(135deg,#111827,#1F2937);border:1px solid #374151;
       border-radius:12px;padding:28px;margin-bottom:24px;text-align:center">
    <div style="font-size:28px;font-weight:800;color:#F9FAFB;letter-spacing:-.02em">
      ⊕ Market Analyze
    </div>
    <div style="font-size:13px;color:#6B7280;margin-top:6px;letter-spacing:.05em">
      REPORT SEGNALI DI TRADING · DATI REALI · AI VALIDATION
    </div>
    <div style="margin-top:16px;display:flex;justify-content:center;gap:24px;flex-wrap:wrap;font-size:12px;color:#9CA3AF">
      <span>📅 Generato: <strong style="color:#F9FAFB">{run_dt}</strong></span>
      <span>⏭ Prossimo: <strong style="color:#F9FAFB">{next_dt}</strong></span>
      <span>📊 Analizzati: <strong style="color:#F9FAFB">{len(results)}</strong></span>
    </div>
  </div>

  <!-- RIEPILOGO -->
  <div style="background:#111827;border:1px solid #1F2937;border-radius:10px;padding:16px 20px;margin-bottom:24px;text-align:center">
    <div style="font-size:11px;color:#6B7280;letter-spacing:.1em;margin-bottom:8px">RIEPILOGO SEGNALI</div>
    <div style="font-size:14px;line-height:1.8">{riepilogo}</div>
  </div>

  <!-- SEGNALI PRINCIPALI -->
  {sezione_acquisti}
  {sezione_vendite}
  {sezione_watch}
  {sezione_hold}

  <!-- DISCLAIMER -->
  <div style="text-align:center;padding:16px;font-size:10px;color:#374151;margin-top:8px;border-top:1px solid #1F2937">
    Market Analyze Add-on · Home Assistant · Aggiornamento automatico ogni ora<br>
    <strong>Questo report è puramente informativo e non costituisce consulenza finanziaria.</strong><br>
    Verifica sempre le informazioni prima di prendere decisioni di investimento.
  </div>

</div>
</body>
</html>"""


# ── Entry point ───────────────────────────────────────────────────────────────

def send_report(results: List[Dict], run_ts: str, next_ts: str, cfg: Dict) -> bool:
    if not cfg.get("email_enabled"):
        return False

    email_to   = cfg.get("email_to",   "")
    email_from = cfg.get("email_from", "")
    if not email_to or not email_from:
        log.warning("[MAILER] email_to o email_from mancanti")
        return False

    min_score = int(cfg.get("email_min_score", 40))
    active    = [r for r in results if r.get("action") in ("BUY","SELL","WATCHLIST")]
    strong    = [r for r in active if abs(r.get("score", 0)) >= min_score]
    if not strong:
        log.info(f"[MAILER] Nessun segnale con |score| >= {min_score}")
        return False

    html_body = build_html_report(results, run_ts, next_ts)

    n_buy  = sum(1 for r in active if r.get("action") == "BUY")
    n_sell = sum(1 for r in active if r.get("action") == "SELL")
    parts  = []
    if n_buy:  parts.append(f"{n_buy} ACQUISTO{'I' if n_buy>1 else ''}")
    if n_sell: parts.append(f"{n_sell} VENDITA{'E' if n_sell>1 else ''}")
    subject = f"📊 Market Analyze — {', '.join(parts) or 'Nessun segnale'} — {run_ts[:10] if run_ts else ''}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = email_from
    msg["To"]      = email_to
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    # OAuth2 (preferito)
    cid   = cfg.get("oauth2_client_id",     "")
    csec  = cfg.get("oauth2_client_secret", "")
    rtok  = cfg.get("oauth2_refresh_token", "")
    if cid and csec and rtok:
        log.info("[MAILER] Invio con OAuth2...")
        if _send_oauth2(msg, email_from, email_to, cid, csec, rtok):
            return True
        log.warning("[MAILER] OAuth2 fallito, provo App Password")

    # App Password (fallback)
    user = cfg.get("smtp_user",     "")
    pw   = cfg.get("smtp_password", "")
    if user and pw:
        log.info("[MAILER] Invio con App Password...")
        return _send_apppassword(
            msg, email_from, email_to,
            cfg.get("smtp_host", "smtp.gmail.com"),
            int(cfg.get("smtp_port", 587)),
            user, pw, bool(cfg.get("smtp_tls", True)),
        )

    log.error("[MAILER] Nessuna credenziale configurata (OAuth2 o App Password)")
    return False