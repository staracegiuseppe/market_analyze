# signal_engine.py v2.0 - Pure quantitative signals. Zero random/simulated data.
# build_quant_signal() is the single entry point.
import logging
from datetime import datetime, time as dtime
from typing   import Dict, List, Optional

log = logging.getLogger("signal_engine")

TRADING_START = dtime(8, 0)   # Inizio analisi e invio segnali
TRADING_END   = dtime(23, 30) # Fine analisi e invio segnali
ACTIONS       = ("BUY", "SELL", "WATCHLIST", "HOLD", "NO_DATA")


def is_trading_hours() -> bool:
    return TRADING_START <= datetime.now().time() <= TRADING_END


def _safe(d: dict, *keys, default=None):
    try:
        v = d
        for k in keys: v = v[k]
        return v
    except (KeyError, TypeError):
        return default


def build_quant_signal(ind: Optional[Dict], asset: Dict) -> Dict:
    """
    Pure quantitative signal from real indicators.
    Returns NO_DATA if ind is None or data insufficient.
    Requires: RSI, MA, MACD, ADX, OBV, Bollinger, ATR, SR.
    """
    sym  = asset.get("symbol", "?")
    base = {
        "symbol":        sym,
        "name":          asset.get("name", sym),
        "full_name":     asset.get("full_name", asset.get("name", sym)),
        "isin":          asset.get("isin", ""),
        "exchange":      asset.get("exchange", ""),
        "market":        asset.get("market", "?"),
        "asset_type":    asset.get("asset_type", "?"),
        "currency":      asset.get("currency", "?"),
        "action":        "NO_DATA",
        "confidence":    0,
        "score":         0,
        "score_breakdown": {},
        "reasons":       [],
        "price":         None,
        "indicators":    {},
        "entry":         None,
        "stop_loss":     None,
        "take_profit":   None,
        "risk_reward":   None,
        "has_real_data": False,
        "trading_window":is_trading_hours(),
        "timestamp":     datetime.now().isoformat(),
    }

    if ind is None:
        base["reasons"] = ["NO_DATA: real data unavailable"]
        log.info(f"[SIGNAL] {sym}: NO_DATA")
        return base

    price   = ind["last_price"]
    rsi     = ind["rsi"]
    bb      = ind["bollinger"]
    ma      = ind["ma"]
    macd_d  = ind["macd"]
    stoch   = ind["stochastic"]
    adx_d   = ind["adx"]
    vol     = ind["volume"]
    obv_d   = ind["obv"]
    sr      = ind["support_res"]
    atr_r   = ind["atr_regime"]
    donch   = ind["donchian20"]
    roc10       = ind["roc10"]
    rsi_div     = ind.get("rsi_divergence", "none")
    bb_sq_data  = ind.get("bb_squeeze_data", {})
    bounce_prob = ind.get("bounce_probability", None)

    ma20    = _safe(ma, "ma20")
    ma50    = _safe(ma, "ma50")
    ma200   = _safe(ma, "ma200")
    cross   = _safe(ma, "cross", default="none")
    hist    = _safe(macd_d, "histogram", default=0)
    macd_cross = _safe(macd_d, "crossing", default="none")
    adx_val = _safe(adx_d, "adx", default=0)
    pdi     = _safe(adx_d, "pdi", default=0)
    ndi     = _safe(adx_d, "ndi", default=0)
    obv_tr  = _safe(obv_d, "trend", default="neutral")
    sup     = _safe(sr, "support")
    res     = _safe(sr, "resistance")
    atr_val = _safe(atr_r, "atr", default=None)
    bb_pos  = _safe(bb, "position", default=50)
    bb_bw   = _safe(bb, "bandwidth", default=5)
    stoch_k = _safe(stoch, "k", default=50)
    vol_sig = _safe(vol, "signal", default="NORMAL")
    bb_up   = _safe(bb, "upper")
    bb_lo   = _safe(bb, "lower")

    # ── Scoring system (0–100) ────────────────────────────────────────────────
    bull_score = 0
    bear_score = 0
    reasons_b  = []  # bullish reasons
    reasons_s  = []  # bearish reasons
    breakdown  = {}

    # 1. MA alignment (max 20pts)
    if ma20 and ma50:
        if price > ma20 > ma50:
            bull_score += 15; breakdown["ma_align"] = +15
            reasons_b.append(f"price>MA20>MA50 ({ma20:.2f}/{ma50:.2f})")
        elif price < ma20 < ma50:
            bear_score += 15; breakdown["ma_align"] = -15
            reasons_s.append(f"price<MA20<MA50 ({ma20:.2f}/{ma50:.2f})")
        else:
            breakdown["ma_align"] = 0
    if cross == "golden_cross":
        bull_score += 8; breakdown["ma_cross"] = +8; reasons_b.append("golden cross")
    elif cross == "death_cross":
        bear_score += 8; breakdown["ma_cross"] = -8; reasons_s.append("death cross")
    else:
        breakdown["ma_cross"] = 0

    if ma200:
        if price > ma200:
            bull_score += 5; breakdown["vs_ma200"] = +5; reasons_b.append(f"above MA200 (+{ma['vs_ma200']:.1f}%)")
        else:
            bear_score += 5; breakdown["vs_ma200"] = -5; reasons_s.append(f"below MA200 ({ma['vs_ma200']:.1f}%)")

    # 2. MACD (max 15pts)
    if hist > 0:
        bull_score += 8; breakdown["macd"] = +8; reasons_b.append(f"MACD hist>0 ({hist:.4f})")
    else:
        bear_score += 8; breakdown["macd"] = -8; reasons_s.append(f"MACD hist<0 ({hist:.4f})")
    if macd_cross == "bullish_cross":
        bull_score += 7; breakdown["macd_cross"] = +7; reasons_b.append("MACD bullish crossover")
    elif macd_cross == "bearish_cross":
        bear_score += 7; breakdown["macd_cross"] = -7; reasons_s.append("MACD bearish crossover")

    # 3. RSI (max 12pts) - avoid extremes
    if 40 <= rsi <= 65:
        bull_score += 8; breakdown["rsi"] = +8; reasons_b.append(f"RSI constructive ({rsi})")
    elif 35 <= rsi < 40:
        bull_score += 4; breakdown["rsi"] = +4; reasons_b.append(f"RSI recovering ({rsi})")
    elif rsi > 70:
        bear_score += 6; breakdown["rsi"] = -6; reasons_b.append(f"RSI overbought ({rsi})")
    elif rsi < 30:
        bear_score += 6; breakdown["rsi"] = -6; reasons_s.append(f"RSI oversold ({rsi})")
    elif 65 < rsi <= 70:
        bull_score += 3; breakdown["rsi"] = +3
    elif rsi < 35:
        bear_score += 8; breakdown["rsi"] = -8; reasons_s.append(f"RSI weak ({rsi})")
    else:
        breakdown["rsi"] = 0

    # 4. ADX / trend strength (max 10pts)
    if adx_val > 25:
        if pdi > ndi:
            bull_score += 10; breakdown["adx"] = +10; reasons_b.append(f"ADX={adx_val:.1f} trend bullish")
        else:
            bear_score += 10; breakdown["adx"] = -10; reasons_s.append(f"ADX={adx_val:.1f} trend bearish")
    else:
        breakdown["adx"] = 0  # no trend

    # 5. OBV (max 8pts)
    if obv_tr == "bullish":
        bull_score += 8; breakdown["obv"] = +8; reasons_b.append("OBV bullish")
    else:
        bear_score += 5; breakdown["obv"] = -5; reasons_s.append("OBV bearish")

    # 6. Volume confirmation (max 6pts)
    if vol_sig == "HIGH":
        # High volume confirms direction
        if bull_score > bear_score:
            bull_score += 6; breakdown["volume"] = +6; reasons_b.append("high volume confirms")
        else:
            bear_score += 6; breakdown["volume"] = -6; reasons_s.append("high volume confirms down")
    else:
        breakdown["volume"] = 0

    # 7. Bollinger position (max 6pts)
    if bb_pos < 30 and stoch_k < 30:
        bull_score += 6; breakdown["bb"] = +6; reasons_b.append(f"BB oversold zone ({bb_pos:.0f}%)")
    elif bb_pos > 80 and stoch_k > 75:
        bear_score += 4; breakdown["bb"] = -4; reasons_s.append(f"BB overbought ({bb_pos:.0f}%)")
    else:
        breakdown["bb"] = 0

    # 8. ROC momentum (max 4pts)
    if roc10 > 3:
        bull_score += 4; breakdown["roc"] = +4; reasons_b.append(f"ROC10={roc10:.1f}%")
    elif roc10 < -3:
        bear_score += 4; breakdown["roc"] = -4; reasons_s.append(f"ROC10={roc10:.1f}%")
    else:
        breakdown["roc"] = 0

    # 9. RSI Divergence (max 10pts) — high-conviction reversal signal
    if rsi_div == "bullish_divergence":
        bull_score += 10; breakdown["rsi_div"] = +10
        reasons_b.append("Divergenza RSI rialzista: prezzo fa minimo più basso, RSI fa minimo più alto → momentum nascosto")
    elif rsi_div == "bearish_divergence":
        bear_score += 10; breakdown["rsi_div"] = -10
        reasons_s.append("Divergenza RSI ribassista: prezzo fa massimo più alto, RSI fa massimo più basso → momentum in esaurimento")
    else:
        breakdown["rsi_div"] = 0

    # 10. Bollinger Band Squeeze (max 6pts) — breakout confirmation
    bb_squeeze   = _safe(bb_sq_data, "squeeze", default=False)
    bb_breakout  = _safe(bb_sq_data, "breakout", default="none")
    if bb_squeeze and bb_breakout == "bullish_breakout":
        bull_score += 6; breakdown["bb_squeeze"] = +6
        reasons_b.append("BB Squeeze con breakout rialzista → espansione volatilità in corso")
    elif bb_squeeze and bb_breakout == "bearish_breakout":
        bear_score += 6; breakdown["bb_squeeze"] = -6
        reasons_s.append("BB Squeeze con breakout ribassista → pressione vendita accelerata")
    elif bb_squeeze:
        # Squeeze senza breakout: attenzione, segnale imminente
        reasons_b.append("BB Squeeze attivo → breakout imminente, direzione da confermare")
        breakdown["bb_squeeze"] = 0
    else:
        breakdown["bb_squeeze"] = 0

    # ── Value Trap Filter — penalizza BUY su titoli in forte downtrend ───────
    # Un titolo può avere RSI basso MA essere in trend ribassista strutturale.
    # Condizioni trap: profondamente sotto MA50 + MA200 + ROC60 molto negativo.
    vs_ma50_val  = _safe(ma, "vs_ma50",  default=0) or 0
    vs_ma200_val = _safe(ma, "vs_ma200", default=0) or 0
    perf_60d     = ind.get("performance", {}).get("60d")
    is_value_trap = (
        vs_ma50_val  < -20 and
        vs_ma200_val < -30 and
        perf_60d is not None and perf_60d < -20
    )
    if is_value_trap:
        bear_score += 8; breakdown["value_trap"] = -8
        reasons_s.append(f"⚠ Value Trap Filter: sotto MA50 {vs_ma50_val:.1f}% + MA200 {vs_ma200_val:.1f}% + perf60d {perf_60d:.1f}% → downtrend strutturale")
    else:
        breakdown["value_trap"] = 0

    # ── Net score → action ────────────────────────────────────────────────────
    net     = bull_score - bear_score
    max_pos = bull_score + bear_score if (bull_score + bear_score) > 0 else 1
    conf    = round(abs(net) / max_pos * 100)
    conf    = max(0, min(99, conf))

    if net >= 25 and conf >= 55:
        action = "BUY"
    elif net <= -25 and conf >= 55:
        action = "SELL"
    elif abs(net) >= 12 and conf >= 40:
        action = "WATCHLIST"
    else:
        action = "HOLD"

    reasons = reasons_b if net >= 0 else reasons_s

    # ── Entry / SL / TP from ATR and S/R ─────────────────────────────────────
    entry = sl = tp = rr = None
    if atr_val and atr_val > 0:
        # Usa ATR puro per SL/TP — garantisce R:R sensato indipendentemente da S/R
        # S/R usato solo come riferimento informativo nei log
        if action == "BUY":
            entry = round(price, 4)
            sl    = round(price - 1.5 * atr_val, 4)
            tp    = round(price + 2.5 * atr_val, 4)
        elif action == "SELL":
            entry = round(price, 4)
            sl    = round(price + 1.5 * atr_val, 4)
            tp    = round(price - 2.5 * atr_val, 4)
        elif action == "WATCHLIST":
            if net > 0:
                entry = round(price, 4)
                sl    = round(price - 2.0 * atr_val, 4)
                tp    = round(price + 2.0 * atr_val, 4)
            else:
                entry = round(price, 4)
                sl    = round(price + 2.0 * atr_val, 4)
                tp    = round(price - 2.0 * atr_val, 4)

        if entry and sl and tp:
            risk   = abs(entry - sl)
            reward = abs(tp - entry)
            rr     = round(reward / risk, 2) if risk > 0 else None

    # ── Log ───────────────────────────────────────────────────────────────────
    if action in ("BUY", "SELL"):
        log.info(f"[SIGNAL ★] {sym}: {action} conf={conf}% score={net:+d} "
                 f"entry={entry} SL={sl} TP={tp} RR=1:{rr} | {' | '.join(reasons[:3])}")
    else:
        log.info(f"[SIGNAL  ] {sym}: {action} conf={conf}% score={net:+d}")

    # ── Segnale qualità (1–5 stelle) ─────────────────────────────────────────
    # Basato su: confidence, layer agreement, presenza divergenza, non value trap
    quality_pts = 0
    if conf >= 70:             quality_pts += 2
    elif conf >= 50:           quality_pts += 1
    if rsi_div != "none":      quality_pts += 1
    if not is_value_trap:      quality_pts += 1
    if bb_squeeze and bb_breakout != "none": quality_pts += 1
    signal_quality = min(5, max(1, quality_pts))

    ind_snap = {
        "rsi": rsi, "adx": adx_val, "macd_hist": hist,
        "bb_pos": bb_pos, "bb_bw": bb_bw,
        "obv_trend": obv_tr, "roc10": roc10,
        "stoch_k": stoch_k, "vol_signal": vol_sig,
        "support": sup, "resistance": res,
        "atr": atr_val, "atr_regime": _safe(atr_r, "regime"),
        "ma_cross": cross, "ma20": ma20, "ma50": ma50, "ma200": ma200,
        "rsi_divergence":    rsi_div,
        "bb_squeeze":        bb_squeeze,
        "bounce_probability": bounce_prob,
    }

    return {**base,
        "action":            action,
        "confidence":        conf,
        "score":             net,
        "score_breakdown":   breakdown,
        "reasons":           reasons[:5],
        "price":             price,
        "indicators":        ind_snap,
        "entry":             entry,
        "stop_loss":         sl,
        "take_profit":       tp,
        "risk_reward":       rr,
        "has_real_data":     True,
        "signal_quality":    signal_quality,
        "rsi_divergence":    rsi_div,
        "bb_squeeze":        bb_squeeze,
        "bounce_probability": bounce_prob,
        "value_trap_flag":   is_value_trap,
        "action_effective":  action if is_trading_hours() else "HOLD (market closed)",
    }


def run_scanner(assets: List[Dict], tech_data: Dict) -> List[Dict]:
    """Scan all assets. Returns list sorted by |score| desc."""
    signals = []
    for asset in assets:
        sym = asset["symbol"]
        ind = tech_data.get(sym)
        sig = build_quant_signal(ind, asset)
        signals.append(sig)

    signals.sort(key=lambda s: abs(s.get("score", 0)), reverse=True)

    active = [s for s in signals if s["action"] in ("BUY","SELL")]
    log.info(f"[SCANNER] Done: {len(active)} active signals "
             f"({sum(1 for s in signals if s['action']=='BUY')} BUY, "
             f"{sum(1 for s in signals if s['action']=='SELL')} SELL, "
             f"{sum(1 for s in signals if s['action']=='WATCHLIST')} WATCHLIST)")
    return signals


# ── HA-style step logger ──────────────────────────────────────────────────────
class StepLogger:
    def step(self, n, total, tag, msg):
        log.info(f"[STEP {n}/{total} {tag.upper()}]  {msg}")
    def signal(self, sym, action, entry, sl, tp, conf, reason):
        if action in ("BUY","SELL"):
            rr = abs(tp-entry)/abs(entry-sl) if sl and abs(entry-sl)>0 else 0
            log.info(f"[SIGNAL ★★★]  {sym}: *** {action} ***  "
                     f"entry={entry}  SL={sl}  TP={tp}  R:R=1:{rr:.2f}  conf={conf}%  | {reason[:80]}")
        else:
            log.info(f"[SIGNAL ---]  {sym}: {action}  conf={conf}%  | {reason[:60]}")
    def warn(self, tag, msg):  log.warning(f"[{tag.upper()}]  {msg}")
    def error(self, tag, msg): log.error(f"[{tag.upper()}]  {msg}")

HA = StepLogger()