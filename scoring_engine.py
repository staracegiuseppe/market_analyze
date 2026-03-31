# scoring_engine.py v2.0
# Composite scoring multi-layer: Technical + Macro + Regime + Sector + Institutional + Fundamental
# Score finale: -100 a +100
# Sostituisce il vecchio signal_engine per il calcolo del punteggio complessivo

from typing import Dict, Optional, List, Tuple
import logging
from datetime import datetime

log = logging.getLogger("scoring_engine")

# ── Pesi layer (somma = 1.0) ──────────────────────────────────────────────────
# Configurabili — il tecnico rimane il layer principale ma ora è contestualizzato
WEIGHTS = {
    "technical":     0.35,   # RSI, MACD, ADX, MA, OBV, BB → da signal_engine
    "macro":         0.20,   # Fed, CPI, VIX, DXY, curva → macro_layer
    "regime":        0.15,   # risk-on/off, drawdown, breadth → derivato da macro
    "sector":        0.10,   # rotazione settoriale → derivato da macro/ai
    "institutional": 0.10,   # 13F, insider buying, holders → fundamental_layer
    "fundamental":   0.10,   # PE, revenue growth, margini → fundamental_layer
}

# Range punteggi per layer
RANGES = {
    "technical":     (-60, 60),   # da signal_engine (score / 100 * 60)
    "macro":         (-20, 20),   # da macro_layer
    "regime":        (-15, 15),   # derivato
    "sector":        (-10, 10),   # derivato
    "institutional": (-10, 10),   # da fundamental_layer
    "fundamental":   (-15, 15),   # da fundamental_layer
}

# Soglie segnale
BUY_THRESHOLD      = 35   # composite score minimo per segnale BUY
SELL_THRESHOLD     = -35  # composite score massimo per segnale SELL
WATCHLIST_BULL_THR = 15
WATCHLIST_BEAR_THR = -15
MIN_CONF_FOR_ACTIVE = 50  # confidence minima per BUY/SELL — sotto → WATCHLIST


def _normalize_technical(raw_score: int) -> float:
    """Normalizza il punteggio tecnico da signal_engine a [-60, +60]."""
    # signal_engine genera score da circa -70 a +70
    return max(-60, min(60, raw_score * 0.85))


def _derive_regime_score(macro_ctx: Optional[Dict]) -> Tuple[int, Dict]:
    """
    Deriva regime score da -15 a +15 dal contesto macro.
    Classifica mercato in: RISK_ON, CAUTIOUS, NEUTRAL, RISK_OFF, PANIC
    """
    if not macro_ctx:
        return 0, {"regime": "UNKNOWN", "score": 0}

    data   = macro_ctx.get("data", {})
    regime = macro_ctx.get("regime", "NEUTRAL")
    vix    = data.get("vix") or 20
    ycrv   = data.get("yield_curve_10y2y")
    sp500  = data.get("sp500")

    score = 0
    if regime == "RISK_ON":
        score = 12
    elif regime == "CAUTIOUS":
        score = 3
    elif regime == "NEUTRAL":
        score = 0
    elif regime == "RISK_OFF":
        score = -8
    else:
        score = -15

    # Penalizzazione VIX alto
    if vix > 35:
        score -= 5
    elif vix > 25:
        score -= 2

    score = max(-15, min(15, score))
    return score, {"regime": regime, "vix": vix, "score": score}


def _derive_sector_score(symbol: str, asset_type: str, market: str, macro_ctx: Optional[Dict]) -> Tuple[int, Dict]:
    """
    Sector score da -10 a +10.
    Confronta il settore dell'asset con i settori favoriti/headwind dal macro.
    """
    if not macro_ctx:
        return 0, {"score": 0, "alignment": "neutral"}

    favored   = macro_ctx.get("favored_sectors", [])
    headwinds = macro_ctx.get("headwind_sectors", [])

    # Mappa settore/mercato a categoria macro
    SECTOR_MAP = {
        "XLE":  "Energy",        "XOM": "Energy",  "ENI.MI": "Energy",
        "XLK":  "Growth Tech",   "QQQ": "Growth Tech", "NVDA": "Growth Tech",
        "AAPL": "Growth Tech",   "MSFT": "Growth Tech", "META": "Growth Tech", "AMZN": "Growth Tech",
        "XLF":  "Financials",    "UCG.MI": "Financials", "ISP.MI": "Financials",
        "TLT":  "Bonds",         "GLD": "Gold",
        "SPY":  "Broad Market",  "IWDA.AS": "Broad Market", "VWRL.AS": "Broad Market",
        "IWM":  "Small Cap",     "IEMG": "EM",
        "XLE":  "Energy",        "DIA": "Broad Market",
        "ENEL.MI": "Utilities",  "SRG.MI": "Utilities",
        "LDO.MI": "Defense",     "STLAM.MI": "Consumer Discretionary",
        "TIT.MI": "Telecom",     "PIRC.MI": "Consumer Discretionary",
        "EXS1.DE": "Europe Index","XDWD.DE": "Broad Market",
    }
    sector_cat = SECTOR_MAP.get(symbol, "Other")

    score = 0
    alignment = "neutral"
    if any(sector_cat.lower() in f.lower() for f in favored):
        score = 8; alignment = "tailwind"
    elif any(sector_cat.lower() in h.lower() for h in headwinds):
        score = -8; alignment = "headwind"
    elif market == "IT" and "Europe" in str(favored):
        score = 3; alignment = "mild_tailwind"
    elif market == "EU" and "Europe" in str(favored):
        score = 5; alignment = "tailwind"

    return max(-10, min(10, score)), {
        "score":     score,
        "alignment": alignment,
        "sector":    sector_cat,
        "favored":   favored,
        "headwinds": headwinds,
    }


def _penalize_conflicts(
    tech_score: float,
    macro_score: int,
    regime_score: int,
    inst_score: int,
) -> float:
    """
    Penalizza il composite score quando i layer si contraddicono fortemente.
    Esempio: segnale tecnico BUY ma macro molto negativo → riduzione confidence.
    """
    penalty = 0.0

    # Tecnico positivo + macro molto negativo
    if tech_score > 20 and macro_score < -10:
        penalty += 5
        log.debug("Conflict: BUY tecnico vs macro negativo → penalty -5")

    # Tecnico negativo + macro molto positivo
    if tech_score < -20 and macro_score > 10:
        penalty += 3
        log.debug("Conflict: SELL tecnico vs macro positivo → penalty -3")

    # Istituzionali negativi su segnale tecnico positivo
    if tech_score > 15 and inst_score < -5:
        penalty += 4
        log.debug("Conflict: BUY tecnico vs istituzionali negativi → penalty -4")

    return penalty


def _confidence(
    composite: float,
    layer_agreement: float,
    macro_quality: str = "medium",
) -> int:
    """
    Calcola confidence 0-99%.
    Layer agreement = percentuale di layer concordi con la direzione.
    """
    base = min(95, abs(composite) * 0.85)

    # Bonus/malus per accordo tra layer
    if layer_agreement >= 0.80:
        base = min(99, base + 10)
    elif layer_agreement >= 0.60:
        base = min(99, base + 5)
    elif layer_agreement < 0.40:
        base = max(0, base - 10)

    # Qualità dati macro
    if macro_quality == "high":
        base = min(99, base + 3)
    elif macro_quality == "none":
        base = max(0, base - 8)

    return max(0, min(99, int(base)))


def composite_signal(
    # Layer tecnico (da signal_engine)
    technical_signal: Dict,
    # Layer macro (da macro_layer)
    macro_ctx: Optional[Dict] = None,
    # Layer fondamentale (da fundamental_layer)
    fundamental_data: Optional[Dict] = None,
    # Layer istituzionale (da institutional_layer) — opzionale
    institutional_data: Optional[Dict] = None,
    # Layer rotazione settoriale (da sector_rotation_layer) — opzionale
    sector_rotation_ctx: Optional[Dict] = None,
    # Metadati asset
    asset: Dict = None,
) -> Dict:
    """
    Funzione principale: aggrega tutti i layer in un segnale composito.
    Input:
      technical_signal: output di signal_engine.build_quant_signal()
      macro_ctx:        output di macro_layer.fetch_macro_context()
      fundamental_data: output di fundamental_layer.fetch_fundamental_score()
      asset:            dict con symbol, market, asset_type, etc.
    Output:
      Signal dict arricchito con composite_score, sub_scores, confidence
    """
    if asset is None:
        asset = {}

    sym       = technical_signal.get("symbol", "?")
    action_t  = technical_signal.get("action", "NO_DATA")
    raw_score = technical_signal.get("score", 0)

    # ── Layer tecnico ─────────────────────────────────────────────────────────
    tech_norm = _normalize_technical(raw_score)

    # ── Layer macro ───────────────────────────────────────────────────────────
    macro_score  = macro_ctx.get("macro_score", 0) if macro_ctx else 0
    macro_detail = macro_ctx.get("macro_score_detail", {}) if macro_ctx else {}

    # ── Layer regime ──────────────────────────────────────────────────────────
    regime_score, regime_detail = _derive_regime_score(macro_ctx)

    # ── Layer settore ─────────────────────────────────────────────────────────
    sector_score, sector_detail = _derive_sector_score(
        sym,
        asset.get("asset_type", "stock"),
        asset.get("market", "?"),
        macro_ctx,
    )

    # ── Layer fondamentale ────────────────────────────────────────────────────
    f_score  = fundamental_data.get("fundamental_score", 0)   if fundamental_data else 0
    f_detail = fundamental_data.get("fundamental_detail", {}) if fundamental_data else {}

    # ── Layer istituzionale (institutional_layer priorità, fallback fundamental_data) ──
    if institutional_data:
        i_score  = institutional_data.get("institutional_score",  0)
        i_detail = institutional_data.get("institutional_detail", {})
    else:
        i_score  = fundamental_data.get("institutional_score", 0)  if fundamental_data else 0
        i_detail = fundamental_data.get("institutional_detail", {}) if fundamental_data else {}

    # ── Layer rotazione settoriale (sector_rotation_layer) ────────────────────
    if sector_rotation_ctx and sector_rotation_ctx.get("available"):
        from sector_rotation_layer import get_sector_score
        sec_score_rt, sec_detail_rt = get_sector_score(
            sym, asset.get("asset_type","stock"), sector_rotation_ctx
        )
        # Usa sector_rotation se migliore (più dati reali) rispetto al derive interno
        log.debug(f"[SCORING] {sym}: sector_rotation={sec_score_rt:+d} vs derive={sector_score:+d}")
        if sec_detail_rt.get("status") not in ("sector_not_classified", "no_rotation_data", None):
            sector_score  = sec_score_rt
            sector_detail = sec_detail_rt

    # ── Composite score pesato ────────────────────────────────────────────────
    # Scala ogni layer a [-100, +100] prima di pesare
    def _to100(v, rng):
        lo, hi = rng
        span = hi - lo
        return (v - lo) / span * 200 - 100 if span else 0

    tech_100   = _to100(tech_norm,    RANGES["technical"])
    macro_100  = _to100(macro_score,  RANGES["macro"])
    regime_100 = _to100(regime_score, RANGES["regime"])
    sector_100 = _to100(sector_score, RANGES["sector"])
    inst_100   = _to100(i_score,      RANGES["institutional"])
    fund_100   = _to100(f_score,      RANGES["fundamental"])

    composite_raw = (
        tech_100   * WEIGHTS["technical"]
      + macro_100  * WEIGHTS["macro"]
      + regime_100 * WEIGHTS["regime"]
      + sector_100 * WEIGHTS["sector"]
      + inst_100   * WEIGHTS["institutional"]
      + fund_100   * WEIGHTS["fundamental"]
    )

    # Penalizzazioni per conflitti tra layer
    penalty = _penalize_conflicts(tech_norm, macro_score, regime_score, i_score)
    composite_final = composite_raw - (penalty if composite_raw > 0 else -penalty)
    composite_int   = max(-100, min(100, int(composite_final)))

    # ── Layer agreement ───────────────────────────────────────────────────────
    direction = 1 if composite_int > 0 else -1
    scores_dir = [
        tech_norm, macro_score, regime_score, sector_score, f_score, i_score
    ]
    agreeing = sum(1 for s in scores_dir if s * direction > 0)
    agreement_pct = agreeing / len(scores_dir)

    # ── Azione finale ─────────────────────────────────────────────────────────
    # Se il tecnico dice NO_DATA, mantieni NO_DATA
    if action_t == "NO_DATA":
        final_action = "NO_DATA"
    elif composite_int >= BUY_THRESHOLD:
        final_action = "BUY"
    elif composite_int <= SELL_THRESHOLD:
        final_action = "SELL"
    elif composite_int >= WATCHLIST_BULL_THR:
        final_action = "WATCHLIST"
    elif composite_int <= WATCHLIST_BEAR_THR:
        final_action = "WATCHLIST"
    else:
        final_action = "HOLD"

    # ── Confidence ────────────────────────────────────────────────────────────
    macro_quality = "none" if not macro_ctx else ("high" if macro_ctx.get("sources", {}).get("fred") else "medium")
    conf = _confidence(composite_int, agreement_pct, macro_quality)

    # Regola: BUY/SELL con confidenza < MIN_CONF_FOR_ACTIVE sono fuorvianti.
    # Un segnale con conf bassa significa layer discordanti → degrada a WATCHLIST.
    if final_action in ("BUY", "SELL") and conf < MIN_CONF_FOR_ACTIVE:
        log.info(
            f"[SCORING] {asset.get('symbol','?') if asset else '?'}: "
            f"{final_action} → WATCHLIST (conf={conf}% < {MIN_CONF_FOR_ACTIVE}% minimo)"
        )
        final_action = "WATCHLIST"

    # ── Report motivazioni composite ─────────────────────────────────────────
    composite_reasons = []
    if macro_score > 5:
        composite_reasons.append(f"Macro favorevole (score={macro_score:+d}): " + " · ".join((macro_detail.get("notes", []) or [])[:2]))
    elif macro_score < -5:
        composite_reasons.append(f"Macro sfavorevole (score={macro_score:+d}): " + " · ".join((macro_detail.get("notes", []) or [])[:2]))
    if regime_detail.get("regime") not in ("NEUTRAL", "UNKNOWN"):
        composite_reasons.append(f"Regime di mercato: {regime_detail.get('regime')} (VIX={regime_detail.get('vix')})")
    if sector_detail.get("alignment") == "tailwind":
        composite_reasons.append(f"Settore {sector_detail.get('sector')} in tailwind macro")
    elif sector_detail.get("alignment") == "headwind":
        composite_reasons.append(f"Settore {sector_detail.get('sector')} in headwind macro ⚠")
    if f_detail.get("reasons"):
        composite_reasons += f_detail["reasons"][:1]
    if i_detail.get("reasons"):
        composite_reasons += i_detail["reasons"][:1]

    # ── Output ────────────────────────────────────────────────────────────────
    log.info(
        f"[SCORING] {sym}: tech={tech_norm:+.0f} macro={macro_score:+d} "
        f"regime={regime_score:+d} sector={sector_score:+d} "
        f"fund={f_score:+d} inst={i_score:+d} "
        f"→ composite={composite_int:+d} action={final_action} conf={conf}% "
        f"agreement={agreement_pct:.0%}"
    )

    # Merge con il segnale tecnico, aggiunge i layer composite
    result = {
        **technical_signal,
        # Sovrascrive i campi con i valori composite
        "action":            final_action,
        "confidence":        conf,
        "composite_score":   composite_int,
        "technical_score":   int(tech_norm),
        # Sotto-punteggi
        "sub_scores": {
            "technical":     int(tech_norm),
            "macro":         macro_score,
            "regime":        regime_score,
            "sector":        sector_score,
            "institutional": i_score,
            "fundamental":   f_score,
        },
        # Dettagli layer
        "macro_context": {
            "regime":         regime_detail.get("regime"),
            "vix":            regime_detail.get("vix"),
            "score":          macro_score,
            "favored":        macro_ctx.get("favored_sectors", []) if macro_ctx else [],
            "headwinds":      macro_ctx.get("headwind_sectors", []) if macro_ctx else [],
            "data": macro_ctx.get("data", {}) if macro_ctx else {},
        } if macro_ctx else {},
        "sector_alignment":  sector_detail.get("alignment", "neutral"),
        "layer_agreement":   round(agreement_pct, 2),
        "composite_reasons": composite_reasons[:5],
        "fundamental_detail":f_detail,
        "institutional_detail": i_detail,
        # Mantieni reasons tecniche originali
        "technical_reasons": technical_signal.get("reasons", []),
    }

    # Combina reasons tecniche + composite per il campo "reasons" principale
    result["reasons"] = composite_reasons[:3] + technical_signal.get("reasons", [])[:3]

    return result


# ── Batch composite scoring ───────────────────────────────────────────────────
def run_composite_scanner(
    assets:          List[Dict],
    technical_sigs:  Dict,           # {symbol: signal_dict}
    macro_ctx:       Optional[Dict],
    fundamental_db:  Optional[Dict] = None,  # {symbol: fund_dict}
    institutional_db:Optional[Dict] = None,  # {symbol: inst_dict}
    sector_rotation: Optional[Dict] = None,  # output fetch_sector_rotation()
) -> List[Dict]:
    """
    Applica composite scoring a tutti gli asset.
    Restituisce lista ordinata per composite_score assoluto.
    """
    results = []
    for asset in assets:
        sym  = asset["symbol"]
        tech = technical_sigs.get(sym)
        fund = fundamental_db.get(sym)     if fundamental_db  else None
        inst = institutional_db.get(sym)   if institutional_db else None

        if tech is None:
            # Asset senza dati tecnici
            results.append({
                "symbol": sym, "name": asset.get("name", sym),
                "full_name": asset.get("full_name", asset.get("name", sym)),
                "isin": asset.get("isin",""),
                "market": asset.get("market","?"),
                "asset_type": asset.get("asset_type","?"),
                "currency": asset.get("currency",""),
                "action": "NO_DATA", "composite_score": 0, "confidence": 0,
                "has_real_data": False,
            })
            continue

        composite = composite_signal(
            technical_signal=tech,
            macro_ctx=macro_ctx,
            fundamental_data=fund,
            institutional_data=inst,
            sector_rotation_ctx=sector_rotation,
            asset=asset,
        )
        results.append(composite)

    results.sort(key=lambda s: abs(s.get("composite_score", 0)), reverse=True)

    active   = [s for s in results if s.get("action") in ("BUY","SELL")]
    watchlist= [s for s in results if s.get("action") == "WATCHLIST"]
    log.info(
        f"[SCORING] Composite done: {len(active)} BUY/SELL "
        f"({sum(1 for s in results if s.get('action')=='BUY')} BUY, "
        f"{sum(1 for s in results if s.get('action')=='SELL')} SELL), "
        f"{len(watchlist)} WATCHLIST"
    )
    return results
