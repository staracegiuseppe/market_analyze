# backtest_engine.py v1.0 - No look-ahead bias. Signal at t, entry at t+1.
import logging, warnings
from dataclasses import dataclass, field, asdict
from typing      import Dict, List, Optional, Tuple
import numpy  as np
import pandas as pd

warnings.filterwarnings("ignore")
log = logging.getLogger("backtest")


@dataclass
class BacktestConfig:
    fee_pct:        float = 0.1    # round-trip fee %
    slippage_pct:   float = 0.05   # slippage %
    max_hold_bars:  int   = 20     # force exit after N bars
    initial_capital:float = 10_000


@dataclass
class Trade:
    symbol:     str
    entry_date: str
    exit_date:  str
    entry_price:float
    exit_price: float
    direction:  str   # BUY or SELL
    pnl_pct:    float
    pnl_abs:    float
    exit_reason:str   # stop, target, time, signal


def _compute_indicators_row(df: pd.DataFrame, i: int) -> Optional[Dict]:
    """Compute indicators using only data up to row i — no look-ahead."""
    from market_data import (_rsi, _bollinger, _ma, _macd, _adx,
                              _atr_regime, _obv, _roc, _sr, _volume)
    sl = df.iloc[:i+1]
    if len(sl) < 52:
        return None
    cl, hi, lo, vol = sl["Close"], sl["High"], sl["Low"], sl["Volume"]
    try:
        return {
            "last_price":  float(cl.iloc[-1]),
            "rsi":         _rsi(cl),
            "bollinger":   _bollinger(cl),
            "ma":          _ma(cl),
            "macd":        _macd(cl),
            "adx":         _adx(hi, lo, cl),
            "atr_regime":  _atr_regime(hi, lo, cl),
            "obv":         _obv(cl, vol),
            "roc10":       _roc(cl, 10),
            "support_res": _sr(hi, lo, 20),
            "volume":      _volume(vol),
            "stochastic":  {"k": 50, "d": 50, "signal": "NEUTRAL"},  # simplified
            "source":      "backtest",
        }
    except Exception:
        return None


def backtest_symbol(
    symbol:  str,
    asset:   Dict,
    period:  str = "3y",
    config:  BacktestConfig = BacktestConfig(),
) -> Dict:
    """
    Run backtest for a single symbol.
    Signal at bar t → entry at bar t+1.
    """
    from market_data  import fetch_indicators
    from signal_engine import build_quant_signal

    log.info(f"[BACKTEST] {symbol}: fetching {period} data")
    try:
        import yfinance as yf
        df = yf.Ticker(symbol).history(period=period, interval="1d", auto_adjust=True)
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

    if df is None or len(df) < 80:
        return {"symbol": symbol, "error": "insufficient data"}

    df = df.reset_index()
    dates  = df["Date"].astype(str).tolist()
    closes = df["Close"].tolist()

    trades: List[Trade] = []
    equity  = [config.initial_capital]
    cash    = config.initial_capital
    position: Optional[Dict] = None

    for i in range(52, len(df) - 1):
        price_now  = closes[i]
        price_next = closes[i + 1]

        # Check exit if in position
        if position:
            bars_held = i - position["entry_bar"]
            hit_stop   = (position["direction"] == "BUY"  and price_now <= position["sl"]) or \
                         (position["direction"] == "SELL" and price_now >= position["sl"])
            hit_target = (position["direction"] == "BUY"  and price_now >= position["tp"]) or \
                         (position["direction"] == "SELL" and price_now <= position["tp"])
            force_exit  = bars_held >= config.max_hold_bars

            reason = None
            if hit_stop:   reason = "stop"
            elif hit_target: reason = "target"
            elif force_exit: reason = "time"

            if reason:
                ep  = position["entry_price"]
                ex  = price_next * (1 - config.slippage_pct/100)
                fee = (ep + ex) * config.fee_pct / 100
                if position["direction"] == "BUY":
                    pnl_pct = (ex - ep) / ep * 100 - config.fee_pct
                    pnl_abs = (ex - ep) * position["size"] - fee
                else:
                    pnl_pct = (ep - ex) / ep * 100 - config.fee_pct
                    pnl_abs = (ep - ex) * position["size"] - fee

                cash += position["invested"] + pnl_abs
                trades.append(Trade(
                    symbol=symbol, entry_date=position["entry_date"],
                    exit_date=dates[i+1], entry_price=ep, exit_price=ex,
                    direction=position["direction"], pnl_pct=pnl_pct,
                    pnl_abs=pnl_abs, exit_reason=reason,
                ))
                position = None
                equity.append(cash)
                continue

        # Generate signal at bar i (data up to i only)
        if position is None:
            ind = _compute_indicators_row(df, i)
            if ind:
                sig = build_quant_signal(ind, asset)
                if sig["action"] in ("BUY", "SELL") and sig["entry"] and sig["stop_loss"] and sig["take_profit"]:
                    invest = cash * 0.95  # use 95% of cash per trade
                    ep     = price_next * (1 + config.slippage_pct/100)
                    size   = invest / ep
                    position = {
                        "direction":   sig["action"],
                        "entry_price": ep,
                        "entry_bar":   i,
                        "entry_date":  dates[i+1],
                        "sl":          sig["stop_loss"],
                        "tp":          sig["take_profit"],
                        "invested":    invest,
                        "size":        size,
                    }
                    cash -= invest

        equity.append(cash + (position["invested"] if position else 0))

    # ── Metrics ───────────────────────────────────────────────────────────────
    if not trades:
        return {"symbol": symbol, "error": "no trades generated", "trades": []}

    pnls = [t.pnl_pct for t in trades]
    wins  = [p for p in pnls if p > 0]
    loses = [p for p in pnls if p <= 0]

    eq_series = pd.Series(equity)
    roll_max  = eq_series.cummax()
    drawdowns = (eq_series - roll_max) / roll_max
    max_dd    = round(float(drawdowns.min()) * 100, 2)

    total_return = round((equity[-1] - config.initial_capital) / config.initial_capital * 100, 2)

    bars_per_year = 252
    years = len(df) / bars_per_year
    cagr  = round((((equity[-1] / config.initial_capital) ** (1/years)) - 1) * 100, 2) if years > 0 else None

    daily_ret = eq_series.pct_change().dropna()
    sharpe    = round(float(daily_ret.mean() / daily_ret.std() * np.sqrt(bars_per_year)), 2) \
                if float(daily_ret.std()) > 0 else None

    bh_return = round((closes[-1] - closes[52]) / closes[52] * 100, 2)

    profit_factor = round(sum(wins) / abs(sum(loses)), 2) if loses and sum(loses) != 0 else None
    expectancy    = round(np.mean(pnls), 2)

    log.info(f"[BACKTEST] {symbol}: {len(trades)} trades "
             f"WR={len(wins)/len(trades)*100:.0f}% "
             f"ret={total_return}% MDD={max_dd}%")

    return {
        "symbol":        symbol,
        "name":          asset.get("name", symbol),
        "market":        asset.get("market"),
        "period":        period,
        "total_trades":  len(trades),
        "win_rate":      round(len(wins) / len(trades) * 100, 1) if trades else 0,
        "avg_win":       round(np.mean(wins), 2) if wins else 0,
        "avg_loss":      round(np.mean(loses), 2) if loses else 0,
        "expectancy":    expectancy,
        "profit_factor": profit_factor,
        "max_drawdown":  max_dd,
        "total_return":  total_return,
        "cagr":          cagr,
        "sharpe":        sharpe,
        "bh_return":     bh_return,
        "trades":        [asdict(t) for t in trades],
        "equity_curve":  equity[::5],  # downsample for API response
    }


def backtest_batch(
    assets: List[Dict],
    period: str = "3y",
    config: BacktestConfig = BacktestConfig(),
) -> Dict:
    results = {}
    for asset in assets:
        sym = asset["symbol"]
        results[sym] = backtest_symbol(sym, asset, period, config)

    # Aggregate stats
    valid = [v for v in results.values() if "error" not in v and v.get("total_trades",0) > 0]
    if not valid:
        return {"results": results, "aggregate": {}}

    by_market: Dict[str, List] = {}
    for r in valid:
        m = r.get("market", "?")
        by_market.setdefault(m, []).append(r)

    agg = {
        "total_symbols":   len(valid),
        "avg_return":      round(np.mean([r["total_return"] for r in valid]), 2),
        "avg_win_rate":    round(np.mean([r["win_rate"] for r in valid]), 1),
        "avg_sharpe":      round(np.mean([r["sharpe"] for r in valid if r.get("sharpe")]), 2),
        "avg_max_dd":      round(np.mean([r["max_drawdown"] for r in valid]), 2),
        "by_market":       {m: {"count": len(v),
                                "avg_return": round(np.mean([r["total_return"] for r in v]),2)}
                            for m, v in by_market.items()},
    }
    return {"results": results, "aggregate": agg}
