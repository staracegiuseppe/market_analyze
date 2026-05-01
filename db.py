# db.py — MariaDB integration for market_analyze
# Gestisce connessione, schema, CRUD assets e persistenza analisi per trend building.
# Se MariaDB non è configurato, tutte le funzioni sono no-op e l'app usa JSON locale.

import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

log = logging.getLogger("db")

_cfg: Optional[Dict] = None
_enabled = False


def init_db(host: str, port: int, user: str, password: str, database: str) -> bool:
    """
    Inizializza la connessione MariaDB e crea lo schema se non esiste.
    Ritorna True se la connessione è riuscita, False altrimenti.
    """
    global _cfg, _enabled
    if not host or not user or not database:
        log.info("[DB] MariaDB non configurato — uso JSON locale")
        return False
    try:
        import pymysql  # noqa
    except ImportError:
        log.warning("[DB] PyMySQL non installato — usa: pip install PyMySQL")
        return False

    _cfg = {
        "host":       host,
        "port":       int(port),
        "user":       user,
        "password":   password,
        "database":   database,
        "charset":    "utf8mb4",
        "autocommit": True,
    }
    try:
        conn = _connect()
        conn.close()
        _enabled = True
        log.info(f"[DB] Connessione MariaDB OK: {user}@{host}:{port}/{database}")
        _create_schema()
        _migrate_schema()
        return True
    except Exception as e:
        log.error(f"[DB] Connessione fallita: {e}")
        _cfg = None
        return False


def is_enabled() -> bool:
    return _enabled


def _connect():
    import pymysql
    from pymysql.cursors import DictCursor
    return pymysql.connect(**_cfg, cursorclass=DictCursor)


def _create_schema():
    """Crea le tabelle se non esistono."""
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS assets (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            symbol     VARCHAR(32)  NOT NULL UNIQUE,
            name       VARCHAR(255),
            full_name  VARCHAR(512),
            isin       VARCHAR(64),
            market     VARCHAR(8),
            country    VARCHAR(8),
            asset_type VARCHAR(16),
            currency   VARCHAR(8),
            exchange   VARCHAR(32),
            enabled    TINYINT(1) DEFAULT 1,
            note       TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS analysis_runs (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            run_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            signals_json    MEDIUMTEXT,
            macro_json      TEXT,
            assets_count    INT DEFAULT 0,
            active_count    INT DEFAULT 0,
            buy_count       INT DEFAULT 0,
            sell_count      INT DEFAULT 0,
            watchlist_count INT DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS signal_history (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            run_id          INT,
            run_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            symbol          VARCHAR(32),
            action          VARCHAR(16),
            score           INT,
            composite_score INT,
            confidence      INT,
            price           DECIMAL(18,4),
            entry           DECIMAL(18,4),
            stop_loss       DECIMAL(18,4),
            take_profit     DECIMAL(18,4),
            risk_reward     DECIMAL(6,2),
            market          VARCHAR(8),
            asset_type      VARCHAR(16),
            sub_scores_json TEXT,
            indicators_json TEXT,
            INDEX idx_symbol_run (symbol, run_at),
            INDEX idx_run_id     (run_id),
            INDEX idx_run_at     (run_at)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS smart_money_history (
            id                   INT AUTO_INCREMENT PRIMARY KEY,
            computed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_quality         VARCHAR(16),
            opportunities_count  INT DEFAULT 0,
            macro_regime         VARCHAR(32),
            analysis_json        MEDIUMTEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS wallet_holdings (
            id                    INT AUTO_INCREMENT PRIMARY KEY,
            symbol                VARCHAR(32) NOT NULL UNIQUE,
            name                  VARCHAR(255),
            full_name             VARCHAR(512),
            isin                  VARCHAR(64),
            market                VARCHAR(8),
            country               VARCHAR(8),
            asset_type            VARCHAR(16),
            currency              VARCHAR(8),
            exchange              VARCHAR(32),
            quantity              DECIMAL(18,6) DEFAULT 0,
            avg_price             DECIMAL(18,6) DEFAULT 0,
            target_price          DECIMAL(18,6) NULL,
            stop_loss             DECIMAL(18,6) NULL,
            horizon_days          INT DEFAULT 30,
            alert_enabled         TINYINT(1) DEFAULT 1,
            enabled               TINYINT(1) DEFAULT 1,
            position_status       VARCHAR(16) DEFAULT 'ACTIVE',
            closed_at             TIMESTAMP NULL,
            exit_price            DECIMAL(18,6) NULL,
            exit_note             TEXT,
            note                  TEXT,
            created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS wallet_analysis_runs (
            id                    INT AUTO_INCREMENT PRIMARY KEY,
            run_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            holdings_count        INT DEFAULT 0,
            invested_total        DECIMAL(18,2) DEFAULT 0,
            market_value_total    DECIMAL(18,2) DEFAULT 0,
            pnl_total             DECIMAL(18,2) DEFAULT 0,
            pnl_pct_total         DECIMAL(10,4) DEFAULT 0,
            alert_count           INT DEFAULT 0,
            analysis_json         MEDIUMTEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS wallet_snapshot_items (
            id                    INT AUTO_INCREMENT PRIMARY KEY,
            run_id                INT NOT NULL,
            run_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            symbol                VARCHAR(32) NOT NULL,
            quantity              DECIMAL(18,6) DEFAULT 0,
            avg_price             DECIMAL(18,6) DEFAULT 0,
            current_price         DECIMAL(18,6) NULL,
            invested_amount       DECIMAL(18,2) DEFAULT 0,
            market_value          DECIMAL(18,2) DEFAULT 0,
            pnl_amount            DECIMAL(18,2) DEFAULT 0,
            pnl_pct               DECIMAL(10,4) DEFAULT 0,
            signal_action         VARCHAR(16),
            recommendation        VARCHAR(24),
            confidence            INT DEFAULT 0,
            holding_days_estimate INT DEFAULT 0,
            analysis_json         MEDIUMTEXT,
            INDEX idx_wallet_symbol_run (symbol, run_at),
            INDEX idx_wallet_run_id     (run_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS wallet_alerts (
            id                    INT AUTO_INCREMENT PRIMARY KEY,
            created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            symbol                VARCHAR(32) NOT NULL,
            alert_type            VARCHAR(32) NOT NULL,
            recommendation        VARCHAR(24),
            confidence            INT DEFAULT 0,
            payload_json          MEDIUMTEXT,
            INDEX idx_wallet_alert_symbol_created (symbol, created_at)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS income_plan_runs (
            id                    INT AUTO_INCREMENT PRIMARY KEY,
            run_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            scenario              TEXT,
            risk_level            VARCHAR(16),
            monthly_income_est    DECIMAL(18,2) DEFAULT 0,
            target_capital        DECIMAL(18,2) DEFAULT 0,
            gap_to_target         DECIMAL(18,2) DEFAULT 0,
            plan_json             MEDIUMTEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS crypto_alerts (
            id                    INT AUTO_INCREMENT PRIMARY KEY,
            created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            symbol                VARCHAR(32) NOT NULL,
            action                VARCHAR(16) NOT NULL,
            confidence            INT DEFAULT 0,
            payload_json          MEDIUMTEXT,
            INDEX idx_crypto_alert_symbol_created (symbol, created_at)
        )
        """,
    ]
    conn = _connect()
    try:
        with conn.cursor() as cur:
            for stmt in stmts:
                cur.execute(stmt.strip())
        log.info("[DB] Schema verificato/creato")
    finally:
        conn.close()


def _migrate_schema():
    """
    Aggiunge nuove colonne a tabelle esistenti — idempotente.
    Necessario per DB già creati con schema vecchio.
    """
    migrations = [
        "ALTER TABLE signal_history ADD COLUMN composite_score INT NULL",
        "ALTER TABLE signal_history ADD COLUMN sub_scores_json TEXT NULL",
        "ALTER TABLE signal_history ADD COLUMN indicators_json TEXT NULL",
        "ALTER TABLE wallet_holdings ADD COLUMN target_price DECIMAL(18,6) NULL",
        "ALTER TABLE wallet_holdings ADD COLUMN stop_loss DECIMAL(18,6) NULL",
        "ALTER TABLE wallet_holdings ADD COLUMN horizon_days INT DEFAULT 30",
        "ALTER TABLE wallet_holdings ADD COLUMN alert_enabled TINYINT(1) DEFAULT 1",
        "ALTER TABLE wallet_holdings ADD COLUMN enabled TINYINT(1) DEFAULT 1",
        "ALTER TABLE wallet_holdings ADD COLUMN position_status VARCHAR(16) DEFAULT 'ACTIVE'",
        "ALTER TABLE wallet_holdings ADD COLUMN closed_at TIMESTAMP NULL",
        "ALTER TABLE wallet_holdings ADD COLUMN exit_price DECIMAL(18,6) NULL",
        "ALTER TABLE wallet_holdings ADD COLUMN exit_note TEXT NULL",
        "ALTER TABLE income_plan_runs MODIFY COLUMN scenario TEXT NULL",
    ]
    conn = _connect()
    try:
        with conn.cursor() as cur:
            for sql in migrations:
                try:
                    cur.execute(sql)
                except Exception:
                    pass  # colonna già esistente — ignora
        log.info("[DB] Migration colonne signal_history OK")
    except Exception as e:
        log.warning(f"[DB] _migrate_schema: {e}")
    finally:
        conn.close()


# ── Assets ─────────────────────────────────────────────────────────────────────

def load_assets_from_db() -> List[Dict]:
    """Carica tutti gli asset dal DB."""
    if not _enabled:
        return []
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM assets ORDER BY market, symbol")
            rows = cur.fetchall()
        conn.close()
        return [_row_to_asset(r) for r in rows]
    except Exception as e:
        log.error(f"[DB] load_assets_from_db: {e}")
        return []


def _row_to_asset(r: Dict) -> Dict:
    return {
        "symbol":     r["symbol"],
        "name":       r.get("name") or "",
        "full_name":  r.get("full_name") or "",
        "isin":       r.get("isin") or "",
        "market":     r.get("market") or "US",
        "country":    r.get("country") or "US",
        "asset_type": r.get("asset_type") or "stock",
        "currency":   r.get("currency") or "USD",
        "exchange":   r.get("exchange") or "",
        "enabled":    bool(r.get("enabled", 1)),
        "note":       r.get("note") or "",
    }


def _row_to_wallet_holding(r: Dict) -> Dict:
    return {
        "symbol":        r["symbol"],
        "name":          r.get("name") or "",
        "full_name":     r.get("full_name") or "",
        "isin":          r.get("isin") or "",
        "market":        r.get("market") or "US",
        "country":       r.get("country") or "US",
        "asset_type":    r.get("asset_type") or "stock",
        "currency":      r.get("currency") or "USD",
        "exchange":      r.get("exchange") or "",
        "quantity":      float(r.get("quantity") or 0),
        "avg_price":     float(r.get("avg_price") or 0),
        "target_price":  float(r["target_price"]) if r.get("target_price") is not None else None,
        "stop_loss":     float(r["stop_loss"]) if r.get("stop_loss") is not None else None,
        "horizon_days":  int(r.get("horizon_days") or 30),
        "alert_enabled": bool(r.get("alert_enabled", 1)),
        "enabled":       bool(r.get("enabled", 1)),
        "position_status": (r.get("position_status") or "ACTIVE").upper(),
        "closed_at":     r["closed_at"].isoformat() if r.get("closed_at") and hasattr(r["closed_at"], "isoformat") else None,
        "exit_price":    float(r["exit_price"]) if r.get("exit_price") is not None else None,
        "exit_note":     r.get("exit_note") or "",
        "note":          r.get("note") or "",
    }


def save_asset_to_db(asset: Dict) -> bool:
    """Insert o update di un asset (upsert su symbol)."""
    if not _enabled:
        return False
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO assets
                    (symbol, name, full_name, isin, market, country,
                     asset_type, currency, exchange, enabled, note)
                VALUES
                    (%(symbol)s, %(name)s, %(full_name)s, %(isin)s, %(market)s, %(country)s,
                     %(asset_type)s, %(currency)s, %(exchange)s, %(enabled)s, %(note)s)
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name), full_name=VALUES(full_name), isin=VALUES(isin),
                    market=VALUES(market), country=VALUES(country),
                    asset_type=VALUES(asset_type), currency=VALUES(currency),
                    exchange=VALUES(exchange), enabled=VALUES(enabled), note=VALUES(note)
            """, {
                "symbol":     asset.get("symbol", ""),
                "name":       asset.get("name", ""),
                "full_name":  asset.get("full_name", ""),
                "isin":       asset.get("isin", ""),
                "market":     asset.get("market", "US"),
                "country":    asset.get("country", "US"),
                "asset_type": asset.get("asset_type", "stock"),
                "currency":   asset.get("currency", "USD"),
                "exchange":   asset.get("exchange", ""),
                "enabled":    1 if asset.get("enabled", True) else 0,
                "note":       asset.get("note", ""),
            })
        conn.close()
        return True
    except Exception as e:
        log.error(f"[DB] save_asset_to_db ({asset.get('symbol')}): {e}")
        return False


def delete_asset_from_db(symbol: str) -> bool:
    """Elimina un asset dal DB. Ritorna True se trovato e eliminato."""
    if not _enabled:
        return False
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM assets WHERE symbol=%s", (symbol,))
            affected = cur.rowcount
        conn.close()
        return affected > 0
    except Exception as e:
        log.error(f"[DB] delete_asset ({symbol}): {e}")
        return False


def toggle_asset_in_db(symbol: str) -> Optional[bool]:
    """Inverte il flag enabled. Ritorna il nuovo stato, o None se non trovato."""
    if not _enabled:
        return None
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("SELECT enabled FROM assets WHERE symbol=%s", (symbol,))
            row = cur.fetchone()
            if not row:
                conn.close()
                return None
            new_val = 0 if row["enabled"] else 1
            cur.execute("UPDATE assets SET enabled=%s WHERE symbol=%s", (new_val, symbol))
        conn.close()
        return bool(new_val)
    except Exception as e:
        log.error(f"[DB] toggle_asset ({symbol}): {e}")
        return None


def migrate_assets_from_json(assets: List[Dict]):
    """
    Importa gli asset da JSON nel DB solo se la tabella assets è vuota.
    Usato alla prima avvio dopo configurazione MariaDB.
    """
    if not _enabled or not assets:
        return
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM assets")
            count = cur.fetchone()["cnt"]
        conn.close()
        if count > 0:
            log.info(f"[DB] Assets già presenti ({count}) — skip migrazione JSON")
            return
        log.info(f"[DB] Prima avvio: migrazione {len(assets)} assets da JSON → MariaDB")
        for a in assets:
            save_asset_to_db(a)
        log.info("[DB] Migrazione assets completata")
    except Exception as e:
        log.error(f"[DB] migrate_assets_from_json: {e}")


# ── Wallet holdings ───────────────────────────────────────────────────────────

def load_wallet_holdings(include_closed: bool = False) -> List[Dict]:
    if not _enabled:
        return []
    try:
        conn = _connect()
        with conn.cursor() as cur:
            if include_closed:
                cur.execute("SELECT * FROM wallet_holdings ORDER BY position_status ASC, symbol")
            else:
                cur.execute("SELECT * FROM wallet_holdings WHERE COALESCE(position_status,'ACTIVE')='ACTIVE' ORDER BY symbol")
            rows = cur.fetchall()
        conn.close()
        return [_row_to_wallet_holding(r) for r in rows]
    except Exception as e:
        log.error(f"[DB] load_wallet_holdings: {e}")
        return []


def save_wallet_holding(holding: Dict) -> bool:
    if not _enabled:
        return False
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wallet_holdings
                    (symbol, name, full_name, isin, market, country, asset_type, currency,
                     exchange, quantity, avg_price, target_price, stop_loss, horizon_days,
                     alert_enabled, enabled, position_status, closed_at, exit_price, exit_note, note)
                VALUES
                    (%(symbol)s, %(name)s, %(full_name)s, %(isin)s, %(market)s, %(country)s,
                     %(asset_type)s, %(currency)s, %(exchange)s, %(quantity)s, %(avg_price)s,
                     %(target_price)s, %(stop_loss)s, %(horizon_days)s, %(alert_enabled)s,
                     %(enabled)s, %(position_status)s, %(closed_at)s, %(exit_price)s, %(exit_note)s, %(note)s)
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name), full_name=VALUES(full_name), isin=VALUES(isin),
                    market=VALUES(market), country=VALUES(country), asset_type=VALUES(asset_type),
                    currency=VALUES(currency), exchange=VALUES(exchange), quantity=VALUES(quantity),
                    avg_price=VALUES(avg_price), target_price=VALUES(target_price),
                    stop_loss=VALUES(stop_loss), horizon_days=VALUES(horizon_days),
                    alert_enabled=VALUES(alert_enabled), enabled=VALUES(enabled),
                    position_status=VALUES(position_status), closed_at=VALUES(closed_at),
                    exit_price=VALUES(exit_price), exit_note=VALUES(exit_note), note=VALUES(note)
            """, {
                "symbol":        holding.get("symbol", ""),
                "name":          holding.get("name", ""),
                "full_name":     holding.get("full_name", ""),
                "isin":          holding.get("isin", ""),
                "market":        holding.get("market", "US"),
                "country":       holding.get("country", "US"),
                "asset_type":    holding.get("asset_type", "stock"),
                "currency":      holding.get("currency", "USD"),
                "exchange":      holding.get("exchange", ""),
                "quantity":      holding.get("quantity", 0),
                "avg_price":     holding.get("avg_price", 0),
                "target_price":  holding.get("target_price"),
                "stop_loss":     holding.get("stop_loss"),
                "horizon_days":  holding.get("horizon_days", 30),
                "alert_enabled": 1 if holding.get("alert_enabled", True) else 0,
                "enabled":       1 if holding.get("enabled", True) else 0,
                "position_status": (holding.get("position_status") or "ACTIVE").upper(),
                "closed_at":     holding.get("closed_at"),
                "exit_price":    holding.get("exit_price"),
                "exit_note":     holding.get("exit_note", ""),
                "note":          holding.get("note", ""),
            })
        conn.close()
        return True
    except Exception as e:
        log.error(f"[DB] save_wallet_holding ({holding.get('symbol')}): {e}")
        return False


def delete_wallet_holding(symbol: str) -> bool:
    if not _enabled:
        return False
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM wallet_holdings WHERE symbol=%s", (symbol,))
            affected = cur.rowcount
        conn.close()
        return affected > 0
    except Exception as e:
        log.error(f"[DB] delete_wallet_holding ({symbol}): {e}")
        return False


def close_wallet_holding(symbol: str, exit_price: Optional[float] = None, exit_note: str = "") -> bool:
    if not _enabled:
        return False
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE wallet_holdings
                SET position_status='CLOSED',
                    closed_at=NOW(),
                    exit_price=%s,
                    exit_note=%s,
                    enabled=0
                WHERE symbol=%s
            """, (exit_price, exit_note, symbol.upper()))
            affected = cur.rowcount
        conn.close()
        return affected > 0
    except Exception as e:
        log.error(f"[DB] close_wallet_holding ({symbol}): {e}")
        return False


def reopen_wallet_holding(symbol: str) -> bool:
    if not _enabled:
        return False
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE wallet_holdings
                SET position_status='ACTIVE',
                    closed_at=NULL,
                    exit_price=NULL,
                    exit_note='',
                    enabled=1
                WHERE symbol=%s
            """, (symbol.upper(),))
            affected = cur.rowcount
        conn.close()
        return affected > 0
    except Exception as e:
        log.error(f"[DB] reopen_wallet_holding ({symbol}): {e}")
        return False


# ── Persistenza analisi ────────────────────────────────────────────────────────

def save_analysis_run(signals: List[Dict], macro_ctx: Optional[Dict] = None) -> Optional[int]:
    """
    Salva i risultati di uno scan completo.
    Ritorna run_id per riferimento, o None in caso di errore.
    """
    if not _enabled:
        return None
    try:
        active = [s for s in signals if s.get("action") in ("BUY", "SELL", "WATCHLIST")]
        buys   = sum(1 for s in signals if s.get("action") == "BUY")
        sells  = sum(1 for s in signals if s.get("action") == "SELL")
        watch  = sum(1 for s in signals if s.get("action") == "WATCHLIST")

        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO analysis_runs
                    (signals_json, macro_json, assets_count, active_count,
                     buy_count, sell_count, watchlist_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                json.dumps(signals, default=str),
                json.dumps(macro_ctx, default=str) if macro_ctx else None,
                len(signals), len(active), buys, sells, watch,
            ))
            run_id = cur.lastrowid

            # Righe individuali per trend analysis e lookback
            def _safe(v):
                try:
                    return float(v) if v not in (None, "", "N/A") else None
                except (TypeError, ValueError):
                    return None

            for s in signals:
                if s.get("action") in ("BUY", "SELL", "WATCHLIST", "HOLD"):
                    # Sub-scores compatti (solo valori non-zero)
                    sub_raw = s.get("sub_scores", {})
                    sub_compact = {k: v for k, v in sub_raw.items() if v != 0}

                    # Indicatori chiave compatti (non tutto il dict indicators)
                    ind_raw = s.get("indicators", {})
                    ind_compact = {}
                    for k in ("rsi", "adx", "macd_hist", "obv_trend", "bb_pos", "stoch_k", "vol_signal", "ma_cross", "atr_regime"):
                        if ind_raw.get(k) is not None:
                            ind_compact[k] = ind_raw[k]
                    # Aggiungi risk_metrics se presenti
                    rm = s.get("risk_metrics", {})
                    if rm:
                        ind_compact["sharpe"] = rm.get("sharpe_1y")
                        ind_compact["beta"] = rm.get("beta")
                        ind_compact["maxdd"] = rm.get("max_drawdown_1y_pct")

                    cur.execute("""
                        INSERT INTO signal_history
                            (run_id, symbol, action, score, composite_score, confidence,
                             price, entry, stop_loss, take_profit, risk_reward,
                             market, asset_type, sub_scores_json, indicators_json)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        run_id,
                        s.get("symbol", ""),
                        s.get("action", ""),
                        s.get("score", 0),
                        s.get("composite_score", s.get("score", 0)),
                        s.get("confidence", 0),
                        _safe(s.get("price")),
                        _safe(s.get("entry")),
                        _safe(s.get("stop_loss")),
                        _safe(s.get("take_profit")),
                        _safe(s.get("risk_reward")),
                        s.get("market", ""),
                        s.get("asset_type", ""),
                        json.dumps(sub_compact) if sub_compact else None,
                        json.dumps(ind_compact) if ind_compact else None,
                    ))
        conn.close()
        log.info(f"[DB] Analisi salvata: run_id={run_id} | {len(signals)} signals | "
                 f"BUY={buys} SELL={sells} WATCH={watch}")
        return run_id
    except Exception as e:
        log.error(f"[DB] save_analysis_run: {e}")
        return None


def save_smart_money(data: Dict) -> bool:
    """Salva un'analisi Smart Money nello storico."""
    if not _enabled:
        return False
    try:
        regime = "?"
        if isinstance(data.get("macro_regime"), dict):
            regime = data["macro_regime"].get("rate_environment", "?")
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO smart_money_history
                    (data_quality, opportunities_count, macro_regime, analysis_json)
                VALUES (%s, %s, %s, %s)
            """, (
                data.get("data_quality", "?"),
                len(data.get("opportunities", [])),
                regime,
                json.dumps(data, default=str),
            ))
        conn.close()
        return True
    except Exception as e:
        log.error(f"[DB] save_smart_money: {e}")
        return False


def save_wallet_analysis_run(payload: Dict) -> Optional[int]:
    if not _enabled:
        return None
    try:
        holdings = payload.get("holdings", []) or []
        summary  = payload.get("summary", {}) or {}
        alerts   = payload.get("alerts", []) or []
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wallet_analysis_runs
                    (holdings_count, invested_total, market_value_total, pnl_total,
                     pnl_pct_total, alert_count, analysis_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                len(holdings),
                summary.get("invested_total", 0),
                summary.get("market_value_total", 0),
                summary.get("pnl_total", 0),
                summary.get("pnl_pct_total", 0),
                len(alerts),
                json.dumps(payload, default=str),
            ))
            run_id = cur.lastrowid

            for item in holdings:
                cur.execute("""
                    INSERT INTO wallet_snapshot_items
                        (run_id, symbol, quantity, avg_price, current_price, invested_amount,
                         market_value, pnl_amount, pnl_pct, signal_action, recommendation,
                         confidence, holding_days_estimate, analysis_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    run_id,
                    item.get("symbol", ""),
                    item.get("quantity", 0),
                    item.get("avg_price", 0),
                    item.get("current_price"),
                    item.get("invested_amount", 0),
                    item.get("market_value", 0),
                    item.get("pnl_amount", 0),
                    item.get("pnl_pct", 0),
                    item.get("signal_action", ""),
                    item.get("recommendation", ""),
                    item.get("confidence", 0),
                    item.get("holding_days_estimate", 0),
                    json.dumps(item, default=str),
                ))
        conn.close()
        return run_id
    except Exception as e:
        log.error(f"[DB] save_wallet_analysis_run: {e}")
        return None


def get_wallet_history(days: int = 7) -> List[Dict]:
    if not _enabled:
        return []
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, run_at, holdings_count, invested_total, market_value_total,
                       pnl_total, pnl_pct_total, alert_count
                FROM wallet_analysis_runs
                WHERE run_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY run_at DESC
                LIMIT 500
            """, (days,))
            rows = cur.fetchall()
        conn.close()
        out = []
        for r in rows:
            d = dict(r)
            if d.get("run_at") and hasattr(d["run_at"], "isoformat"):
                d["run_at"] = d["run_at"].isoformat()
            for key in ("invested_total", "market_value_total", "pnl_total", "pnl_pct_total"):
                if d.get(key) is not None:
                    d[key] = float(d[key])
            out.append(d)
        return out
    except Exception as e:
        log.error(f"[DB] get_wallet_history: {e}")
        return []


def get_wallet_position_history(symbol: str, days: int = 30) -> List[Dict]:
    if not _enabled:
        return []
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT run_at, current_price, market_value, pnl_amount, pnl_pct,
                       signal_action, recommendation, confidence, holding_days_estimate
                FROM wallet_snapshot_items
                WHERE symbol=%s
                  AND run_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY run_at DESC
                LIMIT 500
            """, (symbol.upper(), days))
            rows = cur.fetchall()
        conn.close()
        out = []
        for r in rows:
            d = dict(r)
            if d.get("run_at") and hasattr(d["run_at"], "isoformat"):
                d["run_at"] = d["run_at"].isoformat()
            for key in ("current_price", "market_value", "pnl_amount", "pnl_pct"):
                if d.get(key) is not None:
                    d[key] = float(d[key])
            out.append(d)
        return out
    except Exception as e:
        log.error(f"[DB] get_wallet_position_history ({symbol}): {e}")
        return []


def get_recent_wallet_alert(symbol: str, alert_type: str, within_hours: int = 12) -> Optional[Dict]:
    if not _enabled:
        return None
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT created_at, recommendation, confidence, payload_json
                FROM wallet_alerts
                WHERE symbol=%s AND alert_type=%s
                  AND created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
                ORDER BY created_at DESC
                LIMIT 1
            """, (symbol.upper(), alert_type, within_hours))
            row = cur.fetchone()
        conn.close()
        if not row:
            return None
        out = dict(row)
        if out.get("created_at") and hasattr(out["created_at"], "isoformat"):
            out["created_at"] = out["created_at"].isoformat()
        if out.get("payload_json"):
            try:
                out["payload"] = json.loads(out["payload_json"])
            except Exception:
                out["payload"] = {}
        return out
    except Exception as e:
        log.error(f"[DB] get_recent_wallet_alert ({symbol}, {alert_type}): {e}")
        return None


def save_wallet_alert(symbol: str, alert_type: str, recommendation: str, confidence: int, payload: Dict) -> bool:
    if not _enabled:
        return False
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wallet_alerts
                    (symbol, alert_type, recommendation, confidence, payload_json)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                symbol.upper(),
                alert_type,
                recommendation,
                confidence,
                json.dumps(payload, default=str),
            ))
        conn.close()
        return True
    except Exception as e:
        log.error(f"[DB] save_wallet_alert ({symbol}, {alert_type}): {e}")
        return False


def save_income_plan(plan: Dict) -> Optional[int]:
    if not _enabled:
        return None
    try:
        summary = plan.get("summary", {})
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO income_plan_runs
                    (scenario, risk_level, monthly_income_est, target_capital, gap_to_target, plan_json)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                plan.get("market_scenario", ""),
                plan.get("risk_level", ""),
                summary.get("estimated_monthly_income", 0),
                summary.get("target_capital_for_goal", 0),
                summary.get("gap_to_target", 0),
                json.dumps(plan, default=str),
            ))
            run_id = cur.lastrowid
        conn.close()
        return run_id
    except Exception as e:
        log.error(f"[DB] save_income_plan: {e}")
        return None


def get_latest_crypto_alert(symbol: str) -> Optional[Dict]:
    if not _enabled:
        return None
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT created_at, action, confidence, payload_json
                FROM crypto_alerts
                WHERE symbol=%s
                ORDER BY created_at DESC
                LIMIT 1
            """, (symbol.upper(),))
            row = cur.fetchone()
        conn.close()
        if not row:
            return None
        out = dict(row)
        if out.get("created_at") and hasattr(out["created_at"], "isoformat"):
            out["created_at"] = out["created_at"].isoformat()
        return out
    except Exception as e:
        log.error(f"[DB] get_latest_crypto_alert ({symbol}): {e}")
        return None


def save_crypto_alert(symbol: str, action: str, confidence: int, payload: Dict) -> bool:
    if not _enabled:
        return False
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crypto_alerts
                    (symbol, action, confidence, payload_json)
                VALUES (%s, %s, %s, %s)
            """, (
                symbol.upper(),
                action,
                confidence,
                json.dumps(payload, default=str),
            ))
        conn.close()
        return True
    except Exception as e:
        log.error(f"[DB] save_crypto_alert ({symbol}): {e}")
        return False


def get_income_history(days: int = 7) -> List[Dict]:
    if not _enabled:
        return []
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, run_at, scenario, risk_level, monthly_income_est, target_capital, gap_to_target
                FROM income_plan_runs
                WHERE run_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY run_at DESC
                LIMIT 200
            """, (days,))
            rows = cur.fetchall()
        conn.close()
        out = []
        for r in rows:
            d = dict(r)
            if d.get("run_at") and hasattr(d["run_at"], "isoformat"):
                d["run_at"] = d["run_at"].isoformat()
            for key in ("monthly_income_est", "target_capital", "gap_to_target"):
                if d.get(key) is not None:
                    d[key] = float(d[key])
            out.append(d)
        return out
    except Exception as e:
        log.error(f"[DB] get_income_history: {e}")
        return []


# ── Query trend ────────────────────────────────────────────────────────────────

def get_signal_trend(symbol: str, days: int = 30) -> List[Dict]:
    """
    Storico segnali per un asset (per visualizzazione trend).
    Ritorna lista ordinata dal più recente.
    """
    if not _enabled:
        return []
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT run_at, action, score, composite_score, confidence,
                       price, entry, stop_loss, take_profit, risk_reward,
                       sub_scores_json, indicators_json
                FROM signal_history
                WHERE symbol=%s
                  AND run_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY run_at DESC
                LIMIT 500
            """, (symbol.upper(), days))
            rows = cur.fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("run_at") and hasattr(d["run_at"], "isoformat"):
                d["run_at"] = d["run_at"].isoformat()
            result.append(d)
        return result
    except Exception as e:
        log.error(f"[DB] get_signal_trend ({symbol}): {e}")
        return []


def get_history_compact(symbols: List[str], limit_per_symbol: int = 5) -> Dict[str, List[Dict]]:
    """
    Per ogni symbol, ritorna gli ultimi N segnali dal DB.
    Usato internamente da run_scan() per il lookback storico e il momentum adjustment.
    Ritorna: {SYMBOL_UPPER: [{run_at, action, composite_score, sub_scores_json, indicators_json}]}
    """
    if not _enabled or not symbols:
        return {}
    try:
        # Normalizza e deduplica
        syms = list({s.upper() for s in symbols if s})
        placeholders = ",".join(["%s"] * len(syms))
        conn = _connect()
        with conn.cursor() as cur:
            # Fetch last N per symbol usando variabile di sessione per row_number emulato
            cur.execute(f"""
                SELECT symbol, run_at, action, score, composite_score,
                       confidence, sub_scores_json, indicators_json
                FROM (
                    SELECT *,
                           @rn := IF(@prev = symbol, @rn + 1, 1) AS rn,
                           @prev := symbol
                    FROM signal_history
                    CROSS JOIN (SELECT @rn:=0, @prev:='') AS init
                    WHERE symbol IN ({placeholders})
                    ORDER BY symbol, run_at DESC
                ) ranked
                WHERE rn <= %s
                ORDER BY symbol, run_at DESC
            """, syms + [limit_per_symbol])
            rows = cur.fetchall()
        conn.close()

        out: Dict[str, List[Dict]] = {}
        for r in rows:
            sym = r["symbol"].upper()
            d = {
                "run_at":         r["run_at"].isoformat() if hasattr(r.get("run_at"), "isoformat") else str(r.get("run_at", "")),
                "action":         r.get("action", ""),
                "composite_score":r.get("composite_score") or r.get("score", 0),
                "confidence":     r.get("confidence", 0),
                "sub_scores":     json.loads(r["sub_scores_json"]) if r.get("sub_scores_json") else {},
                "indicators":     json.loads(r["indicators_json"])  if r.get("indicators_json")  else {},
            }
            out.setdefault(sym, []).append(d)
        return out
    except Exception as e:
        log.error(f"[DB] get_history_compact: {e}")
        return {}


def get_analysis_summary(days: int = 7) -> List[Dict]:
    """Riepilogo degli ultimi run di analisi."""
    if not _enabled:
        return []
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, run_at, assets_count, active_count,
                       buy_count, sell_count, watchlist_count
                FROM analysis_runs
                WHERE run_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY run_at DESC
                LIMIT 200
            """, (days,))
            rows = cur.fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("run_at") and hasattr(d["run_at"], "isoformat"):
                d["run_at"] = d["run_at"].isoformat()
            result.append(d)
        return result
    except Exception as e:
        log.error(f"[DB] get_analysis_summary: {e}")
        return []


def get_smart_money_history(limit: int = 10) -> List[Dict]:
    """Riepilogo delle ultime analisi Smart Money."""
    if not _enabled:
        return []
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, computed_at, data_quality, opportunities_count, macro_regime
                FROM smart_money_history
                ORDER BY computed_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("computed_at") and hasattr(d["computed_at"], "isoformat"):
                d["computed_at"] = d["computed_at"].isoformat()
            result.append(d)
        return result
    except Exception as e:
        log.error(f"[DB] get_smart_money_history: {e}")
        return []


def get_top_signals_trend(days: int = 7, min_score: int = 25) -> List[Dict]:
    """
    Ritorna gli asset che appaiono più spesso come BUY/SELL negli ultimi N giorni.
    Utile per identificare trend di consensus.
    """
    if not _enabled:
        return []
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, action, market, asset_type,
                       COUNT(*)               AS appearances,
                       AVG(score)             AS avg_score,
                       MAX(score)             AS max_score,
                       AVG(confidence)        AS avg_confidence,
                       MAX(run_at)            AS last_seen,
                       MIN(price)             AS price_low,
                       MAX(price)             AS price_high
                FROM signal_history
                WHERE run_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                  AND score >= %s
                  AND action IN ('BUY','SELL','WATCHLIST')
                GROUP BY symbol, action, market, asset_type
                ORDER BY appearances DESC, avg_score DESC
                LIMIT 50
            """, (days, min_score))
            rows = cur.fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("last_seen") and hasattr(d["last_seen"], "isoformat"):
                d["last_seen"] = d["last_seen"].isoformat()
            for k in ("avg_score", "max_score", "avg_confidence", "price_low", "price_high"):
                if d.get(k) is not None:
                    d[k] = round(float(d[k]), 2)
            result.append(d)
        return result
    except Exception as e:
        log.error(f"[DB] get_top_signals_trend: {e}")
        return []
