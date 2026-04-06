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
            id           INT AUTO_INCREMENT PRIMARY KEY,
            run_id       INT,
            run_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            symbol       VARCHAR(32),
            action       VARCHAR(16),
            score        INT,
            confidence   INT,
            price        DECIMAL(18,4),
            entry        DECIMAL(18,4),
            stop_loss    DECIMAL(18,4),
            take_profit  DECIMAL(18,4),
            risk_reward  DECIMAL(6,2),
            market       VARCHAR(8),
            asset_type   VARCHAR(16),
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
    ]
    conn = _connect()
    try:
        with conn.cursor() as cur:
            for stmt in stmts:
                cur.execute(stmt.strip())
        log.info("[DB] Schema verificato/creato")
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

            # Righe individuali per trend analysis
            for s in signals:
                if s.get("action") in ("BUY", "SELL", "WATCHLIST", "HOLD"):
                    def _safe(v):
                        try:
                            return float(v) if v not in (None, "", "N/A") else None
                        except (TypeError, ValueError):
                            return None

                    cur.execute("""
                        INSERT INTO signal_history
                            (run_id, symbol, action, score, confidence,
                             price, entry, stop_loss, take_profit, risk_reward,
                             market, asset_type)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        run_id,
                        s.get("symbol", ""),
                        s.get("action", ""),
                        s.get("score", 0),
                        s.get("confidence", 0),
                        _safe(s.get("price")),
                        _safe(s.get("entry")),
                        _safe(s.get("stop_loss")),
                        _safe(s.get("take_profit")),
                        _safe(s.get("risk_reward")),
                        s.get("market", ""),
                        s.get("asset_type", ""),
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
                SELECT run_at, action, score, confidence,
                       price, entry, stop_loss, take_profit, risk_reward
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
