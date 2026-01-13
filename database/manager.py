"""
Database Manager

Handles all database operations for the Multi-Broker Arbitrage System.
Uses SQLite with both sync and async interfaces.
"""

import sqlite3
import asyncio
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path
import logging
import json
from contextlib import contextmanager

from .models import (
    PriceHistory,
    TradingConfig,
    Trade,
    SDTouchLog,
    LimitOrderLog,
    Broker,
)

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    SQLite database manager for trading system.

    Provides methods for:
    - Schema initialization
    - CRUD operations for all tables
    - Transaction management
    - Data export/import
    """

    # SQL Schema definitions
    SCHEMA_SQL = """
    -- Price history for mean/std calculation
    CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        asset TEXT DEFAULT 'ACTIVE',
        spot_price REAL,
        futures_price REAL,
        spread REAL,
        swap_diff REAL
    );

    -- Master trading configuration (singleton)
    CREATE TABLE IF NOT EXISTS trading_config (
        id INTEGER PRIMARY KEY CHECK (id = 1),

        -- Asset Configuration
        asset_name TEXT DEFAULT 'GOLD',
        spot_symbol TEXT DEFAULT 'XAUUSD',
        futures_symbol TEXT DEFAULT 'GC0226',
        futures_expiry TEXT,
        contract_size REAL DEFAULT 100,
        swap_charge REAL DEFAULT 0,

        -- Signal Parameters
        lookback_period INTEGER DEFAULT 90,
        lookback_unit TEXT DEFAULT 'minutes',
        entry_std_dev REAL DEFAULT 2.0,
        exit_std_dev REAL DEFAULT 0.5,
        stop_loss_std_dev REAL DEFAULT 3.0,
        exit_at_opposite_sd REAL DEFAULT 0,

        -- Risk Management
        time_stop_loss_days REAL DEFAULT 0,
        max_positions INTEGER DEFAULT 3,
        lot_size REAL DEFAULT 0.1,
        commission_per_lot REAL DEFAULT 0,
        min_profit_per_lot REAL DEFAULT 50,
        max_loss_per_lot REAL DEFAULT 100,

        -- Hurst Exponent Filter
        hurst_enabled INTEGER DEFAULT 1,
        hurst_threshold REAL DEFAULT 0.5,
        trending_duration_minutes INTEGER DEFAULT 15,

        -- Overnight Protection
        close_before_overnight INTEGER DEFAULT 0,
        overnight_close_hour INTEGER DEFAULT 16,
        overnight_close_minute INTEGER DEFAULT 55,

        -- Order Execution
        order_type TEXT DEFAULT 'MARKET',
        limit_order_timeout INTEGER DEFAULT 60,
        limit_peg_interval REAL DEFAULT 1.5,

        -- Mode Settings
        algo_enabled INTEGER DEFAULT 0,
        paper_mode INTEGER DEFAULT 1,
        selected_asset TEXT DEFAULT 'GOLD'
    );

    -- Trade journal with cross-broker references
    CREATE TABLE IF NOT EXISTS trades (
        trade_id TEXT PRIMARY KEY,
        asset TEXT,
        direction TEXT,

        -- Timestamps
        entry_date TEXT,
        exit_date TEXT,
        days_held REAL,

        -- Z-Scores
        entry_zscore REAL,
        exit_zscore REAL,

        -- Entry Prices
        entry_spot_price REAL,
        entry_futures_price REAL,

        -- Exit Prices
        exit_spot_price REAL,
        exit_futures_price REAL,

        -- P&L Components
        spot_pnl REAL,
        futures_pnl REAL,
        gross_pnl REAL,
        swap_cost REAL,
        commission REAL,
        spread_cost REAL,
        net_pnl REAL,
        return_pct REAL,

        -- Position Details
        lot_size REAL,

        -- Multi-Broker References (NEW)
        spot_broker_id TEXT,
        mt5_spot_ticket INTEGER,
        futures_broker_id TEXT,
        mt5_futures_ticket INTEGER,

        -- Status
        order_status TEXT,
        status TEXT DEFAULT 'OPEN'
    );

    -- SD touch tracking
    CREATE TABLE IF NOT EXISTS sd_touch_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        asset TEXT,
        touch_date TEXT,
        touch_time TEXT,
        sd_level TEXT,
        direction TEXT,
        touch_spread REAL,
        touch_zscore REAL,
        mean_at_touch REAL,
        std_at_touch REAL,
        reached_mean INTEGER DEFAULT 0,
        mean_reached_time TEXT,
        spread_at_mean REAL,
        potential_profit REAL,
        max_adverse_move REAL,
        status TEXT DEFAULT 'PENDING',
        entry_spot_spread REAL,
        entry_futures_spread REAL,
        exit_spot_spread REAL,
        exit_futures_spread REAL
    );

    -- Limit order execution log
    CREATE TABLE IF NOT EXISTS limit_order_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        broker_id TEXT,
        symbol TEXT,
        order_type TEXT,
        side TEXT,
        volume REAL,
        target_price REAL,
        fill_price REAL,
        status TEXT,
        elapsed_seconds REAL,
        iterations INTEGER,
        error_message TEXT,
        context TEXT
    );

    -- Multi-broker configuration
    CREATE TABLE IF NOT EXISTS brokers (
        broker_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        broker_type TEXT DEFAULT 'MT5',
        role TEXT NOT NULL,

        -- MT5 specific
        mt5_path TEXT,
        mt5_account INTEGER,
        mt5_server TEXT,
        mt5_password TEXT,

        -- FIX specific
        fix_host TEXT,
        fix_port INTEGER,
        fix_sender_comp TEXT,
        fix_target_comp TEXT,
        fix_username TEXT,
        fix_password TEXT,

        -- FlexTrade specific
        flex_host TEXT,
        flex_port INTEGER,
        flex_api_key TEXT,

        -- IB specific
        ib_host TEXT,
        ib_port INTEGER,
        ib_client_id INTEGER,

        -- Common
        symbol TEXT NOT NULL,
        contract_size REAL DEFAULT 100,
        commission_per_lot REAL DEFAULT 0,
        min_volume REAL DEFAULT 0.01,

        -- Status
        status TEXT DEFAULT 'DISCONNECTED',
        last_heartbeat TEXT,
        latency_ms INTEGER,

        -- Additional config
        config_json TEXT
    );

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_price_history_timestamp ON price_history(timestamp);
    CREATE INDEX IF NOT EXISTS idx_price_history_asset ON price_history(asset);
    CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
    CREATE INDEX IF NOT EXISTS idx_trades_entry_date ON trades(entry_date);
    CREATE INDEX IF NOT EXISTS idx_sd_touch_log_asset ON sd_touch_log(asset);
    CREATE INDEX IF NOT EXISTS idx_sd_touch_log_status ON sd_touch_log(status);
    CREATE INDEX IF NOT EXISTS idx_limit_order_log_broker ON limit_order_log(broker_id);
    CREATE INDEX IF NOT EXISTS idx_brokers_role ON brokers(role);
    """

    def __init__(self, db_path: str = "trading.db"):
        """
        Initialize database manager.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self._connection: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection"""
        if self._connection is None:
            self._connection = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False
            )
            self._connection.row_factory = sqlite3.Row
        return self._connection

    @contextmanager
    def _cursor(self):
        """Context manager for database cursor"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()

    def initialize(self) -> bool:
        """
        Initialize database schema.

        Returns:
            True if successful
        """
        try:
            with self._cursor() as cursor:
                cursor.executescript(self.SCHEMA_SQL)

            # Ensure default config exists
            self._ensure_default_config()

            logger.info(f"Database initialized: {self.db_path}")
            return True

        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            return False

    def _ensure_default_config(self):
        """Ensure default trading config exists"""
        with self._cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM trading_config WHERE id = 1")
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    INSERT INTO trading_config (id) VALUES (1)
                """)

    def close(self):
        """Close database connection"""
        if self._connection:
            self._connection.close()
            self._connection = None

    # ==================== Price History ====================

    def add_price_history(self, price: PriceHistory) -> int:
        """Add price history record"""
        with self._cursor() as cursor:
            cursor.execute("""
                INSERT INTO price_history (timestamp, asset, spot_price, futures_price, spread, swap_diff)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (price.timestamp, price.asset, price.spot_price, price.futures_price,
                  price.spread, price.swap_diff))
            return cursor.lastrowid

    def get_price_history(
        self,
        asset: str = "ACTIVE",
        limit: int = 1000,
        lookback_minutes: Optional[int] = None
    ) -> List[PriceHistory]:
        """Get price history records"""
        with self._cursor() as cursor:
            if lookback_minutes:
                cursor.execute("""
                    SELECT * FROM price_history
                    WHERE asset = ? AND timestamp >= datetime('now', ? || ' minutes')
                    ORDER BY timestamp DESC LIMIT ?
                """, (asset, -lookback_minutes, limit))
            else:
                cursor.execute("""
                    SELECT * FROM price_history
                    WHERE asset = ?
                    ORDER BY timestamp DESC LIMIT ?
                """, (asset, limit))

            return [PriceHistory(**dict(row)) for row in cursor.fetchall()]

    def clear_price_history(self, asset: str = "ACTIVE") -> int:
        """Clear price history for asset"""
        with self._cursor() as cursor:
            cursor.execute("DELETE FROM price_history WHERE asset = ?", (asset,))
            return cursor.rowcount

    # ==================== Trading Config ====================

    def get_config(self) -> TradingConfig:
        """Get trading configuration"""
        with self._cursor() as cursor:
            cursor.execute("SELECT * FROM trading_config WHERE id = 1")
            row = cursor.fetchone()
            if row:
                data = dict(row)
                # Convert SQLite integers to bools
                data['hurst_enabled'] = bool(data.get('hurst_enabled', 1))
                data['close_before_overnight'] = bool(data.get('close_before_overnight', 0))
                data['algo_enabled'] = bool(data.get('algo_enabled', 0))
                data['paper_mode'] = bool(data.get('paper_mode', 1))
                return TradingConfig(**data)
            return TradingConfig()

    def update_config(self, config: TradingConfig) -> bool:
        """Update trading configuration"""
        with self._cursor() as cursor:
            cursor.execute("""
                UPDATE trading_config SET
                    asset_name = ?, spot_symbol = ?, futures_symbol = ?, futures_expiry = ?,
                    contract_size = ?, swap_charge = ?, lookback_period = ?, lookback_unit = ?,
                    entry_std_dev = ?, exit_std_dev = ?, stop_loss_std_dev = ?, exit_at_opposite_sd = ?,
                    time_stop_loss_days = ?, max_positions = ?, lot_size = ?, commission_per_lot = ?,
                    min_profit_per_lot = ?, max_loss_per_lot = ?, hurst_enabled = ?, hurst_threshold = ?,
                    trending_duration_minutes = ?, close_before_overnight = ?, overnight_close_hour = ?,
                    overnight_close_minute = ?, order_type = ?, limit_order_timeout = ?,
                    limit_peg_interval = ?, algo_enabled = ?, paper_mode = ?, selected_asset = ?
                WHERE id = 1
            """, (
                config.asset_name, config.spot_symbol, config.futures_symbol, config.futures_expiry,
                config.contract_size, config.swap_charge, config.lookback_period, config.lookback_unit,
                config.entry_std_dev, config.exit_std_dev, config.stop_loss_std_dev, config.exit_at_opposite_sd,
                config.time_stop_loss_days, config.max_positions, config.lot_size, config.commission_per_lot,
                config.min_profit_per_lot, config.max_loss_per_lot, int(config.hurst_enabled), config.hurst_threshold,
                config.trending_duration_minutes, int(config.close_before_overnight), config.overnight_close_hour,
                config.overnight_close_minute, config.order_type, config.limit_order_timeout,
                config.limit_peg_interval, int(config.algo_enabled), int(config.paper_mode), config.selected_asset
            ))
            return True

    def update_config_field(self, field: str, value: Any) -> bool:
        """Update single config field"""
        allowed_fields = set(TradingConfig.__dataclass_fields__.keys())
        if field not in allowed_fields:
            raise ValueError(f"Invalid config field: {field}")

        with self._cursor() as cursor:
            cursor.execute(f"UPDATE trading_config SET {field} = ? WHERE id = 1", (value,))
            return True

    # ==================== Trades ====================

    def add_trade(self, trade: Trade) -> bool:
        """Add new trade record"""
        with self._cursor() as cursor:
            cursor.execute("""
                INSERT INTO trades (
                    trade_id, asset, direction, entry_date, exit_date, days_held,
                    entry_zscore, exit_zscore, entry_spot_price, entry_futures_price,
                    exit_spot_price, exit_futures_price, spot_pnl, futures_pnl,
                    gross_pnl, swap_cost, commission, spread_cost, net_pnl, return_pct,
                    lot_size, spot_broker_id, mt5_spot_ticket, futures_broker_id,
                    mt5_futures_ticket, order_status, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.trade_id, trade.asset, trade.direction, trade.entry_date,
                trade.exit_date, trade.days_held, trade.entry_zscore, trade.exit_zscore,
                trade.entry_spot_price, trade.entry_futures_price, trade.exit_spot_price,
                trade.exit_futures_price, trade.spot_pnl, trade.futures_pnl, trade.gross_pnl,
                trade.swap_cost, trade.commission, trade.spread_cost, trade.net_pnl,
                trade.return_pct, trade.lot_size, trade.spot_broker_id, trade.mt5_spot_ticket,
                trade.futures_broker_id, trade.mt5_futures_ticket, trade.order_status, trade.status
            ))
            return True

    def get_trade(self, trade_id: str) -> Optional[Trade]:
        """Get trade by ID"""
        with self._cursor() as cursor:
            cursor.execute("SELECT * FROM trades WHERE trade_id = ?", (trade_id,))
            row = cursor.fetchone()
            return Trade(**dict(row)) if row else None

    def get_trades(
        self,
        status: Optional[str] = None,
        asset: Optional[str] = None,
        limit: int = 100
    ) -> List[Trade]:
        """Get trades with optional filters"""
        with self._cursor() as cursor:
            query = "SELECT * FROM trades WHERE 1=1"
            params = []

            if status:
                query += " AND status = ?"
                params.append(status)
            if asset:
                query += " AND asset = ?"
                params.append(asset)

            query += " ORDER BY entry_date DESC LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)
            return [Trade(**dict(row)) for row in cursor.fetchall()]

    def get_open_trades(self) -> List[Trade]:
        """Get all open trades"""
        return self.get_trades(status="OPEN")

    def update_trade(self, trade: Trade) -> bool:
        """Update existing trade"""
        with self._cursor() as cursor:
            cursor.execute("""
                UPDATE trades SET
                    exit_date = ?, days_held = ?, exit_zscore = ?,
                    exit_spot_price = ?, exit_futures_price = ?,
                    spot_pnl = ?, futures_pnl = ?, gross_pnl = ?,
                    swap_cost = ?, commission = ?, spread_cost = ?,
                    net_pnl = ?, return_pct = ?, order_status = ?, status = ?
                WHERE trade_id = ?
            """, (
                trade.exit_date, trade.days_held, trade.exit_zscore,
                trade.exit_spot_price, trade.exit_futures_price,
                trade.spot_pnl, trade.futures_pnl, trade.gross_pnl,
                trade.swap_cost, trade.commission, trade.spread_cost,
                trade.net_pnl, trade.return_pct, trade.order_status,
                trade.status, trade.trade_id
            ))
            return cursor.rowcount > 0

    def clear_trades(self) -> int:
        """Clear all trades"""
        with self._cursor() as cursor:
            cursor.execute("DELETE FROM trades")
            return cursor.rowcount

    # ==================== SD Touch Log ====================

    def add_sd_touch(self, touch: SDTouchLog) -> int:
        """Add SD touch record"""
        with self._cursor() as cursor:
            cursor.execute("""
                INSERT INTO sd_touch_log (
                    asset, touch_date, touch_time, sd_level, direction,
                    touch_spread, touch_zscore, mean_at_touch, std_at_touch,
                    reached_mean, mean_reached_time, spread_at_mean, potential_profit,
                    max_adverse_move, status, entry_spot_spread, entry_futures_spread,
                    exit_spot_spread, exit_futures_spread
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                touch.asset, touch.touch_date, touch.touch_time, touch.sd_level,
                touch.direction, touch.touch_spread, touch.touch_zscore,
                touch.mean_at_touch, touch.std_at_touch, int(touch.reached_mean),
                touch.mean_reached_time, touch.spread_at_mean, touch.potential_profit,
                touch.max_adverse_move, touch.status, touch.entry_spot_spread,
                touch.entry_futures_spread, touch.exit_spot_spread, touch.exit_futures_spread
            ))
            return cursor.lastrowid

    def get_sd_touches(
        self,
        asset: Optional[str] = None,
        sd_level: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[SDTouchLog]:
        """Get SD touch records with filters"""
        with self._cursor() as cursor:
            query = "SELECT * FROM sd_touch_log WHERE 1=1"
            params = []

            if asset:
                query += " AND asset = ?"
                params.append(asset)
            if sd_level:
                query += " AND sd_level = ?"
                params.append(sd_level)
            if status:
                query += " AND status = ?"
                params.append(status)

            query += " ORDER BY touch_date DESC, touch_time DESC LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [SDTouchLog(**{**dict(row), 'reached_mean': bool(row['reached_mean'])})
                    for row in rows]

    def update_sd_touch(self, touch: SDTouchLog) -> bool:
        """Update SD touch record"""
        with self._cursor() as cursor:
            cursor.execute("""
                UPDATE sd_touch_log SET
                    reached_mean = ?, mean_reached_time = ?, spread_at_mean = ?,
                    potential_profit = ?, max_adverse_move = ?, status = ?,
                    exit_spot_spread = ?, exit_futures_spread = ?
                WHERE id = ?
            """, (
                int(touch.reached_mean), touch.mean_reached_time, touch.spread_at_mean,
                touch.potential_profit, touch.max_adverse_move, touch.status,
                touch.exit_spot_spread, touch.exit_futures_spread, touch.id
            ))
            return cursor.rowcount > 0

    def get_sd_touch_stats(self, asset: Optional[str] = None) -> Dict[str, Any]:
        """Get SD touch statistics by level"""
        with self._cursor() as cursor:
            query = """
                SELECT
                    sd_level,
                    COUNT(*) as total_touches,
                    SUM(CASE WHEN reached_mean = 1 THEN 1 ELSE 0 END) as reached_mean_count,
                    AVG(potential_profit) as avg_profit,
                    AVG(max_adverse_move) as avg_adverse
                FROM sd_touch_log
                WHERE 1=1
            """
            params = []
            if asset:
                query += " AND asset = ?"
                params.append(asset)

            query += " GROUP BY sd_level ORDER BY sd_level"
            cursor.execute(query, params)

            stats = {}
            for row in cursor.fetchall():
                level = row['sd_level']
                total = row['total_touches']
                reached = row['reached_mean_count']
                stats[level] = {
                    'total': total,
                    'reached_mean': reached,
                    'success_rate': (reached / total * 100) if total > 0 else 0,
                    'avg_profit': row['avg_profit'],
                    'avg_adverse': row['avg_adverse']
                }
            return stats

    def clear_sd_touches(self, asset: Optional[str] = None) -> int:
        """Clear SD touch records"""
        with self._cursor() as cursor:
            if asset:
                cursor.execute("DELETE FROM sd_touch_log WHERE asset = ?", (asset,))
            else:
                cursor.execute("DELETE FROM sd_touch_log")
            return cursor.rowcount

    # ==================== Limit Order Log ====================

    def add_limit_order_log(self, log: LimitOrderLog) -> int:
        """Add limit order log entry"""
        with self._cursor() as cursor:
            cursor.execute("""
                INSERT INTO limit_order_log (
                    timestamp, broker_id, symbol, order_type, side, volume,
                    target_price, fill_price, status, elapsed_seconds,
                    iterations, error_message, context
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                log.timestamp, log.broker_id, log.symbol, log.order_type,
                log.side, log.volume, log.target_price, log.fill_price,
                log.status, log.elapsed_seconds, log.iterations,
                log.error_message, log.context
            ))
            return cursor.lastrowid

    def get_limit_order_logs(
        self,
        broker_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[LimitOrderLog]:
        """Get limit order log entries"""
        with self._cursor() as cursor:
            query = "SELECT * FROM limit_order_log WHERE 1=1"
            params = []

            if broker_id:
                query += " AND broker_id = ?"
                params.append(broker_id)
            if status:
                query += " AND status = ?"
                params.append(status)

            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)
            return [LimitOrderLog(**dict(row)) for row in cursor.fetchall()]

    def get_limit_order_stats(self, broker_id: Optional[str] = None) -> Dict[str, Any]:
        """Get limit order fill statistics"""
        with self._cursor() as cursor:
            query = """
                SELECT
                    COUNT(*) as total_orders,
                    SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) as filled,
                    SUM(CASE WHEN status = 'TIMEOUT' THEN 1 ELSE 0 END) as timeout,
                    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as errors,
                    AVG(elapsed_seconds) as avg_time,
                    AVG(iterations) as avg_iterations
                FROM limit_order_log WHERE 1=1
            """
            params = []
            if broker_id:
                query += " AND broker_id = ?"
                params.append(broker_id)

            cursor.execute(query, params)
            row = cursor.fetchone()

            total = row['total_orders'] or 0
            filled = row['filled'] or 0

            return {
                'total_orders': total,
                'filled': filled,
                'timeout': row['timeout'] or 0,
                'errors': row['errors'] or 0,
                'fill_rate': (filled / total * 100) if total > 0 else 0,
                'avg_time_seconds': row['avg_time'],
                'avg_iterations': row['avg_iterations']
            }

    # ==================== Brokers ====================

    def add_broker(self, broker: Broker) -> bool:
        """Add or update broker configuration"""
        with self._cursor() as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO brokers (
                    broker_id, name, broker_type, role, mt5_path, mt5_account, mt5_server,
                    mt5_password, fix_host, fix_port, fix_sender_comp, fix_target_comp,
                    fix_username, fix_password, flex_host, flex_port, flex_api_key,
                    ib_host, ib_port, ib_client_id, symbol, contract_size,
                    commission_per_lot, min_volume, status, last_heartbeat, latency_ms, config_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                broker.broker_id, broker.name, broker.broker_type, broker.role,
                broker.mt5_path, broker.mt5_account, broker.mt5_server, broker.mt5_password,
                broker.fix_host, broker.fix_port, broker.fix_sender_comp, broker.fix_target_comp,
                broker.fix_username, broker.fix_password, broker.flex_host, broker.flex_port,
                broker.flex_api_key, broker.ib_host, broker.ib_port, broker.ib_client_id,
                broker.symbol, broker.contract_size, broker.commission_per_lot, broker.min_volume,
                broker.status, broker.last_heartbeat, broker.latency_ms, broker.config_json
            ))
            return True

    def get_broker(self, broker_id: str) -> Optional[Broker]:
        """Get broker by ID"""
        with self._cursor() as cursor:
            cursor.execute("SELECT * FROM brokers WHERE broker_id = ?", (broker_id,))
            row = cursor.fetchone()
            return Broker(**dict(row)) if row else None

    def get_brokers(self, role: Optional[str] = None) -> List[Broker]:
        """Get all brokers, optionally filtered by role"""
        with self._cursor() as cursor:
            if role:
                cursor.execute("SELECT * FROM brokers WHERE role = ?", (role,))
            else:
                cursor.execute("SELECT * FROM brokers")
            return [Broker(**dict(row)) for row in cursor.fetchall()]

    def get_spot_broker(self) -> Optional[Broker]:
        """Get configured spot broker"""
        brokers = self.get_brokers(role="SPOT")
        return brokers[0] if brokers else None

    def get_futures_broker(self) -> Optional[Broker]:
        """Get configured futures broker"""
        brokers = self.get_brokers(role="FUTURES")
        return brokers[0] if brokers else None

    def update_broker_status(
        self,
        broker_id: str,
        status: str,
        latency_ms: Optional[int] = None
    ) -> bool:
        """Update broker connection status"""
        with self._cursor() as cursor:
            cursor.execute("""
                UPDATE brokers SET
                    status = ?,
                    last_heartbeat = ?,
                    latency_ms = ?
                WHERE broker_id = ?
            """, (status, datetime.now().isoformat(), latency_ms, broker_id))
            return cursor.rowcount > 0

    def delete_broker(self, broker_id: str) -> bool:
        """Delete broker configuration"""
        with self._cursor() as cursor:
            cursor.execute("DELETE FROM brokers WHERE broker_id = ?", (broker_id,))
            return cursor.rowcount > 0

    # ==================== Export/Import ====================

    def export_trades_csv(self) -> str:
        """Export trades to CSV format"""
        trades = self.get_trades(limit=10000)
        if not trades:
            return "No trades found"

        headers = list(Trade.__dataclass_fields__.keys())
        lines = [",".join(headers)]

        for trade in trades:
            values = [str(getattr(trade, h, "")) for h in headers]
            lines.append(",".join(values))

        return "\n".join(lines)

    def get_trade_statistics(self) -> Dict[str, Any]:
        """Get overall trade statistics"""
        with self._cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) as closed_trades,
                    SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                    SUM(CASE WHEN net_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                    SUM(net_pnl) as total_pnl,
                    AVG(net_pnl) as avg_pnl,
                    MAX(net_pnl) as max_profit,
                    MIN(net_pnl) as max_loss,
                    AVG(days_held) as avg_hold_days
                FROM trades WHERE status = 'CLOSED'
            """)
            row = cursor.fetchone()

            total = row['closed_trades'] or 0
            wins = row['winning_trades'] or 0

            return {
                'total_trades': row['total_trades'],
                'closed_trades': total,
                'open_trades': row['total_trades'] - total if row['total_trades'] else 0,
                'winning_trades': wins,
                'losing_trades': row['losing_trades'] or 0,
                'win_rate': (wins / total * 100) if total > 0 else 0,
                'total_pnl': row['total_pnl'] or 0,
                'avg_pnl': row['avg_pnl'] or 0,
                'max_profit': row['max_profit'] or 0,
                'max_loss': row['max_loss'] or 0,
                'avg_hold_days': row['avg_hold_days'] or 0
            }
