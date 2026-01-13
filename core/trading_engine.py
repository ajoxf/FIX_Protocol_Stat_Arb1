"""
Trading Engine

Main trading loop that coordinates:
- Price data collection
- Signal generation
- Position management
- Order execution
- Risk monitoring

This is the central controller that ties all components together.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
import logging

from .signals import SignalGenerator, Signal, SignalType, MarketRegime
from .orchestrator import OrderOrchestrator
from .ipc import IPCManager, IPCMessage, MessageType, CommandType
from database.manager import DatabaseManager
from database.models import TradingConfig, Trade, PriceHistory, SDTouchLog

logger = logging.getLogger(__name__)


class EngineState(Enum):
    """Trading engine states"""
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    ERROR = "ERROR"


@dataclass
class BrokerState:
    """State tracking for a broker connection"""
    broker_id: str
    role: str  # 'SPOT' or 'FUTURES'
    symbol: str
    status: str = "DISCONNECTED"
    latency_ms: Optional[float] = None
    last_tick: Optional[Dict[str, Any]] = None
    last_heartbeat: Optional[datetime] = None


@dataclass
class PositionState:
    """Current position state"""
    trade: Optional[Trade] = None
    entry_time: Optional[datetime] = None
    current_pnl: float = 0.0
    spot_price: float = 0.0
    futures_price: float = 0.0
    unrealized_spread: float = 0.0


@dataclass
class MarketState:
    """Current market state"""
    spot_bid: float = 0.0
    spot_ask: float = 0.0
    futures_bid: float = 0.0
    futures_ask: float = 0.0
    spread: float = 0.0
    zscore: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    hurst: Optional[float] = None
    regime: Optional[str] = None
    last_update: Optional[datetime] = None

    @property
    def is_valid(self) -> bool:
        """Check if market data is valid"""
        return (
            self.spot_bid > 0 and
            self.spot_ask > 0 and
            self.futures_bid > 0 and
            self.futures_ask > 0
        )


class TradingEngine:
    """
    Main trading engine coordinating all system components.

    Responsibilities:
    - Manage broker connections
    - Collect and process price data
    - Generate and act on signals
    - Track positions and P&L
    - Enforce risk limits
    - Handle overnight protection
    """

    # Main loop interval in seconds
    LOOP_INTERVAL = 0.5

    # Price history sampling interval
    PRICE_SAMPLE_INTERVAL = 1.0  # seconds

    def __init__(self, db_path: str = "trading.db", use_redis: bool = True):
        """
        Initialize trading engine.

        Args:
            db_path: Path to database
            use_redis: Use Redis for IPC if available
        """
        self.db_path = db_path
        self.use_redis = use_redis

        # Components
        self._db: Optional[DatabaseManager] = None
        self._ipc: Optional[IPCManager] = None
        self._signal_gen: Optional[SignalGenerator] = None
        self._orchestrator: Optional[OrderOrchestrator] = None

        # State
        self._state = EngineState.STOPPED
        self._config: Optional[TradingConfig] = None
        self._brokers: Dict[str, BrokerState] = {}
        self._market = MarketState()
        self._position = PositionState()

        # Callbacks for UI updates
        self._tick_callbacks: List[Callable] = []
        self._signal_callbacks: List[Callable] = []
        self._trade_callbacks: List[Callable] = []

        # Background tasks
        self._main_task: Optional[asyncio.Task] = None
        self._price_task: Optional[asyncio.Task] = None

        # Timing
        self._last_price_sample = datetime.now()

    async def initialize(self) -> bool:
        """Initialize all engine components"""
        logger.info("Initializing trading engine...")
        self._state = EngineState.STARTING

        try:
            # Initialize database
            self._db = DatabaseManager(self.db_path)
            self._db.initialize()

            # Load configuration
            self._config = self._db.get_config()

            # Initialize signal generator
            self._signal_gen = SignalGenerator(
                lookback_period=self._config.lookback_period,
                lookback_unit=self._config.lookback_unit,
                entry_threshold=self._config.entry_std_dev,
                exit_threshold=self._config.exit_std_dev,
                stop_loss_threshold=self._config.stop_loss_std_dev,
                hurst_enabled=self._config.hurst_enabled,
                hurst_threshold=self._config.hurst_threshold
            )

            # Load price history
            self._load_price_history()

            # Initialize IPC
            self._ipc = IPCManager(use_redis=self.use_redis)
            await self._ipc.initialize()

            # Subscribe to broker status
            await self._ipc.subscribe_status(self._handle_broker_status)

            # Initialize brokers from database
            self._init_brokers()

            # Initialize orchestrator
            spot_broker = self._get_broker_by_role("SPOT")
            futures_broker = self._get_broker_by_role("FUTURES")

            if spot_broker and futures_broker:
                self._orchestrator = OrderOrchestrator(
                    ipc=self._ipc,
                    db=self._db,
                    spot_broker_id=spot_broker.broker_id,
                    futures_broker_id=futures_broker.broker_id
                )

            # Load open positions
            self._load_open_positions()

            logger.info("Trading engine initialized")
            return True

        except Exception as e:
            logger.error(f"Engine initialization failed: {e}")
            self._state = EngineState.ERROR
            return False

    def _init_brokers(self) -> None:
        """Initialize broker state from database"""
        brokers = self._db.get_brokers()
        for broker in brokers:
            self._brokers[broker.broker_id] = BrokerState(
                broker_id=broker.broker_id,
                role=broker.role,
                symbol=broker.symbol,
                status=broker.status
            )

    def _get_broker_by_role(self, role: str) -> Optional[BrokerState]:
        """Get broker by role"""
        for broker in self._brokers.values():
            if broker.role == role:
                return broker
        return None

    def _load_price_history(self) -> None:
        """Load historical prices into signal generator"""
        history = self._db.get_price_history(limit=1000)
        for record in reversed(history):  # Oldest first
            if record.spread is not None:
                ts = datetime.fromisoformat(record.timestamp) if record.timestamp else datetime.now()
                self._signal_gen.add_spread(record.spread, ts)

        logger.info(f"Loaded {len(history)} price history records")

    def _load_open_positions(self) -> None:
        """Load any open positions"""
        open_trades = self._db.get_open_trades()
        if open_trades:
            trade = open_trades[0]  # Assume single position for now
            self._position.trade = trade
            if trade.entry_date:
                self._position.entry_time = datetime.fromisoformat(trade.entry_date)
            logger.info(f"Loaded open position: {trade.trade_id}")

    async def start(self) -> None:
        """Start the trading engine"""
        if self._state not in (EngineState.STOPPED, EngineState.PAUSED, EngineState.STARTING):
            logger.warning(f"Cannot start engine in state: {self._state}")
            return

        logger.info("Starting trading engine...")
        self._state = EngineState.RUNNING

        # Connect to brokers
        await self._connect_brokers()

        # Subscribe to price data
        await self._subscribe_prices()

        # Start main loop
        self._main_task = asyncio.create_task(self._main_loop())
        self._price_task = asyncio.create_task(self._price_sampling_loop())

        logger.info("Trading engine started")

    async def stop(self) -> None:
        """Stop the trading engine"""
        logger.info("Stopping trading engine...")
        self._state = EngineState.STOPPING

        # Cancel tasks
        if self._main_task:
            self._main_task.cancel()
        if self._price_task:
            self._price_task.cancel()

        # Disconnect brokers
        await self._disconnect_brokers()

        # Shutdown IPC
        if self._ipc:
            await self._ipc.shutdown()

        # Close database
        if self._db:
            self._db.close()

        self._state = EngineState.STOPPED
        logger.info("Trading engine stopped")

    async def pause(self) -> None:
        """Pause trading (keeps connections alive)"""
        self._state = EngineState.PAUSED
        logger.info("Trading engine paused")

    async def resume(self) -> None:
        """Resume trading from paused state"""
        if self._state == EngineState.PAUSED:
            self._state = EngineState.RUNNING
            logger.info("Trading engine resumed")

    async def _connect_brokers(self) -> None:
        """Send connect commands to all brokers"""
        for broker_id in self._brokers:
            await self._ipc.send_command(broker_id, CommandType.CONNECT)
            # Subscribe to symbol
            broker = self._brokers[broker_id]
            if broker.symbol:
                await self._ipc.send_command(
                    broker_id,
                    CommandType.SUBSCRIBE,
                    symbol=broker.symbol
                )

    async def _disconnect_brokers(self) -> None:
        """Send disconnect commands to all brokers"""
        for broker_id in self._brokers:
            await self._ipc.send_command(broker_id, CommandType.DISCONNECT)

    async def _subscribe_prices(self) -> None:
        """Subscribe to tick data from brokers"""
        for broker_id, broker in self._brokers.items():
            await self._ipc.subscribe_ticks(broker_id, self._handle_tick)

    def _handle_tick(self, message: IPCMessage) -> None:
        """Handle incoming tick data"""
        broker_id = message.broker_id
        payload = message.payload

        if broker_id not in self._brokers:
            return

        broker = self._brokers[broker_id]
        broker.last_tick = payload
        broker.last_heartbeat = datetime.now()

        # Update market state
        bid = payload.get('bid', 0)
        ask = payload.get('ask', 0)

        if broker.role == "SPOT":
            self._market.spot_bid = bid
            self._market.spot_ask = ask
        else:  # FUTURES
            self._market.futures_bid = bid
            self._market.futures_ask = ask

        self._market.last_update = datetime.now()

        # Calculate spread using mid prices
        if self._market.is_valid:
            spot_mid = (self._market.spot_bid + self._market.spot_ask) / 2
            futures_mid = (self._market.futures_bid + self._market.futures_ask) / 2
            self._market.spread = spot_mid - futures_mid

        # Notify callbacks
        for callback in self._tick_callbacks:
            try:
                callback(self._market)
            except Exception as e:
                logger.error(f"Tick callback error: {e}")

    def _handle_broker_status(self, message: IPCMessage) -> None:
        """Handle broker status updates"""
        broker_id = message.broker_id
        if broker_id in self._brokers:
            self._brokers[broker_id].status = message.payload.get('status', 'UNKNOWN')
            self._brokers[broker_id].latency_ms = message.payload.get('latency_ms')

    async def _main_loop(self) -> None:
        """Main trading loop"""
        logger.info("Main trading loop started")

        while self._state == EngineState.RUNNING:
            try:
                await self._trading_iteration()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Trading loop error: {e}")

            await asyncio.sleep(self.LOOP_INTERVAL)

        logger.info("Main trading loop stopped")

    async def _trading_iteration(self) -> None:
        """Single iteration of the trading loop"""
        # Check if algo is enabled
        if not self._config.algo_enabled:
            return

        # Check market data validity
        if not self._market.is_valid:
            return

        # Check broker connections
        if not self._all_brokers_connected():
            return

        # Check overnight protection
        if self._should_close_overnight():
            await self._handle_overnight_close()
            return

        # Update signal generator with current spread
        self._signal_gen.add_spread(self._market.spread)

        # Calculate current statistics
        stats = self._signal_gen.calculate_statistics()
        if stats:
            self._market.mean, self._market.std = stats
            self._market.zscore = self._signal_gen.last_zscore
            self._market.hurst = self._signal_gen.last_hurst
            self._market.regime = self._signal_gen.get_regime().value

        # Check for signals
        has_position = self._position.trade is not None

        if has_position:
            # Check exit signals
            await self._check_exit_signals()
        else:
            # Check entry signals
            await self._check_entry_signals()

    async def _check_entry_signals(self) -> None:
        """Check and act on entry signals"""
        signal = self._signal_gen.generate_entry_signal(
            current_spread=self._market.spread,
            has_position=False
        )

        # Update stats
        self._market.zscore = signal.zscore

        # Notify callbacks
        for callback in self._signal_callbacks:
            try:
                callback(signal)
            except Exception:
                pass

        # Act on signal
        if signal.is_entry:
            # Check position limits
            open_trades = self._db.get_open_trades()
            if len(open_trades) >= self._config.max_positions:
                logger.info(f"Max positions ({self._config.max_positions}) reached")
                return

            # Log SD touch
            self._log_sd_touch(signal)

            # Execute entry
            spot_broker = self._get_broker_by_role("SPOT")
            futures_broker = self._get_broker_by_role("FUTURES")

            if spot_broker and futures_broker and self._orchestrator:
                success, trade, error = await self._orchestrator.execute_entry(
                    signal=signal,
                    spot_symbol=spot_broker.symbol,
                    futures_symbol=futures_broker.symbol,
                    lot_size=self._config.lot_size,
                    order_type=self._config.order_type,
                    limit_timeout=self._config.limit_order_timeout,
                    limit_peg_interval=self._config.limit_peg_interval
                )

                if success and trade:
                    self._position.trade = trade
                    self._position.entry_time = datetime.now()

                    # Notify callbacks
                    for callback in self._trade_callbacks:
                        try:
                            callback("ENTRY", trade)
                        except Exception:
                            pass

    async def _check_exit_signals(self) -> None:
        """Check and act on exit signals"""
        trade = self._position.trade
        if not trade:
            return

        # Calculate current P&L
        self._update_position_pnl()

        signal = self._signal_gen.generate_exit_signal(
            current_spread=self._market.spread,
            entry_zscore=trade.entry_zscore or 0,
            position_direction="LONG" if "Long" in (trade.direction or "") else "SHORT",
            entry_time=self._position.entry_time,
            current_pnl=self._position.current_pnl,
            min_profit=self._config.min_profit_per_lot * self._config.lot_size,
            max_loss=self._config.max_loss_per_lot * self._config.lot_size,
            time_stop_days=self._config.time_stop_loss_days if self._config.time_stop_loss_days > 0 else None,
            exit_at_opposite_sd=self._config.exit_at_opposite_sd if self._config.exit_at_opposite_sd > 0 else None
        )

        # Notify callbacks
        for callback in self._signal_callbacks:
            try:
                callback(signal)
            except Exception:
                pass

        # Act on signal
        if signal.is_exit:
            spot_broker = self._get_broker_by_role("SPOT")
            futures_broker = self._get_broker_by_role("FUTURES")

            if spot_broker and futures_broker and self._orchestrator:
                success, error = await self._orchestrator.execute_exit(
                    trade=trade,
                    exit_zscore=signal.zscore,
                    spot_symbol=spot_broker.symbol,
                    futures_symbol=futures_broker.symbol,
                    order_type=self._config.order_type,
                    limit_timeout=self._config.limit_order_timeout,
                    limit_peg_interval=self._config.limit_peg_interval
                )

                if success:
                    # Clear position state
                    self._position.trade = None
                    self._position.entry_time = None
                    self._position.current_pnl = 0

                    # Notify callbacks
                    for callback in self._trade_callbacks:
                        try:
                            callback("EXIT", trade)
                        except Exception:
                            pass

    def _update_position_pnl(self) -> None:
        """Update current position P&L"""
        trade = self._position.trade
        if not trade:
            return

        # Calculate based on current prices vs entry prices
        if trade.entry_spot_price and trade.entry_futures_price:
            spot_mid = (self._market.spot_bid + self._market.spot_ask) / 2
            futures_mid = (self._market.futures_bid + self._market.futures_ask) / 2

            entry_spread = trade.entry_spot_price - trade.entry_futures_price
            current_spread = spot_mid - futures_mid
            spread_change = current_spread - entry_spread

            # Long Spread profits when spread increases
            # Short Spread profits when spread decreases
            if "Long" in (trade.direction or ""):
                self._position.current_pnl = spread_change * self._config.lot_size * self._config.contract_size
            else:
                self._position.current_pnl = -spread_change * self._config.lot_size * self._config.contract_size

            self._position.spot_price = spot_mid
            self._position.futures_price = futures_mid
            self._position.unrealized_spread = current_spread

    def _should_close_overnight(self) -> bool:
        """Check if overnight protection should trigger"""
        if not self._config.close_before_overnight:
            return False

        if not self._position.trade:
            return False

        now = datetime.now().time()
        close_time = time(
            self._config.overnight_close_hour,
            self._config.overnight_close_minute
        )

        return now >= close_time

    async def _handle_overnight_close(self) -> None:
        """Handle overnight position close"""
        logger.info("Overnight close triggered")

        trade = self._position.trade
        if trade and self._orchestrator:
            spot_broker = self._get_broker_by_role("SPOT")
            futures_broker = self._get_broker_by_role("FUTURES")

            if spot_broker and futures_broker:
                await self._orchestrator.execute_exit(
                    trade=trade,
                    exit_zscore=self._market.zscore or 0,
                    spot_symbol=spot_broker.symbol,
                    futures_symbol=futures_broker.symbol,
                    order_type="MARKET"  # Force market for overnight close
                )

                self._position.trade = None
                self._position.entry_time = None

    def _all_brokers_connected(self) -> bool:
        """Check if all brokers are connected"""
        for broker in self._brokers.values():
            if broker.status != "CONNECTED":
                return False
        return True

    def _log_sd_touch(self, signal: Signal) -> None:
        """Log SD touch to database"""
        sd_level = f"{abs(signal.zscore):.1f}σ"
        direction = "HIGH" if signal.zscore > 0 else "LOW"

        touch = SDTouchLog(
            asset=self._config.asset_name,
            touch_date=datetime.now().strftime("%Y-%m-%d"),
            touch_time=datetime.now().strftime("%H:%M:%S"),
            sd_level=sd_level,
            direction=direction,
            touch_spread=signal.spread,
            touch_zscore=signal.zscore,
            mean_at_touch=signal.mean,
            std_at_touch=signal.std,
            entry_spot_spread=(self._market.spot_ask - self._market.spot_bid),
            entry_futures_spread=(self._market.futures_ask - self._market.futures_bid)
        )
        self._db.add_sd_touch(touch)

    async def _price_sampling_loop(self) -> None:
        """Sample prices periodically for history"""
        while self._state == EngineState.RUNNING:
            try:
                if self._market.is_valid:
                    now = datetime.now()
                    if (now - self._last_price_sample).total_seconds() >= self.PRICE_SAMPLE_INTERVAL:
                        spot_mid = (self._market.spot_bid + self._market.spot_ask) / 2
                        futures_mid = (self._market.futures_bid + self._market.futures_ask) / 2

                        price = PriceHistory(
                            timestamp=now.isoformat(),
                            asset=self._config.selected_asset,
                            spot_price=spot_mid,
                            futures_price=futures_mid,
                            spread=self._market.spread
                        )
                        self._db.add_price_history(price)
                        self._last_price_sample = now

            except Exception as e:
                logger.error(f"Price sampling error: {e}")

            await asyncio.sleep(self.PRICE_SAMPLE_INTERVAL)

    # ==================== Public API ====================

    def reload_config(self) -> None:
        """Reload configuration from database"""
        self._config = self._db.get_config()

        # Update signal generator
        self._signal_gen.lookback_period = self._config.lookback_period
        self._signal_gen.lookback_unit = self._config.lookback_unit
        self._signal_gen.entry_threshold = self._config.entry_std_dev
        self._signal_gen.exit_threshold = self._config.exit_std_dev
        self._signal_gen.stop_loss_threshold = self._config.stop_loss_std_dev
        self._signal_gen.hurst_enabled = self._config.hurst_enabled
        self._signal_gen.hurst_threshold = self._config.hurst_threshold

        logger.info("Configuration reloaded")

    def get_status(self) -> Dict[str, Any]:
        """Get current engine status"""
        return {
            'state': self._state.value,
            'config': self._config.to_dict() if self._config else {},
            'brokers': {
                bid: {
                    'role': b.role,
                    'symbol': b.symbol,
                    'status': b.status,
                    'latency_ms': b.latency_ms
                }
                for bid, b in self._brokers.items()
            },
            'market': {
                'spot_bid': self._market.spot_bid,
                'spot_ask': self._market.spot_ask,
                'futures_bid': self._market.futures_bid,
                'futures_ask': self._market.futures_ask,
                'spread': self._market.spread,
                'zscore': self._market.zscore,
                'mean': self._market.mean,
                'std': self._market.std,
                'hurst': self._market.hurst,
                'regime': self._market.regime
            },
            'position': {
                'has_position': self._position.trade is not None,
                'trade_id': self._position.trade.trade_id if self._position.trade else None,
                'direction': self._position.trade.direction if self._position.trade else None,
                'current_pnl': self._position.current_pnl,
                'entry_time': self._position.entry_time.isoformat() if self._position.entry_time else None
            },
            'statistics': self._signal_gen.get_statistics() if self._signal_gen else {}
        }

    def on_tick(self, callback: Callable) -> None:
        """Register tick callback"""
        self._tick_callbacks.append(callback)

    def on_signal(self, callback: Callable) -> None:
        """Register signal callback"""
        self._signal_callbacks.append(callback)

    def on_trade(self, callback: Callable) -> None:
        """Register trade callback"""
        self._trade_callbacks.append(callback)

    @property
    def state(self) -> EngineState:
        """Get current engine state"""
        return self._state

    @property
    def config(self) -> Optional[TradingConfig]:
        """Get current configuration"""
        return self._config

    @property
    def market(self) -> MarketState:
        """Get current market state"""
        return self._market

    @property
    def position(self) -> PositionState:
        """Get current position state"""
        return self._position
