"""
Order Orchestrator

Handles synchronized order execution across multiple brokers.
Ensures atomic-like behavior for spread trades where both legs
must be executed together.

Key Features:
- Parallel order submission to both brokers
- Synchronization barrier for execution
- Rollback on partial failures
- Position state tracking
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import logging
import uuid

from .ipc import IPCManager, IPCMessage, MessageType
from .signals import Signal, SignalType
from database.manager import DatabaseManager
from database.models import Trade, LimitOrderLog

logger = logging.getLogger(__name__)


class OrderAction(Enum):
    """Order action types"""
    OPEN = "OPEN"
    CLOSE = "CLOSE"


class ExecutionStatus(Enum):
    """Execution status"""
    PENDING = "PENDING"
    EXECUTING = "EXECUTING"
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"


@dataclass
class LegOrder:
    """Single leg of a spread order"""
    broker_id: str
    symbol: str
    side: str  # 'BUY' or 'SELL'
    volume: float
    order_type: str = "MARKET"
    price: Optional[float] = None
    ticket: Optional[int] = None  # For closing existing positions

    # Execution results
    status: ExecutionStatus = ExecutionStatus.PENDING
    fill_price: Optional[float] = None
    fill_ticket: Optional[int] = None
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None


@dataclass
class SpreadOrder:
    """Complete spread order with two legs"""
    order_id: str
    action: OrderAction
    direction: str  # 'LONG' or 'SHORT'

    spot_leg: LegOrder
    futures_leg: LegOrder

    # Entry signal data
    entry_zscore: Optional[float] = None
    entry_spread: Optional[float] = None
    entry_mean: Optional[float] = None
    entry_std: Optional[float] = None

    # Status
    status: ExecutionStatus = ExecutionStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    executed_at: Optional[datetime] = None
    error: Optional[str] = None

    @property
    def is_complete(self) -> bool:
        """Check if both legs are filled"""
        return (
            self.spot_leg.status == ExecutionStatus.FILLED and
            self.futures_leg.status == ExecutionStatus.FILLED
        )

    @property
    def is_failed(self) -> bool:
        """Check if order failed"""
        return self.status in (ExecutionStatus.FAILED, ExecutionStatus.ROLLED_BACK)


class OrderOrchestrator:
    """
    Orchestrates synchronized order execution across brokers.

    Handles the complexity of executing spread trades where:
    1. Two orders must be placed simultaneously
    2. Both must succeed for the trade to be valid
    3. Partial fills must be handled/rolled back
    """

    # Timeout for order execution
    ORDER_TIMEOUT_SECONDS = 60

    # Maximum retry attempts
    MAX_RETRIES = 3

    def __init__(
        self,
        ipc: IPCManager,
        db: DatabaseManager,
        spot_broker_id: str,
        futures_broker_id: str
    ):
        """
        Initialize order orchestrator.

        Args:
            ipc: IPC manager for broker communication
            db: Database manager
            spot_broker_id: ID of spot broker
            futures_broker_id: ID of futures broker
        """
        self.ipc = ipc
        self.db = db
        self.spot_broker_id = spot_broker_id
        self.futures_broker_id = futures_broker_id

        # Active orders
        self._pending_orders: Dict[str, SpreadOrder] = {}

    async def execute_entry(
        self,
        signal: Signal,
        spot_symbol: str,
        futures_symbol: str,
        lot_size: float,
        order_type: str = "MARKET",
        limit_timeout: int = 60,
        limit_peg_interval: float = 1.5
    ) -> Tuple[bool, Optional[Trade], Optional[str]]:
        """
        Execute spread entry based on signal.

        For LONG spread (buy spot, sell futures):
        - Expect spread to increase (spot up, futures down)

        For SHORT spread (sell spot, buy futures):
        - Expect spread to decrease (spot down, futures up)

        Args:
            signal: Entry signal
            spot_symbol: Spot trading symbol
            futures_symbol: Futures trading symbol
            lot_size: Position size
            order_type: 'MARKET', 'LIMIT', or 'PEGGED_LIMIT'
            limit_timeout: Timeout for limit orders
            limit_peg_interval: Interval for pegged limit price updates

        Returns:
            Tuple of (success, Trade object, error message)
        """
        if signal.signal_type == SignalType.ENTRY_LONG:
            direction = "Long Spread"
            spot_side = "BUY"
            futures_side = "SELL"
        elif signal.signal_type == SignalType.ENTRY_SHORT:
            direction = "Short Spread"
            spot_side = "SELL"
            futures_side = "BUY"
        else:
            return False, None, "Invalid entry signal"

        # Create spread order
        order_id = f"ENTRY-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"

        spread_order = SpreadOrder(
            order_id=order_id,
            action=OrderAction.OPEN,
            direction=direction,
            spot_leg=LegOrder(
                broker_id=self.spot_broker_id,
                symbol=spot_symbol,
                side=spot_side,
                volume=lot_size,
                order_type=order_type
            ),
            futures_leg=LegOrder(
                broker_id=self.futures_broker_id,
                symbol=futures_symbol,
                side=futures_side,
                volume=lot_size,
                order_type=order_type
            ),
            entry_zscore=signal.zscore,
            entry_spread=signal.spread,
            entry_mean=signal.mean,
            entry_std=signal.std
        )

        self._pending_orders[order_id] = spread_order

        # Execute both legs in parallel
        success, error = await self._execute_spread_order(spread_order, limit_timeout, limit_peg_interval)

        if success:
            # Create trade record
            trade = Trade(
                trade_id=order_id,
                asset=spot_symbol.replace("USD", ""),
                direction=direction,
                entry_date=datetime.now().isoformat(),
                entry_zscore=signal.zscore,
                entry_spot_price=spread_order.spot_leg.fill_price,
                entry_futures_price=spread_order.futures_leg.fill_price,
                lot_size=lot_size,
                spot_broker_id=self.spot_broker_id,
                mt5_spot_ticket=spread_order.spot_leg.fill_ticket,
                futures_broker_id=self.futures_broker_id,
                mt5_futures_ticket=spread_order.futures_leg.fill_ticket,
                order_status="FILLED",
                status="OPEN"
            )
            self.db.add_trade(trade)

            logger.info(f"Entry executed: {direction} @ Z={signal.zscore:.2f}")
            return True, trade, None
        else:
            logger.error(f"Entry failed: {error}")
            return False, None, error

    async def execute_exit(
        self,
        trade: Trade,
        exit_zscore: float,
        spot_symbol: str,
        futures_symbol: str,
        order_type: str = "MARKET",
        limit_timeout: int = 60,
        limit_peg_interval: float = 1.5
    ) -> Tuple[bool, Optional[str]]:
        """
        Execute spread exit to close position.

        Args:
            trade: Trade to close
            exit_zscore: Current Z-score
            spot_symbol: Spot trading symbol
            futures_symbol: Futures trading symbol
            order_type: Order execution type
            limit_timeout: Timeout for limit orders
            limit_peg_interval: Pegged limit interval

        Returns:
            Tuple of (success, error message)
        """
        # Determine exit sides (opposite of entry)
        if trade.direction == "Long Spread":
            spot_side = "SELL"
            futures_side = "BUY"
        else:  # Short Spread
            spot_side = "BUY"
            futures_side = "SELL"

        order_id = f"EXIT-{trade.trade_id}"

        spread_order = SpreadOrder(
            order_id=order_id,
            action=OrderAction.CLOSE,
            direction=trade.direction,
            spot_leg=LegOrder(
                broker_id=self.spot_broker_id,
                symbol=spot_symbol,
                side=spot_side,
                volume=trade.lot_size,
                order_type=order_type,
                ticket=trade.mt5_spot_ticket
            ),
            futures_leg=LegOrder(
                broker_id=self.futures_broker_id,
                symbol=futures_symbol,
                side=futures_side,
                volume=trade.lot_size,
                order_type=order_type,
                ticket=trade.mt5_futures_ticket
            )
        )

        self._pending_orders[order_id] = spread_order

        # Execute exit
        success, error = await self._execute_spread_order(spread_order, limit_timeout, limit_peg_interval)

        if success:
            # Update trade record
            entry_time = datetime.fromisoformat(trade.entry_date) if trade.entry_date else datetime.now()
            days_held = (datetime.now() - entry_time).total_seconds() / 86400

            # Calculate P&L
            spot_pnl = self._calculate_leg_pnl(
                trade.direction,
                "SPOT",
                trade.entry_spot_price,
                spread_order.spot_leg.fill_price,
                trade.lot_size
            )
            futures_pnl = self._calculate_leg_pnl(
                trade.direction,
                "FUTURES",
                trade.entry_futures_price,
                spread_order.futures_leg.fill_price,
                trade.lot_size
            )

            trade.exit_date = datetime.now().isoformat()
            trade.exit_zscore = exit_zscore
            trade.exit_spot_price = spread_order.spot_leg.fill_price
            trade.exit_futures_price = spread_order.futures_leg.fill_price
            trade.days_held = days_held
            trade.spot_pnl = spot_pnl
            trade.futures_pnl = futures_pnl
            trade.gross_pnl = spot_pnl + futures_pnl
            trade.net_pnl = trade.gross_pnl - (trade.commission or 0) - (trade.swap_cost or 0)
            trade.order_status = "CLOSED"
            trade.status = "CLOSED"

            self.db.update_trade(trade)

            logger.info(f"Exit executed: {trade.direction} @ Z={exit_zscore:.2f}, P&L=${trade.net_pnl:.2f}")
            return True, None
        else:
            logger.error(f"Exit failed: {error}")
            return False, error

    async def _execute_spread_order(
        self,
        order: SpreadOrder,
        limit_timeout: int,
        limit_peg_interval: float
    ) -> Tuple[bool, Optional[str]]:
        """
        Execute spread order with parallel leg submission.

        Args:
            order: Spread order to execute
            limit_timeout: Timeout for limit orders
            limit_peg_interval: Pegged limit interval

        Returns:
            Tuple of (success, error message)
        """
        order.status = ExecutionStatus.EXECUTING
        order.spot_leg.status = ExecutionStatus.EXECUTING
        order.futures_leg.status = ExecutionStatus.EXECUTING

        logger.info(f"Executing spread order: {order.order_id}")

        try:
            # Execute both legs in parallel
            spot_task = asyncio.create_task(
                self._execute_leg(order.spot_leg, order.action, limit_timeout, limit_peg_interval)
            )
            futures_task = asyncio.create_task(
                self._execute_leg(order.futures_leg, order.action, limit_timeout, limit_peg_interval)
            )

            # Wait for both with timeout
            done, pending = await asyncio.wait(
                [spot_task, futures_task],
                timeout=self.ORDER_TIMEOUT_SECONDS,
                return_when=asyncio.ALL_COMPLETED
            )

            # Cancel any pending tasks
            for task in pending:
                task.cancel()

            # Check results
            spot_success = order.spot_leg.status == ExecutionStatus.FILLED
            futures_success = order.futures_leg.status == ExecutionStatus.FILLED

            if spot_success and futures_success:
                order.status = ExecutionStatus.FILLED
                order.executed_at = datetime.now()
                return True, None

            elif spot_success or futures_success:
                # Partial fill - need to rollback
                order.status = ExecutionStatus.PARTIAL
                logger.warning(f"Partial fill detected for {order.order_id}")

                # Attempt rollback
                await self._rollback_partial(order)
                order.status = ExecutionStatus.ROLLED_BACK
                return False, "Partial fill - rolled back"

            else:
                order.status = ExecutionStatus.FAILED
                errors = []
                if order.spot_leg.error:
                    errors.append(f"Spot: {order.spot_leg.error}")
                if order.futures_leg.error:
                    errors.append(f"Futures: {order.futures_leg.error}")
                return False, "; ".join(errors) or "Both legs failed"

        except Exception as e:
            order.status = ExecutionStatus.FAILED
            order.error = str(e)
            logger.error(f"Spread order execution error: {e}")
            return False, str(e)

    async def _execute_leg(
        self,
        leg: LegOrder,
        action: OrderAction,
        limit_timeout: int,
        limit_peg_interval: float
    ) -> None:
        """
        Execute single leg of spread order.

        Args:
            leg: Leg order details
            action: OPEN or CLOSE
            limit_timeout: Timeout for limit orders
            limit_peg_interval: Pegged limit interval
        """
        start_time = datetime.now()

        try:
            action_str = action.value
            if action == OrderAction.CLOSE and leg.ticket:
                action_str = "CLOSE"

            # Send order request via IPC
            response = await self.ipc.send_order_request(
                broker_id=leg.broker_id,
                action=action_str,
                symbol=leg.symbol,
                side=leg.side,
                volume=leg.volume,
                order_type=leg.order_type,
                price=leg.price,
                ticket=leg.ticket,
                timeout=self.ORDER_TIMEOUT_SECONDS
            )

            leg.execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000

            if response is None:
                leg.status = ExecutionStatus.FAILED
                leg.error = "Order timeout - no response"
                return

            payload = response.payload

            if payload.get('success'):
                leg.status = ExecutionStatus.FILLED
                leg.fill_price = payload.get('price')
                leg.fill_ticket = payload.get('ticket')
                logger.info(f"Leg filled: {leg.broker_id} {leg.side} {leg.volume} @ {leg.fill_price}")
            else:
                leg.status = ExecutionStatus.FAILED
                leg.error = payload.get('error', 'Unknown error')
                logger.error(f"Leg failed: {leg.broker_id} - {leg.error}")

            # Log limit order execution
            if leg.order_type in ("LIMIT", "PEGGED_LIMIT"):
                log = LimitOrderLog(
                    timestamp=datetime.now().isoformat(),
                    broker_id=leg.broker_id,
                    symbol=leg.symbol,
                    order_type=leg.order_type,
                    side=leg.side,
                    volume=leg.volume,
                    target_price=leg.price,
                    fill_price=leg.fill_price,
                    status="FILLED" if leg.status == ExecutionStatus.FILLED else "FAILED",
                    elapsed_seconds=leg.execution_time_ms / 1000 if leg.execution_time_ms else None,
                    error_message=leg.error,
                    context=action.value
                )
                self.db.add_limit_order_log(log)

        except Exception as e:
            leg.status = ExecutionStatus.FAILED
            leg.error = str(e)
            logger.error(f"Leg execution error: {e}")

    async def _rollback_partial(self, order: SpreadOrder) -> None:
        """
        Rollback partially filled spread order.

        Closes any filled leg to restore flat position.

        Args:
            order: Partially filled spread order
        """
        logger.warning(f"Rolling back partial fill: {order.order_id}")

        # Determine which leg to rollback
        if order.spot_leg.status == ExecutionStatus.FILLED:
            # Close spot position
            rollback_side = "SELL" if order.spot_leg.side == "BUY" else "BUY"
            await self.ipc.send_order_request(
                broker_id=order.spot_leg.broker_id,
                action="CLOSE",
                symbol=order.spot_leg.symbol,
                side=rollback_side,
                volume=order.spot_leg.volume,
                order_type="MARKET",
                ticket=order.spot_leg.fill_ticket
            )
            logger.info(f"Rolled back spot leg: {order.spot_leg.fill_ticket}")

        if order.futures_leg.status == ExecutionStatus.FILLED:
            # Close futures position
            rollback_side = "SELL" if order.futures_leg.side == "BUY" else "BUY"
            await self.ipc.send_order_request(
                broker_id=order.futures_leg.broker_id,
                action="CLOSE",
                symbol=order.futures_leg.symbol,
                side=rollback_side,
                volume=order.futures_leg.volume,
                order_type="MARKET",
                ticket=order.futures_leg.fill_ticket
            )
            logger.info(f"Rolled back futures leg: {order.futures_leg.fill_ticket}")

    def _calculate_leg_pnl(
        self,
        direction: str,
        leg_type: str,
        entry_price: float,
        exit_price: float,
        lot_size: float,
        contract_size: float = 100.0
    ) -> float:
        """
        Calculate P&L for a single leg.

        Args:
            direction: 'Long Spread' or 'Short Spread'
            leg_type: 'SPOT' or 'FUTURES'
            entry_price: Entry price
            exit_price: Exit price
            lot_size: Position size
            contract_size: Contract size multiplier

        Returns:
            P&L in account currency
        """
        if entry_price is None or exit_price is None:
            return 0.0

        price_change = exit_price - entry_price

        # Long Spread = Long Spot + Short Futures
        # Short Spread = Short Spot + Long Futures
        if direction == "Long Spread":
            if leg_type == "SPOT":
                # Long spot: profit when price goes up
                pnl = price_change * lot_size * contract_size
            else:  # FUTURES
                # Short futures: profit when price goes down
                pnl = -price_change * lot_size * contract_size
        else:  # Short Spread
            if leg_type == "SPOT":
                # Short spot: profit when price goes down
                pnl = -price_change * lot_size * contract_size
            else:  # FUTURES
                # Long futures: profit when price goes up
                pnl = price_change * lot_size * contract_size

        return pnl

    def get_pending_orders(self) -> List[SpreadOrder]:
        """Get list of pending orders"""
        return [o for o in self._pending_orders.values() if o.status == ExecutionStatus.PENDING]

    def get_order(self, order_id: str) -> Optional[SpreadOrder]:
        """Get order by ID"""
        return self._pending_orders.get(order_id)
