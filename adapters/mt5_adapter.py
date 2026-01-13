"""
MetaTrader 5 Broker Adapter

Implements the BrokerAdapter interface for MetaTrader 5 terminals.
Uses the MetaTrader5 Python library for connectivity.

Note: MT5 library requires running in Windows with MT5 terminal installed.
Only ONE mt5.initialize() per Python process is supported.
"""

import asyncio
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

from .base import (
    BrokerAdapter,
    BrokerConfig,
    BrokerStatus,
    Tick,
    OrderResult,
    Position,
    AccountInfo,
    SymbolInfo,
    OrderType,
    OrderSide,
    PositionType,
)

logger = logging.getLogger(__name__)

# MT5 library is only available on Windows
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    mt5 = None
    logger.warning("MetaTrader5 library not available. MT5Adapter will use mock mode.")


class MT5Adapter(BrokerAdapter):
    """
    MetaTrader 5 implementation of BrokerAdapter.

    Handles:
    - Connection to MT5 terminal
    - Real-time price data
    - Market and limit order execution
    - Pegged limit orders with price tracking
    - Position management
    - Account information
    """

    # Magic number range for this system
    MAGIC_BASE = 100000

    def __init__(self, config: BrokerConfig):
        """
        Initialize MT5 adapter.

        Args:
            config: BrokerConfig with MT5 connection details
        """
        super().__init__(config)
        self._initialized = False
        self._mock_mode = not MT5_AVAILABLE
        self._mock_positions: Dict[int, Position] = {}
        self._mock_ticket_counter = 1000000

        if self._mock_mode:
            logger.info(f"MT5Adapter {self.broker_id} running in mock mode")

    async def connect(self) -> bool:
        """
        Establish connection to MT5 terminal.

        Returns:
            True if connection successful
        """
        self._status = BrokerStatus.CONNECTING
        logger.info(f"Connecting to MT5: {self.config.mt5_server}")

        if self._mock_mode:
            # Mock mode for development/testing
            await asyncio.sleep(0.1)  # Simulate connection delay
            self._initialized = True
            self._status = BrokerStatus.CONNECTED
            self._last_heartbeat = datetime.now()
            logger.info(f"MT5 mock connection established: {self.broker_id}")
            return True

        try:
            # Initialize MT5 connection
            init_params = {}

            if self.config.mt5_path:
                init_params['path'] = self.config.mt5_path

            if self.config.mt5_account:
                init_params['login'] = self.config.mt5_account

            if self.config.mt5_password:
                init_params['password'] = self.config.mt5_password

            if self.config.mt5_server:
                init_params['server'] = self.config.mt5_server

            init_params['timeout'] = self.config.timeout_seconds * 1000  # ms

            # Run initialization in thread pool (MT5 is blocking)
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(
                None,
                lambda: mt5.initialize(**init_params)
            )

            if not success:
                error_code = mt5.last_error()
                logger.error(f"MT5 initialization failed: {error_code}")
                self._status = BrokerStatus.ERROR
                return False

            self._initialized = True
            self._status = BrokerStatus.CONNECTED
            self._last_heartbeat = datetime.now()

            # Get terminal info
            terminal_info = mt5.terminal_info()
            if terminal_info:
                logger.info(f"MT5 connected: {terminal_info.company}, Build {terminal_info.build}")

            return True

        except Exception as e:
            logger.error(f"MT5 connection error: {e}")
            self._status = BrokerStatus.ERROR
            return False

    async def disconnect(self) -> None:
        """Close MT5 connection."""
        logger.info(f"Disconnecting from MT5: {self.broker_id}")

        if self._mock_mode:
            self._initialized = False
            self._status = BrokerStatus.DISCONNECTED
            return

        if self._initialized and mt5:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, mt5.shutdown)

        self._initialized = False
        self._status = BrokerStatus.DISCONNECTED

    async def heartbeat(self) -> bool:
        """
        Verify MT5 connection is alive.

        Returns:
            True if connection is healthy
        """
        if self._mock_mode:
            self._last_heartbeat = datetime.now()
            self._latency_ms = 5.0  # Mock latency
            return True

        if not self._initialized:
            return False

        try:
            start = datetime.now()
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(None, mt5.terminal_info)

            if info is not None:
                self._latency_ms = (datetime.now() - start).total_seconds() * 1000
                self._last_heartbeat = datetime.now()
                return True
            return False

        except Exception as e:
            logger.error(f"MT5 heartbeat failed: {e}")
            return False

    # ==================== Market Data ====================

    async def get_tick(self, symbol: str) -> Optional[Tick]:
        """
        Get current bid/ask for symbol.

        Args:
            symbol: MT5 symbol name

        Returns:
            Tick with current prices
        """
        if self._mock_mode:
            # Return mock tick data for development
            import random
            base_price = 2650.0 if 'XAU' in symbol or 'GC' in symbol else 1.0
            spread = 0.30 if 'XAU' in symbol else 0.50
            bid = base_price + random.uniform(-1, 1)
            return Tick(
                symbol=symbol,
                bid=bid,
                ask=bid + spread,
                timestamp=datetime.now(),
                last=bid + spread/2,
                volume=100.0
            )

        if not self._initialized:
            logger.warning("MT5 not initialized, cannot get tick")
            return None

        try:
            loop = asyncio.get_event_loop()
            tick = await loop.run_in_executor(
                None,
                lambda: mt5.symbol_info_tick(symbol)
            )

            if tick is None:
                logger.warning(f"No tick data for {symbol}")
                return None

            return Tick(
                symbol=symbol,
                bid=tick.bid,
                ask=tick.ask,
                timestamp=datetime.fromtimestamp(tick.time),
                last=tick.last,
                volume=tick.volume
            )

        except Exception as e:
            logger.error(f"Error getting tick for {symbol}: {e}")
            return None

    async def get_symbol_info(self, symbol: str) -> Optional[SymbolInfo]:
        """
        Get symbol specification.

        Args:
            symbol: MT5 symbol name

        Returns:
            SymbolInfo with contract details
        """
        if self._mock_mode:
            # Return mock symbol info
            return SymbolInfo(
                symbol=symbol,
                description=f"Mock {symbol}",
                base_currency="XAU" if "XAU" in symbol else "USD",
                quote_currency="USD",
                contract_size=100.0,
                min_volume=0.01,
                max_volume=100.0,
                volume_step=0.01,
                point=0.01,
                digits=2,
                spread=30,
                trade_mode="FULL",
                trade_allowed=True
            )

        if not self._initialized:
            return None

        try:
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(
                None,
                lambda: mt5.symbol_info(symbol)
            )

            if info is None:
                return None

            return SymbolInfo(
                symbol=info.name,
                description=info.description,
                base_currency=info.currency_base,
                quote_currency=info.currency_profit,
                contract_size=info.trade_contract_size,
                min_volume=info.volume_min,
                max_volume=info.volume_max,
                volume_step=info.volume_step,
                point=info.point,
                digits=info.digits,
                spread=info.spread,
                trade_mode=str(info.trade_mode),
                trade_allowed=info.trade_mode != 0
            )

        except Exception as e:
            logger.error(f"Error getting symbol info for {symbol}: {e}")
            return None

    # ==================== Order Execution ====================

    async def place_market_order(
        self,
        symbol: str,
        side: OrderSide,
        volume: float,
        magic: int = 0,
        comment: str = ""
    ) -> OrderResult:
        """
        Execute market order.

        Args:
            symbol: Trading symbol
            side: BUY or SELL
            volume: Lot size
            magic: Magic number
            comment: Order comment

        Returns:
            OrderResult with execution details
        """
        start_time = datetime.now()
        logger.info(f"Placing market order: {side.value} {volume} {symbol}")

        if self._mock_mode:
            # Mock order execution
            await asyncio.sleep(0.05)  # Simulate latency
            tick = await self.get_tick(symbol)
            price = tick.ask if side == OrderSide.BUY else tick.bid

            self._mock_ticket_counter += 1
            ticket = self._mock_ticket_counter

            # Create mock position
            position = Position(
                ticket=ticket,
                symbol=symbol,
                volume=volume,
                entry_price=price,
                current_price=price,
                profit=0.0,
                position_type=PositionType.LONG if side == OrderSide.BUY else PositionType.SHORT,
                open_time=datetime.now(),
                magic=magic,
                comment=comment,
                broker_id=self.broker_id
            )
            self._mock_positions[ticket] = position

            return OrderResult(
                success=True,
                ticket=ticket,
                price=price,
                volume=volume,
                filled_volume=volume,
                execution_time_ms=(datetime.now() - start_time).total_seconds() * 1000
            )

        if not self._initialized:
            return OrderResult(success=False, error="MT5 not initialized")

        try:
            # Get current price
            tick = await self.get_tick(symbol)
            if tick is None:
                return OrderResult(success=False, error=f"Cannot get price for {symbol}")

            price = tick.ask if side == OrderSide.BUY else tick.bid
            order_type = mt5.ORDER_TYPE_BUY if side == OrderSide.BUY else mt5.ORDER_TYPE_SELL

            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": volume,
                "type": order_type,
                "price": price,
                "deviation": 20,  # Max slippage in points
                "magic": magic or self.MAGIC_BASE,
                "comment": comment,
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }

            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: mt5.order_send(request)
            )

            execution_time = (datetime.now() - start_time).total_seconds() * 1000

            if result.retcode != mt5.TRADE_RETCODE_DONE:
                return OrderResult(
                    success=False,
                    error=f"Order failed: {result.comment}",
                    error_code=result.retcode,
                    execution_time_ms=execution_time
                )

            slippage = abs(result.price - price) if result.price else None

            return OrderResult(
                success=True,
                ticket=result.order,
                price=result.price,
                volume=result.volume,
                filled_volume=result.volume,
                execution_time_ms=execution_time,
                slippage=slippage
            )

        except Exception as e:
            logger.error(f"Market order error: {e}")
            return OrderResult(success=False, error=str(e))

    async def place_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        volume: float,
        price: float,
        magic: int = 0,
        comment: str = ""
    ) -> OrderResult:
        """
        Place limit order at specified price.

        Args:
            symbol: Trading symbol
            side: BUY or SELL
            volume: Lot size
            price: Limit price
            magic: Magic number
            comment: Order comment

        Returns:
            OrderResult with order details
        """
        logger.info(f"Placing limit order: {side.value} {volume} {symbol} @ {price}")

        if self._mock_mode:
            await asyncio.sleep(0.05)
            self._mock_ticket_counter += 1
            return OrderResult(
                success=True,
                ticket=self._mock_ticket_counter,
                order_id=str(self._mock_ticket_counter),
                price=price,
                volume=volume
            )

        if not self._initialized:
            return OrderResult(success=False, error="MT5 not initialized")

        try:
            order_type = mt5.ORDER_TYPE_BUY_LIMIT if side == OrderSide.BUY else mt5.ORDER_TYPE_SELL_LIMIT

            request = {
                "action": mt5.TRADE_ACTION_PENDING,
                "symbol": symbol,
                "volume": volume,
                "type": order_type,
                "price": price,
                "magic": magic or self.MAGIC_BASE,
                "comment": comment,
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_RETURN,
            }

            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: mt5.order_send(request)
            )

            if result.retcode != mt5.TRADE_RETCODE_DONE:
                return OrderResult(
                    success=False,
                    error=f"Limit order failed: {result.comment}",
                    error_code=result.retcode
                )

            return OrderResult(
                success=True,
                ticket=result.order,
                order_id=str(result.order),
                price=price,
                volume=volume
            )

        except Exception as e:
            logger.error(f"Limit order error: {e}")
            return OrderResult(success=False, error=str(e))

    async def execute_pegged_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        volume: float,
        timeout_seconds: int,
        peg_interval_seconds: float,
        ticket: Optional[int] = None
    ) -> OrderResult:
        """
        Execute pegged limit order that tracks the market.

        Places limit order at current bid/ask and updates periodically.
        Falls back to market order if timeout reached.

        Args:
            symbol: Trading symbol
            side: BUY or SELL
            volume: Lot size
            timeout_seconds: Max time to wait for fill
            peg_interval_seconds: How often to update price
            ticket: Position ticket if closing

        Returns:
            OrderResult with execution details
        """
        start_time = datetime.now()
        iterations = 0
        last_order_ticket = None

        logger.info(f"Starting pegged limit order: {side.value} {volume} {symbol}")

        while True:
            iterations += 1
            elapsed = (datetime.now() - start_time).total_seconds()

            if elapsed >= timeout_seconds:
                # Timeout - cancel any pending order and fallback to market
                if last_order_ticket:
                    await self.cancel_order(str(last_order_ticket))

                logger.info(f"Pegged limit timeout after {elapsed:.1f}s, falling back to market")

                if ticket:
                    # Closing existing position
                    return await self.close_position(ticket, volume)
                else:
                    # Opening new position
                    return await self.place_market_order(symbol, side, volume)

            # Get current price
            tick = await self.get_tick(symbol)
            if tick is None:
                await asyncio.sleep(0.5)
                continue

            # Determine limit price (buy at bid, sell at ask for better price)
            price = tick.bid if side == OrderSide.BUY else tick.ask

            if last_order_ticket:
                # Modify existing order to new price
                modified = await self.modify_order(str(last_order_ticket), price=price)
                if not modified:
                    # Order might be filled, check positions
                    if ticket:
                        # Was closing - check if position still exists
                        pos = await self.get_position_by_ticket(ticket)
                        if pos is None:
                            # Position closed
                            return OrderResult(
                                success=True,
                                ticket=ticket,
                                price=price,
                                volume=volume,
                                execution_time_ms=(datetime.now() - start_time).total_seconds() * 1000
                            )
                    else:
                        # Was opening - check for new position
                        positions = await self.get_positions(symbol)
                        for pos in positions:
                            if pos.open_time >= start_time:
                                return OrderResult(
                                    success=True,
                                    ticket=pos.ticket,
                                    price=pos.entry_price,
                                    volume=pos.volume,
                                    execution_time_ms=(datetime.now() - start_time).total_seconds() * 1000
                                )
            else:
                # Place initial limit order
                result = await self.place_limit_order(symbol, side, volume, price)
                if result.success:
                    last_order_ticket = result.ticket
                else:
                    logger.warning(f"Failed to place limit order: {result.error}")

            # Wait before next iteration
            await asyncio.sleep(peg_interval_seconds)

    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a pending order.

        Args:
            order_id: Order ticket as string

        Returns:
            True if cancelled successfully
        """
        if self._mock_mode:
            return True

        if not self._initialized:
            return False

        try:
            ticket = int(order_id)
            request = {
                "action": mt5.TRADE_ACTION_REMOVE,
                "order": ticket,
            }

            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: mt5.order_send(request)
            )

            return result.retcode == mt5.TRADE_RETCODE_DONE

        except Exception as e:
            logger.error(f"Cancel order error: {e}")
            return False

    async def modify_order(
        self,
        order_id: str,
        price: Optional[float] = None,
        volume: Optional[float] = None
    ) -> bool:
        """
        Modify a pending order's price or volume.

        Args:
            order_id: Order ticket as string
            price: New limit price
            volume: New volume

        Returns:
            True if modified successfully
        """
        if self._mock_mode:
            return True

        if not self._initialized:
            return False

        try:
            ticket = int(order_id)

            # Get current order info
            loop = asyncio.get_event_loop()
            orders = await loop.run_in_executor(
                None,
                lambda: mt5.orders_get(ticket=ticket)
            )

            if not orders:
                return False

            order = orders[0]

            request = {
                "action": mt5.TRADE_ACTION_MODIFY,
                "order": ticket,
                "price": price if price else order.price_open,
                "volume": volume if volume else order.volume_initial,
            }

            result = await loop.run_in_executor(
                None,
                lambda: mt5.order_send(request)
            )

            return result.retcode == mt5.TRADE_RETCODE_DONE

        except Exception as e:
            logger.error(f"Modify order error: {e}")
            return False

    # ==================== Position Management ====================

    async def close_position(
        self,
        ticket: int,
        volume: Optional[float] = None
    ) -> OrderResult:
        """
        Close an open position.

        Args:
            ticket: Position ticket
            volume: Volume to close (None = close all)

        Returns:
            OrderResult with close details
        """
        logger.info(f"Closing position: ticket={ticket}, volume={volume}")
        start_time = datetime.now()

        if self._mock_mode:
            if ticket in self._mock_positions:
                position = self._mock_positions.pop(ticket)
                return OrderResult(
                    success=True,
                    ticket=ticket,
                    price=position.current_price,
                    volume=volume or position.volume,
                    filled_volume=volume or position.volume,
                    execution_time_ms=(datetime.now() - start_time).total_seconds() * 1000
                )
            return OrderResult(success=False, error="Position not found")

        if not self._initialized:
            return OrderResult(success=False, error="MT5 not initialized")

        try:
            # Get position details
            loop = asyncio.get_event_loop()
            positions = await loop.run_in_executor(
                None,
                lambda: mt5.positions_get(ticket=ticket)
            )

            if not positions:
                return OrderResult(success=False, error="Position not found")

            position = positions[0]
            close_volume = volume if volume else position.volume

            # Determine close direction (opposite of position)
            if position.type == mt5.POSITION_TYPE_BUY:
                order_type = mt5.ORDER_TYPE_SELL
                price = await self.get_tick(position.symbol)
                close_price = price.bid if price else 0
            else:
                order_type = mt5.ORDER_TYPE_BUY
                price = await self.get_tick(position.symbol)
                close_price = price.ask if price else 0

            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": position.symbol,
                "volume": close_volume,
                "type": order_type,
                "position": ticket,
                "price": close_price,
                "deviation": 20,
                "magic": position.magic,
                "comment": "Close position",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }

            result = await loop.run_in_executor(
                None,
                lambda: mt5.order_send(request)
            )

            execution_time = (datetime.now() - start_time).total_seconds() * 1000

            if result.retcode != mt5.TRADE_RETCODE_DONE:
                return OrderResult(
                    success=False,
                    error=f"Close failed: {result.comment}",
                    error_code=result.retcode,
                    execution_time_ms=execution_time
                )

            return OrderResult(
                success=True,
                ticket=ticket,
                price=result.price,
                volume=result.volume,
                filled_volume=result.volume,
                execution_time_ms=execution_time
            )

        except Exception as e:
            logger.error(f"Close position error: {e}")
            return OrderResult(success=False, error=str(e))

    async def get_positions(self, symbol: Optional[str] = None) -> List[Position]:
        """
        Get all open positions.

        Args:
            symbol: Filter by symbol (optional)

        Returns:
            List of Position objects
        """
        if self._mock_mode:
            positions = list(self._mock_positions.values())
            if symbol:
                positions = [p for p in positions if p.symbol == symbol]
            return positions

        if not self._initialized:
            return []

        try:
            loop = asyncio.get_event_loop()

            if symbol:
                mt5_positions = await loop.run_in_executor(
                    None,
                    lambda: mt5.positions_get(symbol=symbol)
                )
            else:
                mt5_positions = await loop.run_in_executor(
                    None,
                    mt5.positions_get
                )

            if mt5_positions is None:
                return []

            positions = []
            for pos in mt5_positions:
                position_type = PositionType.LONG if pos.type == mt5.POSITION_TYPE_BUY else PositionType.SHORT

                positions.append(Position(
                    ticket=pos.ticket,
                    symbol=pos.symbol,
                    volume=pos.volume,
                    entry_price=pos.price_open,
                    current_price=pos.price_current,
                    profit=pos.profit,
                    position_type=position_type,
                    open_time=datetime.fromtimestamp(pos.time),
                    swap=pos.swap,
                    commission=pos.commission,
                    magic=pos.magic,
                    comment=pos.comment,
                    broker_id=self.broker_id
                ))

            return positions

        except Exception as e:
            logger.error(f"Get positions error: {e}")
            return []

    async def get_position_by_ticket(self, ticket: int) -> Optional[Position]:
        """
        Get specific position by ticket.

        Args:
            ticket: Position ticket number

        Returns:
            Position or None
        """
        positions = await self.get_positions()
        for pos in positions:
            if pos.ticket == ticket:
                return pos
        return None

    # ==================== Account Information ====================

    async def get_account_info(self) -> Optional[AccountInfo]:
        """
        Get account balance and margin info.

        Returns:
            AccountInfo object
        """
        if self._mock_mode:
            return AccountInfo(
                balance=100000.0,
                equity=100000.0,
                margin=0.0,
                free_margin=100000.0,
                margin_level=0.0,
                profit=0.0,
                currency="USD",
                leverage=100,
                name="Mock Account",
                server=self.config.mt5_server or "MockServer"
            )

        if not self._initialized:
            return None

        try:
            loop = asyncio.get_event_loop()
            account = await loop.run_in_executor(
                None,
                mt5.account_info
            )

            if account is None:
                return None

            return AccountInfo(
                balance=account.balance,
                equity=account.equity,
                margin=account.margin,
                free_margin=account.margin_free,
                margin_level=account.margin_level,
                profit=account.profit,
                currency=account.currency,
                leverage=account.leverage,
                name=account.name,
                server=account.server,
                company=account.company
            )

        except Exception as e:
            logger.error(f"Get account info error: {e}")
            return None

    # ==================== Symbol Search ====================

    async def search_symbols(self, pattern: str) -> List[str]:
        """
        Search for symbols matching pattern.

        Args:
            pattern: Search pattern (e.g., 'XAU', 'GC')

        Returns:
            List of matching symbol names
        """
        if self._mock_mode:
            mock_symbols = ['XAUUSD', 'XAUEUR', 'GC0226', 'GC0326', 'EURUSD', 'GBPUSD']
            return [s for s in mock_symbols if pattern.upper() in s.upper()]

        if not self._initialized:
            return []

        try:
            loop = asyncio.get_event_loop()
            symbols = await loop.run_in_executor(
                None,
                lambda: mt5.symbols_get(pattern)
            )

            if symbols is None:
                return []

            return [s.name for s in symbols]

        except Exception as e:
            logger.error(f"Symbol search error: {e}")
            return []
