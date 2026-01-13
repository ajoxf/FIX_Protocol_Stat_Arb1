"""
FIX Protocol Broker Adapter

Implements the BrokerAdapter interface for FIX Protocol connectivity.
Used for ECN, Prime Brokers, and institutional trading platforms.

FIX Message Types Used:
- Logon (A/35=A)
- Logout (5/35=5)
- Heartbeat (0/35=0)
- Market Data Request (V/35=V)
- Market Data Snapshot (W/35=W)
- New Order Single (D/35=D)
- Order Cancel Request (F/35=F)
- Order Cancel/Replace (G/35=G)
- Order Status Request (H/35=H)
- Execution Report (8/35=8)
"""

import asyncio
import socket
import ssl
from datetime import datetime
from typing import List, Optional, Dict, Any, Callable
from dataclasses import dataclass
import logging
import uuid

from .base import (
    BrokerAdapter,
    BrokerConfig,
    BrokerStatus,
    Tick,
    OrderResult,
    Position,
    AccountInfo,
    SymbolInfo,
    OrderSide,
    PositionType,
)

logger = logging.getLogger(__name__)

# Try to import simplefix
try:
    import simplefix
    SIMPLEFIX_AVAILABLE = True
except ImportError:
    SIMPLEFIX_AVAILABLE = False
    simplefix = None
    logger.warning("simplefix library not available. Install with: pip install simplefix")


# FIX field tags
class FIXTag:
    """Common FIX protocol field tags"""
    # Header
    BEGIN_STRING = 8
    BODY_LENGTH = 9
    MSG_TYPE = 35
    SENDER_COMP_ID = 49
    TARGET_COMP_ID = 56
    MSG_SEQ_NUM = 34
    SENDING_TIME = 52
    CHECKSUM = 10

    # Session
    ENCRYPT_METHOD = 98
    HEARTBT_INT = 108
    USERNAME = 553
    PASSWORD = 554
    TEST_REQ_ID = 112
    REF_SEQ_NUM = 45

    # Market Data
    MD_REQ_ID = 262
    SUBSCRIPTION_REQ_TYPE = 263
    MARKET_DEPTH = 264
    MD_UPDATE_TYPE = 265
    NO_MD_ENTRY_TYPES = 267
    MD_ENTRY_TYPE = 269
    NO_MD_ENTRIES = 268
    MD_ENTRY_PX = 270
    MD_ENTRY_SIZE = 271
    NO_RELATED_SYM = 146
    SYMBOL = 55

    # Order
    CL_ORD_ID = 11
    ORIG_CL_ORD_ID = 41
    ORDER_ID = 37
    EXEC_ID = 17
    EXEC_TYPE = 150
    ORD_STATUS = 39
    SIDE = 54
    ORDER_QTY = 38
    ORD_TYPE = 40
    PRICE = 44
    TIME_IN_FORCE = 59
    TRANSACT_TIME = 60
    AVG_PX = 6
    LEAVES_QTY = 151
    CUM_QTY = 14
    TEXT = 58
    LAST_PX = 31
    LAST_QTY = 32


class FIXMsgType:
    """FIX message types"""
    HEARTBEAT = '0'
    TEST_REQUEST = '1'
    RESEND_REQUEST = '2'
    REJECT = '3'
    SEQUENCE_RESET = '4'
    LOGOUT = '5'
    LOGON = 'A'
    NEW_ORDER_SINGLE = 'D'
    ORDER_CANCEL_REQUEST = 'F'
    ORDER_CANCEL_REPLACE = 'G'
    ORDER_STATUS_REQUEST = 'H'
    EXECUTION_REPORT = '8'
    ORDER_CANCEL_REJECT = '9'
    MARKET_DATA_REQUEST = 'V'
    MARKET_DATA_SNAPSHOT = 'W'
    MARKET_DATA_INCREMENTAL = 'X'
    MARKET_DATA_REQUEST_REJECT = 'Y'


@dataclass
class FIXOrder:
    """Internal FIX order tracking"""
    cl_ord_id: str
    symbol: str
    side: OrderSide
    quantity: float
    order_type: str
    price: Optional[float]
    status: str
    filled_qty: float = 0.0
    avg_price: float = 0.0
    order_id: Optional[str] = None
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class FIXAdapter(BrokerAdapter):
    """
    FIX Protocol implementation of BrokerAdapter.

    Supports:
    - FIX 4.2, 4.4 protocol versions
    - SSL/TLS encryption
    - Market data subscription
    - Order execution and management
    - Position tracking via execution reports
    """

    FIX_VERSION = "FIX.4.4"

    def __init__(self, config: BrokerConfig):
        """
        Initialize FIX adapter.

        Args:
            config: BrokerConfig with FIX connection details
        """
        super().__init__(config)

        # FIX session state
        self._socket: Optional[socket.socket] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._msg_seq_num = 1
        self._recv_seq_num = 1
        self._heartbeat_interval = config.fix_heartbeat_interval or 30
        self._logged_in = False

        # Message handling
        self._parser = simplefix.FixParser() if SIMPLEFIX_AVAILABLE else None
        self._pending_orders: Dict[str, FIXOrder] = {}
        self._positions: Dict[str, Position] = {}
        self._ticks: Dict[str, Tick] = {}
        self._tick_callbacks: Dict[str, List[Callable]] = {}

        # Background tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._receiver_task: Optional[asyncio.Task] = None

        # Mock mode for development
        self._mock_mode = not SIMPLEFIX_AVAILABLE
        self._mock_positions: Dict[int, Position] = {}
        self._mock_ticket_counter = 2000000

    def _generate_cl_ord_id(self) -> str:
        """Generate unique client order ID"""
        return f"{self.broker_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"

    def _build_header(self, msg_type: str) -> simplefix.FixMessage:
        """Build FIX message header"""
        msg = simplefix.FixMessage()
        msg.append_pair(FIXTag.BEGIN_STRING, self.FIX_VERSION)
        msg.append_pair(FIXTag.MSG_TYPE, msg_type)
        msg.append_pair(FIXTag.SENDER_COMP_ID, self.config.fix_sender_comp)
        msg.append_pair(FIXTag.TARGET_COMP_ID, self.config.fix_target_comp)
        msg.append_pair(FIXTag.MSG_SEQ_NUM, self._msg_seq_num)
        msg.append_pair(FIXTag.SENDING_TIME, datetime.utcnow().strftime('%Y%m%d-%H:%M:%S.%f')[:-3])
        self._msg_seq_num += 1
        return msg

    async def _send_message(self, msg: simplefix.FixMessage) -> bool:
        """Send FIX message over socket"""
        if self._mock_mode:
            logger.debug(f"Mock FIX send: {msg}")
            return True

        if self._writer is None:
            logger.error("FIX socket not connected")
            return False

        try:
            raw = msg.encode()
            self._writer.write(raw)
            await self._writer.drain()
            logger.debug(f"FIX sent: {raw[:100]}...")
            return True
        except Exception as e:
            logger.error(f"FIX send error: {e}")
            return False

    async def _receive_message(self) -> Optional[simplefix.FixMessage]:
        """Receive and parse FIX message"""
        if self._mock_mode or self._reader is None:
            return None

        try:
            data = await asyncio.wait_for(
                self._reader.read(4096),
                timeout=self._heartbeat_interval + 5
            )

            if not data:
                logger.warning("FIX connection closed by server")
                return None

            self._parser.append_buffer(data)
            msg = self._parser.get_message()
            if msg:
                self._recv_seq_num = int(msg.get(FIXTag.MSG_SEQ_NUM) or self._recv_seq_num)
            return msg

        except asyncio.TimeoutError:
            logger.warning("FIX receive timeout")
            return None
        except Exception as e:
            logger.error(f"FIX receive error: {e}")
            return None

    async def connect(self) -> bool:
        """
        Establish FIX connection and logon.

        Returns:
            True if connection and logon successful
        """
        self._status = BrokerStatus.CONNECTING
        logger.info(f"Connecting to FIX: {self.config.fix_host}:{self.config.fix_port}")

        if self._mock_mode:
            await asyncio.sleep(0.1)
            self._logged_in = True
            self._status = BrokerStatus.CONNECTED
            self._last_heartbeat = datetime.now()
            logger.info(f"FIX mock connection established: {self.broker_id}")
            return True

        try:
            # Establish TCP/SSL connection
            if self.config.extra.get('use_ssl', True):
                ssl_context = ssl.create_default_context()
                self._reader, self._writer = await asyncio.open_connection(
                    self.config.fix_host,
                    self.config.fix_port,
                    ssl=ssl_context
                )
            else:
                self._reader, self._writer = await asyncio.open_connection(
                    self.config.fix_host,
                    self.config.fix_port
                )

            # Send Logon message
            logon = self._build_header(FIXMsgType.LOGON)
            logon.append_pair(FIXTag.ENCRYPT_METHOD, 0)  # No encryption
            logon.append_pair(FIXTag.HEARTBT_INT, self._heartbeat_interval)

            if self.config.fix_username:
                logon.append_pair(FIXTag.USERNAME, self.config.fix_username)
            if self.config.fix_password:
                logon.append_pair(FIXTag.PASSWORD, self.config.fix_password)

            await self._send_message(logon)

            # Wait for Logon response
            response = await self._receive_message()
            if response is None:
                raise Exception("No logon response received")

            msg_type = response.get(FIXTag.MSG_TYPE)
            if msg_type != FIXMsgType.LOGON.encode():
                error = response.get(FIXTag.TEXT, b'Unknown error').decode()
                raise Exception(f"Logon rejected: {error}")

            self._logged_in = True
            self._status = BrokerStatus.CONNECTED
            self._last_heartbeat = datetime.now()

            # Start background tasks
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._receiver_task = asyncio.create_task(self._receiver_loop())

            logger.info(f"FIX connection established: {self.broker_id}")
            return True

        except Exception as e:
            logger.error(f"FIX connection error: {e}")
            self._status = BrokerStatus.ERROR
            return False

    async def disconnect(self) -> None:
        """Send Logout and close FIX connection"""
        logger.info(f"Disconnecting FIX: {self.broker_id}")

        # Cancel background tasks
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._receiver_task:
            self._receiver_task.cancel()

        if self._mock_mode:
            self._logged_in = False
            self._status = BrokerStatus.DISCONNECTED
            return

        if self._logged_in and self._writer:
            try:
                logout = self._build_header(FIXMsgType.LOGOUT)
                await self._send_message(logout)
                await asyncio.sleep(1)  # Wait for logout response
            except Exception as e:
                logger.error(f"Logout error: {e}")

        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()

        self._logged_in = False
        self._status = BrokerStatus.DISCONNECTED

    async def _heartbeat_loop(self):
        """Background heartbeat sender"""
        while self._logged_in:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                heartbeat = self._build_header(FIXMsgType.HEARTBEAT)
                await self._send_message(heartbeat)
                self._last_heartbeat = datetime.now()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def _receiver_loop(self):
        """Background message receiver"""
        while self._logged_in:
            try:
                msg = await self._receive_message()
                if msg:
                    await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Receiver error: {e}")

    async def _handle_message(self, msg: simplefix.FixMessage):
        """Process incoming FIX message"""
        msg_type = msg.get(FIXTag.MSG_TYPE).decode()

        if msg_type == FIXMsgType.HEARTBEAT:
            pass  # Just acknowledge

        elif msg_type == FIXMsgType.TEST_REQUEST:
            # Respond to test request
            test_req_id = msg.get(FIXTag.TEST_REQ_ID, b'').decode()
            heartbeat = self._build_header(FIXMsgType.HEARTBEAT)
            heartbeat.append_pair(FIXTag.TEST_REQ_ID, test_req_id)
            await self._send_message(heartbeat)

        elif msg_type == FIXMsgType.EXECUTION_REPORT:
            await self._handle_execution_report(msg)

        elif msg_type == FIXMsgType.MARKET_DATA_SNAPSHOT:
            await self._handle_market_data(msg)

        elif msg_type == FIXMsgType.MARKET_DATA_INCREMENTAL:
            await self._handle_market_data(msg)

        elif msg_type == FIXMsgType.LOGOUT:
            logger.warning("Received logout from server")
            self._logged_in = False
            self._status = BrokerStatus.DISCONNECTED

    async def _handle_execution_report(self, msg: simplefix.FixMessage):
        """Process execution report (order fills, status updates)"""
        cl_ord_id = msg.get(FIXTag.CL_ORD_ID, b'').decode()
        exec_type = msg.get(FIXTag.EXEC_TYPE, b'').decode()
        ord_status = msg.get(FIXTag.ORD_STATUS, b'').decode()

        logger.info(f"Execution report: {cl_ord_id} exec_type={exec_type} status={ord_status}")

        if cl_ord_id in self._pending_orders:
            order = self._pending_orders[cl_ord_id]
            order.status = ord_status
            order.order_id = msg.get(FIXTag.ORDER_ID, b'').decode()

            # Update fill information
            cum_qty = float(msg.get(FIXTag.CUM_QTY, 0))
            avg_px = float(msg.get(FIXTag.AVG_PX, 0))
            order.filled_qty = cum_qty
            order.avg_price = avg_px

            # If filled, create/update position
            if ord_status == '2':  # Filled
                await self._update_position_from_fill(order)

    async def _handle_market_data(self, msg: simplefix.FixMessage):
        """Process market data snapshot/update"""
        symbol = msg.get(FIXTag.SYMBOL, b'').decode()

        # Parse MD entries
        bid = ask = None
        num_entries = int(msg.get(FIXTag.NO_MD_ENTRIES, 0))

        for i in range(num_entries):
            entry_type = msg.get(FIXTag.MD_ENTRY_TYPE)
            entry_px = float(msg.get(FIXTag.MD_ENTRY_PX, 0))

            if entry_type == b'0':  # Bid
                bid = entry_px
            elif entry_type == b'1':  # Offer
                ask = entry_px

        if bid and ask:
            tick = Tick(
                symbol=symbol,
                bid=bid,
                ask=ask,
                timestamp=datetime.now()
            )
            self._ticks[symbol] = tick

            # Notify callbacks
            if symbol in self._tick_callbacks:
                for callback in self._tick_callbacks[symbol]:
                    try:
                        callback(tick)
                    except Exception as e:
                        logger.error(f"Tick callback error: {e}")

    async def _update_position_from_fill(self, order: FIXOrder):
        """Update position tracking from fill"""
        key = f"{order.symbol}_{order.side.value}"

        if key in self._positions:
            # Update existing position
            pos = self._positions[key]
            new_vol = pos.volume + order.filled_qty if order.side == OrderSide.BUY else pos.volume - order.filled_qty
            pos.volume = new_vol
            pos.current_price = order.avg_price
        else:
            # Create new position
            self._positions[key] = Position(
                ticket=hash(order.cl_ord_id) % 10000000,
                symbol=order.symbol,
                volume=order.filled_qty,
                entry_price=order.avg_price,
                current_price=order.avg_price,
                profit=0.0,
                position_type=PositionType.LONG if order.side == OrderSide.BUY else PositionType.SHORT,
                open_time=datetime.now(),
                broker_id=self.broker_id
            )

    async def heartbeat(self) -> bool:
        """Verify FIX connection"""
        if self._mock_mode:
            self._last_heartbeat = datetime.now()
            self._latency_ms = 10.0
            return True

        return self._logged_in

    # ==================== Market Data ====================

    async def get_tick(self, symbol: str) -> Optional[Tick]:
        """Get current prices for symbol"""
        if self._mock_mode:
            import random
            base_price = 2650.0 if 'XAU' in symbol or 'GC' in symbol else 1.0
            spread = 0.20 if 'XAU' in symbol else 0.40
            bid = base_price + random.uniform(-1, 1)
            return Tick(
                symbol=symbol,
                bid=bid,
                ask=bid + spread,
                timestamp=datetime.now()
            )

        # Return cached tick if available
        if symbol in self._ticks:
            return self._ticks[symbol]

        # Request market data if not subscribed
        await self.subscribe_tick(symbol, lambda t: None)
        await asyncio.sleep(0.5)  # Wait for data

        return self._ticks.get(symbol)

    async def get_symbol_info(self, symbol: str) -> Optional[SymbolInfo]:
        """Get symbol contract info (FIX typically doesn't provide this)"""
        # FIX doesn't have a standard way to get symbol info
        # Return defaults based on symbol pattern
        return SymbolInfo(
            symbol=symbol,
            description=f"FIX {symbol}",
            base_currency="XAU" if "XAU" in symbol or "GC" in symbol else "USD",
            quote_currency="USD",
            contract_size=self.config.contract_size,
            min_volume=self.config.min_volume,
            max_volume=self.config.max_volume,
            volume_step=0.01,
            point=0.01,
            digits=2,
            spread=20,
            trade_mode="FULL",
            trade_allowed=True
        )

    async def subscribe_tick(self, symbol: str, callback: Callable) -> bool:
        """Subscribe to market data"""
        if symbol not in self._tick_callbacks:
            self._tick_callbacks[symbol] = []
        self._tick_callbacks[symbol].append(callback)

        if self._mock_mode:
            return True

        # Send market data request
        request = self._build_header(FIXMsgType.MARKET_DATA_REQUEST)
        request.append_pair(FIXTag.MD_REQ_ID, f"MD_{symbol}_{datetime.now().timestamp()}")
        request.append_pair(FIXTag.SUBSCRIPTION_REQ_TYPE, 1)  # Snapshot + Updates
        request.append_pair(FIXTag.MARKET_DEPTH, 1)  # Top of book
        request.append_pair(FIXTag.NO_MD_ENTRY_TYPES, 2)
        request.append_pair(FIXTag.MD_ENTRY_TYPE, 0)  # Bid
        request.append_pair(FIXTag.MD_ENTRY_TYPE, 1)  # Offer
        request.append_pair(FIXTag.NO_RELATED_SYM, 1)
        request.append_pair(FIXTag.SYMBOL, symbol)

        return await self._send_message(request)

    async def unsubscribe_tick(self, symbol: str) -> bool:
        """Unsubscribe from market data"""
        if symbol in self._tick_callbacks:
            del self._tick_callbacks[symbol]

        if self._mock_mode:
            return True

        # Send unsubscribe request
        request = self._build_header(FIXMsgType.MARKET_DATA_REQUEST)
        request.append_pair(FIXTag.MD_REQ_ID, f"MD_{symbol}_unsub")
        request.append_pair(FIXTag.SUBSCRIPTION_REQ_TYPE, 2)  # Disable
        request.append_pair(FIXTag.NO_RELATED_SYM, 1)
        request.append_pair(FIXTag.SYMBOL, symbol)

        return await self._send_message(request)

    # ==================== Order Execution ====================

    async def place_market_order(
        self,
        symbol: str,
        side: OrderSide,
        volume: float,
        magic: int = 0,
        comment: str = ""
    ) -> OrderResult:
        """Place market order via FIX"""
        start_time = datetime.now()
        cl_ord_id = self._generate_cl_ord_id()

        logger.info(f"FIX market order: {side.value} {volume} {symbol}")

        if self._mock_mode:
            await asyncio.sleep(0.03)
            tick = await self.get_tick(symbol)
            price = tick.ask if side == OrderSide.BUY else tick.bid

            self._mock_ticket_counter += 1
            ticket = self._mock_ticket_counter

            position = Position(
                ticket=ticket,
                symbol=symbol,
                volume=volume,
                entry_price=price,
                current_price=price,
                profit=0.0,
                position_type=PositionType.LONG if side == OrderSide.BUY else PositionType.SHORT,
                open_time=datetime.now(),
                broker_id=self.broker_id
            )
            self._mock_positions[ticket] = position

            return OrderResult(
                success=True,
                ticket=ticket,
                order_id=cl_ord_id,
                price=price,
                volume=volume,
                filled_volume=volume,
                execution_time_ms=(datetime.now() - start_time).total_seconds() * 1000
            )

        # Track pending order
        order = FIXOrder(
            cl_ord_id=cl_ord_id,
            symbol=symbol,
            side=side,
            quantity=volume,
            order_type='1',  # Market
            price=None,
            status='0'  # New
        )
        self._pending_orders[cl_ord_id] = order

        # Build New Order Single message
        msg = self._build_header(FIXMsgType.NEW_ORDER_SINGLE)
        msg.append_pair(FIXTag.CL_ORD_ID, cl_ord_id)
        msg.append_pair(FIXTag.SYMBOL, symbol)
        msg.append_pair(FIXTag.SIDE, '1' if side == OrderSide.BUY else '2')
        msg.append_pair(FIXTag.ORDER_QTY, volume)
        msg.append_pair(FIXTag.ORD_TYPE, '1')  # Market
        msg.append_pair(FIXTag.TIME_IN_FORCE, '0')  # Day
        msg.append_pair(FIXTag.TRANSACT_TIME, datetime.utcnow().strftime('%Y%m%d-%H:%M:%S'))

        if not await self._send_message(msg):
            return OrderResult(success=False, error="Failed to send order")

        # Wait for execution report
        for _ in range(50):  # 5 second timeout
            await asyncio.sleep(0.1)
            if order.status in ['2', '8']:  # Filled or Rejected
                break

        if order.status == '2':  # Filled
            return OrderResult(
                success=True,
                ticket=hash(cl_ord_id) % 10000000,
                order_id=order.order_id,
                price=order.avg_price,
                volume=volume,
                filled_volume=order.filled_qty,
                execution_time_ms=(datetime.now() - start_time).total_seconds() * 1000
            )
        else:
            return OrderResult(
                success=False,
                error=f"Order status: {order.status}",
                execution_time_ms=(datetime.now() - start_time).total_seconds() * 1000
            )

    async def place_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        volume: float,
        price: float,
        magic: int = 0,
        comment: str = ""
    ) -> OrderResult:
        """Place limit order via FIX"""
        cl_ord_id = self._generate_cl_ord_id()

        logger.info(f"FIX limit order: {side.value} {volume} {symbol} @ {price}")

        if self._mock_mode:
            await asyncio.sleep(0.02)
            self._mock_ticket_counter += 1
            return OrderResult(
                success=True,
                ticket=self._mock_ticket_counter,
                order_id=cl_ord_id,
                price=price,
                volume=volume
            )

        # Build limit order message
        msg = self._build_header(FIXMsgType.NEW_ORDER_SINGLE)
        msg.append_pair(FIXTag.CL_ORD_ID, cl_ord_id)
        msg.append_pair(FIXTag.SYMBOL, symbol)
        msg.append_pair(FIXTag.SIDE, '1' if side == OrderSide.BUY else '2')
        msg.append_pair(FIXTag.ORDER_QTY, volume)
        msg.append_pair(FIXTag.ORD_TYPE, '2')  # Limit
        msg.append_pair(FIXTag.PRICE, price)
        msg.append_pair(FIXTag.TIME_IN_FORCE, '1')  # GTC
        msg.append_pair(FIXTag.TRANSACT_TIME, datetime.utcnow().strftime('%Y%m%d-%H:%M:%S'))

        if await self._send_message(msg):
            return OrderResult(
                success=True,
                order_id=cl_ord_id,
                price=price,
                volume=volume
            )
        else:
            return OrderResult(success=False, error="Failed to send limit order")

    async def execute_pegged_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        volume: float,
        timeout_seconds: int,
        peg_interval_seconds: float,
        ticket: Optional[int] = None
    ) -> OrderResult:
        """Execute pegged limit order"""
        start_time = datetime.now()
        iterations = 0
        last_order_id = None

        logger.info(f"FIX pegged limit: {side.value} {volume} {symbol}")

        while True:
            iterations += 1
            elapsed = (datetime.now() - start_time).total_seconds()

            if elapsed >= timeout_seconds:
                if last_order_id:
                    await self.cancel_order(last_order_id)
                logger.info(f"Pegged limit timeout, falling back to market")

                if ticket:
                    return await self.close_position(ticket, volume)
                else:
                    return await self.place_market_order(symbol, side, volume)

            tick = await self.get_tick(symbol)
            if tick is None:
                await asyncio.sleep(0.5)
                continue

            price = tick.bid if side == OrderSide.BUY else tick.ask

            if last_order_id:
                modified = await self.modify_order(last_order_id, price=price)
                if not modified:
                    # Check if filled
                    if last_order_id in self._pending_orders:
                        order = self._pending_orders[last_order_id]
                        if order.status == '2':  # Filled
                            return OrderResult(
                                success=True,
                                ticket=hash(last_order_id) % 10000000,
                                order_id=order.order_id,
                                price=order.avg_price,
                                volume=order.filled_qty,
                                execution_time_ms=(datetime.now() - start_time).total_seconds() * 1000
                            )
            else:
                result = await self.place_limit_order(symbol, side, volume, price)
                if result.success:
                    last_order_id = result.order_id

            await asyncio.sleep(peg_interval_seconds)

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel pending order"""
        if self._mock_mode:
            return True

        msg = self._build_header(FIXMsgType.ORDER_CANCEL_REQUEST)
        msg.append_pair(FIXTag.ORIG_CL_ORD_ID, order_id)
        msg.append_pair(FIXTag.CL_ORD_ID, self._generate_cl_ord_id())
        msg.append_pair(FIXTag.TRANSACT_TIME, datetime.utcnow().strftime('%Y%m%d-%H:%M:%S'))

        return await self._send_message(msg)

    async def modify_order(
        self,
        order_id: str,
        price: Optional[float] = None,
        volume: Optional[float] = None
    ) -> bool:
        """Modify pending order"""
        if self._mock_mode:
            return True

        if order_id not in self._pending_orders:
            return False

        order = self._pending_orders[order_id]

        msg = self._build_header(FIXMsgType.ORDER_CANCEL_REPLACE)
        msg.append_pair(FIXTag.ORIG_CL_ORD_ID, order_id)
        msg.append_pair(FIXTag.CL_ORD_ID, self._generate_cl_ord_id())
        msg.append_pair(FIXTag.SYMBOL, order.symbol)
        msg.append_pair(FIXTag.SIDE, '1' if order.side == OrderSide.BUY else '2')
        msg.append_pair(FIXTag.ORDER_QTY, volume or order.quantity)
        msg.append_pair(FIXTag.ORD_TYPE, order.order_type)
        if price:
            msg.append_pair(FIXTag.PRICE, price)
        msg.append_pair(FIXTag.TRANSACT_TIME, datetime.utcnow().strftime('%Y%m%d-%H:%M:%S'))

        return await self._send_message(msg)

    # ==================== Position Management ====================

    async def close_position(
        self,
        ticket: int,
        volume: Optional[float] = None
    ) -> OrderResult:
        """Close position by placing opposite order"""
        logger.info(f"FIX close position: ticket={ticket}")
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

        # Find position
        position = None
        for pos in self._positions.values():
            if pos.ticket == ticket:
                position = pos
                break

        if position is None:
            return OrderResult(success=False, error="Position not found")

        # Place closing order (opposite side)
        close_side = OrderSide.SELL if position.position_type == PositionType.LONG else OrderSide.BUY
        close_volume = volume or position.volume

        return await self.place_market_order(
            position.symbol,
            close_side,
            close_volume
        )

    async def get_positions(self, symbol: Optional[str] = None) -> List[Position]:
        """Get open positions"""
        if self._mock_mode:
            positions = list(self._mock_positions.values())
            if symbol:
                positions = [p for p in positions if p.symbol == symbol]
            return positions

        positions = list(self._positions.values())
        if symbol:
            positions = [p for p in positions if p.symbol == symbol]
        return positions

    async def get_position_by_ticket(self, ticket: int) -> Optional[Position]:
        """Get position by ticket"""
        positions = await self.get_positions()
        for pos in positions:
            if pos.ticket == ticket:
                return pos
        return None

    # ==================== Account Information ====================

    async def get_account_info(self) -> Optional[AccountInfo]:
        """Get account info (limited in FIX)"""
        # FIX doesn't have standard account info messages
        # Return estimated values based on positions
        if self._mock_mode:
            return AccountInfo(
                balance=100000.0,
                equity=100000.0,
                margin=0.0,
                free_margin=100000.0,
                currency="USD",
                leverage=100,
                name="FIX Account",
                server=f"{self.config.fix_host}:{self.config.fix_port}"
            )

        # Calculate based on positions
        total_profit = sum(p.profit for p in self._positions.values())
        base_balance = 100000.0  # Would need to be configured

        return AccountInfo(
            balance=base_balance,
            equity=base_balance + total_profit,
            margin=0.0,
            free_margin=base_balance + total_profit,
            currency="USD",
            name="FIX Account"
        )
