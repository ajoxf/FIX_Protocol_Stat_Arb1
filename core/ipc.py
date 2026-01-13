"""
Inter-Process Communication Layer

Provides communication between the main trading process and broker worker processes.
Supports both Redis and ZeroMQ backends with a fallback to in-memory for development.

Message Types:
- TICK: Real-time price updates
- ORDER_REQUEST: Order execution requests
- ORDER_RESULT: Order execution results
- POSITION_UPDATE: Position state changes
- HEARTBEAT: Connection health checks
- COMMAND: Control commands (connect, disconnect, etc.)
"""

import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, Callable, List
import logging
import uuid

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """IPC message types"""
    TICK = "TICK"
    ORDER_REQUEST = "ORDER_REQUEST"
    ORDER_RESULT = "ORDER_RESULT"
    POSITION_UPDATE = "POSITION_UPDATE"
    ACCOUNT_UPDATE = "ACCOUNT_UPDATE"
    HEARTBEAT = "HEARTBEAT"
    COMMAND = "COMMAND"
    STATUS = "STATUS"
    ERROR = "ERROR"


class CommandType(Enum):
    """Control command types"""
    CONNECT = "CONNECT"
    DISCONNECT = "DISCONNECT"
    SUBSCRIBE = "SUBSCRIBE"
    UNSUBSCRIBE = "UNSUBSCRIBE"
    GET_POSITIONS = "GET_POSITIONS"
    GET_ACCOUNT = "GET_ACCOUNT"
    SHUTDOWN = "SHUTDOWN"


@dataclass
class IPCMessage:
    """
    Standard IPC message format.

    All messages between processes use this structure.
    """
    msg_type: str
    broker_id: str
    payload: Dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    msg_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    correlation_id: Optional[str] = None  # For request/response matching

    def to_json(self) -> str:
        """Serialize to JSON"""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> 'IPCMessage':
        """Deserialize from JSON"""
        parsed = json.loads(data)
        return cls(**parsed)

    @classmethod
    def tick(cls, broker_id: str, symbol: str, bid: float, ask: float) -> 'IPCMessage':
        """Create tick message"""
        return cls(
            msg_type=MessageType.TICK.value,
            broker_id=broker_id,
            payload={
                'symbol': symbol,
                'bid': bid,
                'ask': ask,
                'mid': (bid + ask) / 2,
                'spread': ask - bid
            }
        )

    @classmethod
    def order_request(
        cls,
        broker_id: str,
        action: str,
        symbol: str,
        side: str,
        volume: float,
        order_type: str = "MARKET",
        price: Optional[float] = None,
        ticket: Optional[int] = None,
        correlation_id: Optional[str] = None
    ) -> 'IPCMessage':
        """Create order request message"""
        return cls(
            msg_type=MessageType.ORDER_REQUEST.value,
            broker_id=broker_id,
            payload={
                'action': action,  # 'OPEN', 'CLOSE', 'MODIFY', 'CANCEL'
                'symbol': symbol,
                'side': side,
                'volume': volume,
                'order_type': order_type,
                'price': price,
                'ticket': ticket
            },
            correlation_id=correlation_id or uuid.uuid4().hex[:12]
        )

    @classmethod
    def order_result(
        cls,
        broker_id: str,
        success: bool,
        ticket: Optional[int] = None,
        price: Optional[float] = None,
        error: Optional[str] = None,
        correlation_id: Optional[str] = None
    ) -> 'IPCMessage':
        """Create order result message"""
        return cls(
            msg_type=MessageType.ORDER_RESULT.value,
            broker_id=broker_id,
            payload={
                'success': success,
                'ticket': ticket,
                'price': price,
                'error': error
            },
            correlation_id=correlation_id
        )

    @classmethod
    def command(cls, broker_id: str, command: CommandType, **kwargs) -> 'IPCMessage':
        """Create command message"""
        return cls(
            msg_type=MessageType.COMMAND.value,
            broker_id=broker_id,
            payload={
                'command': command.value,
                **kwargs
            },
            correlation_id=uuid.uuid4().hex[:12]
        )

    @classmethod
    def heartbeat(cls, broker_id: str, status: str, latency_ms: Optional[float] = None) -> 'IPCMessage':
        """Create heartbeat message"""
        return cls(
            msg_type=MessageType.HEARTBEAT.value,
            broker_id=broker_id,
            payload={
                'status': status,
                'latency_ms': latency_ms
            }
        )


class IPCBackend(ABC):
    """Abstract base class for IPC backends"""

    @abstractmethod
    async def connect(self) -> bool:
        """Connect to message broker"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from message broker"""
        pass

    @abstractmethod
    async def publish(self, channel: str, message: IPCMessage) -> bool:
        """Publish message to channel"""
        pass

    @abstractmethod
    async def subscribe(self, channel: str, callback: Callable[[IPCMessage], None]) -> bool:
        """Subscribe to channel with callback"""
        pass

    @abstractmethod
    async def unsubscribe(self, channel: str) -> bool:
        """Unsubscribe from channel"""
        pass

    @abstractmethod
    async def request(self, channel: str, message: IPCMessage, timeout: float = 5.0) -> Optional[IPCMessage]:
        """Send request and wait for response"""
        pass


class InMemoryBackend(IPCBackend):
    """
    In-memory IPC backend for single-process development/testing.

    Uses asyncio queues for message passing.
    """

    def __init__(self):
        self._channels: Dict[str, List[Callable]] = {}
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._connected = False

    async def connect(self) -> bool:
        self._connected = True
        logger.info("InMemory IPC backend connected")
        return True

    async def disconnect(self) -> None:
        self._connected = False
        self._channels.clear()
        self._pending_requests.clear()

    async def publish(self, channel: str, message: IPCMessage) -> bool:
        if not self._connected:
            return False

        # Check for pending request waiting for this response
        if message.correlation_id and message.correlation_id in self._pending_requests:
            future = self._pending_requests.pop(message.correlation_id)
            if not future.done():
                future.set_result(message)
            return True

        # Notify subscribers
        if channel in self._channels:
            for callback in self._channels[channel]:
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, callback, message
                    )
                except Exception as e:
                    logger.error(f"Callback error on {channel}: {e}")

        return True

    async def subscribe(self, channel: str, callback: Callable[[IPCMessage], None]) -> bool:
        if channel not in self._channels:
            self._channels[channel] = []
        self._channels[channel].append(callback)
        logger.debug(f"Subscribed to channel: {channel}")
        return True

    async def unsubscribe(self, channel: str) -> bool:
        if channel in self._channels:
            del self._channels[channel]
        return True

    async def request(self, channel: str, message: IPCMessage, timeout: float = 5.0) -> Optional[IPCMessage]:
        if not message.correlation_id:
            message.correlation_id = uuid.uuid4().hex[:12]

        # Create future for response
        future = asyncio.get_event_loop().create_future()
        self._pending_requests[message.correlation_id] = future

        # Publish request
        await self.publish(channel, message)

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(message.correlation_id, None)
            logger.warning(f"Request timeout on {channel}: {message.correlation_id}")
            return None


class RedisBackend(IPCBackend):
    """
    Redis pub/sub IPC backend for multi-process deployment.

    Requires redis-py[hiredis] package.
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self._host = host
        self._port = port
        self._db = db
        self._redis = None
        self._pubsub = None
        self._subscriptions: Dict[str, Callable] = {}
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._listener_task: Optional[asyncio.Task] = None

    async def connect(self) -> bool:
        try:
            import redis.asyncio as redis
            self._redis = redis.Redis(
                host=self._host,
                port=self._port,
                db=self._db,
                decode_responses=True
            )
            await self._redis.ping()
            self._pubsub = self._redis.pubsub()
            logger.info(f"Redis IPC connected: {self._host}:{self._port}")
            return True
        except ImportError:
            logger.error("redis package not installed. Install with: pip install redis[hiredis]")
            return False
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()

        if self._pubsub:
            await self._pubsub.close()

        if self._redis:
            await self._redis.close()

        logger.info("Redis IPC disconnected")

    async def publish(self, channel: str, message: IPCMessage) -> bool:
        if not self._redis:
            return False

        try:
            await self._redis.publish(channel, message.to_json())
            return True
        except Exception as e:
            logger.error(f"Redis publish error: {e}")
            return False

    async def subscribe(self, channel: str, callback: Callable[[IPCMessage], None]) -> bool:
        if not self._pubsub:
            return False

        try:
            await self._pubsub.subscribe(channel)
            self._subscriptions[channel] = callback

            # Start listener if not running
            if self._listener_task is None or self._listener_task.done():
                self._listener_task = asyncio.create_task(self._listener_loop())

            logger.debug(f"Subscribed to Redis channel: {channel}")
            return True
        except Exception as e:
            logger.error(f"Redis subscribe error: {e}")
            return False

    async def unsubscribe(self, channel: str) -> bool:
        if not self._pubsub:
            return False

        try:
            await self._pubsub.unsubscribe(channel)
            self._subscriptions.pop(channel, None)
            return True
        except Exception as e:
            logger.error(f"Redis unsubscribe error: {e}")
            return False

    async def _listener_loop(self):
        """Background task to receive messages"""
        try:
            async for message in self._pubsub.listen():
                if message['type'] == 'message':
                    channel = message['channel']
                    data = message['data']

                    try:
                        ipc_msg = IPCMessage.from_json(data)

                        # Check for pending request
                        if ipc_msg.correlation_id in self._pending_requests:
                            future = self._pending_requests.pop(ipc_msg.correlation_id)
                            if not future.done():
                                future.set_result(ipc_msg)
                            continue

                        # Call subscriber callback
                        if channel in self._subscriptions:
                            callback = self._subscriptions[channel]
                            await asyncio.get_event_loop().run_in_executor(
                                None, callback, ipc_msg
                            )

                    except Exception as e:
                        logger.error(f"Message processing error: {e}")

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Redis listener error: {e}")

    async def request(self, channel: str, message: IPCMessage, timeout: float = 5.0) -> Optional[IPCMessage]:
        if not message.correlation_id:
            message.correlation_id = uuid.uuid4().hex[:12]

        # Subscribe to response channel
        response_channel = f"{channel}:response:{message.correlation_id}"
        future = asyncio.get_event_loop().create_future()
        self._pending_requests[message.correlation_id] = future

        await self._pubsub.subscribe(response_channel)

        try:
            # Publish request
            await self.publish(channel, message)

            # Wait for response
            return await asyncio.wait_for(future, timeout=timeout)

        except asyncio.TimeoutError:
            logger.warning(f"Request timeout: {message.correlation_id}")
            return None

        finally:
            self._pending_requests.pop(message.correlation_id, None)
            await self._pubsub.unsubscribe(response_channel)


class IPCManager:
    """
    High-level IPC manager for the trading system.

    Provides:
    - Automatic backend selection (Redis if available, else InMemory)
    - Channel naming conventions
    - Message routing
    - Request/response handling
    """

    # Channel naming conventions
    CHANNEL_TICKS = "arb:ticks:{broker_id}"
    CHANNEL_ORDERS = "arb:orders:{broker_id}"
    CHANNEL_POSITIONS = "arb:positions:{broker_id}"
    CHANNEL_COMMANDS = "arb:commands:{broker_id}"
    CHANNEL_STATUS = "arb:status"
    CHANNEL_BROADCAST = "arb:broadcast"

    def __init__(self, backend: Optional[IPCBackend] = None, use_redis: bool = True):
        """
        Initialize IPC manager.

        Args:
            backend: Specific backend to use (auto-selects if None)
            use_redis: Try Redis first if True
        """
        self._backend = backend
        self._use_redis = use_redis
        self._message_handlers: Dict[str, List[Callable]] = {}

    async def initialize(self) -> bool:
        """Initialize IPC backend"""
        if self._backend is None:
            if self._use_redis:
                # Try Redis first
                redis_backend = RedisBackend()
                if await redis_backend.connect():
                    self._backend = redis_backend
                    logger.info("Using Redis IPC backend")
                else:
                    logger.warning("Redis unavailable, falling back to InMemory")
                    self._backend = InMemoryBackend()
                    await self._backend.connect()
            else:
                self._backend = InMemoryBackend()
                await self._backend.connect()
        else:
            await self._backend.connect()

        return True

    async def shutdown(self) -> None:
        """Shutdown IPC manager"""
        if self._backend:
            await self._backend.disconnect()

    def _get_channel(self, template: str, broker_id: str = "") -> str:
        """Get channel name from template"""
        return template.format(broker_id=broker_id)

    # ==================== Tick Data ====================

    async def publish_tick(self, broker_id: str, symbol: str, bid: float, ask: float) -> bool:
        """Publish tick data from broker worker"""
        channel = self._get_channel(self.CHANNEL_TICKS, broker_id)
        message = IPCMessage.tick(broker_id, symbol, bid, ask)
        return await self._backend.publish(channel, message)

    async def subscribe_ticks(self, broker_id: str, callback: Callable[[IPCMessage], None]) -> bool:
        """Subscribe to tick data from broker"""
        channel = self._get_channel(self.CHANNEL_TICKS, broker_id)
        return await self._backend.subscribe(channel, callback)

    # ==================== Orders ====================

    async def send_order_request(
        self,
        broker_id: str,
        action: str,
        symbol: str,
        side: str,
        volume: float,
        order_type: str = "MARKET",
        price: Optional[float] = None,
        ticket: Optional[int] = None,
        timeout: float = 30.0
    ) -> Optional[IPCMessage]:
        """Send order request and wait for result"""
        channel = self._get_channel(self.CHANNEL_ORDERS, broker_id)
        message = IPCMessage.order_request(
            broker_id=broker_id,
            action=action,
            symbol=symbol,
            side=side,
            volume=volume,
            order_type=order_type,
            price=price,
            ticket=ticket
        )
        return await self._backend.request(channel, message, timeout=timeout)

    async def subscribe_orders(self, broker_id: str, callback: Callable[[IPCMessage], None]) -> bool:
        """Subscribe to order requests (for broker workers)"""
        channel = self._get_channel(self.CHANNEL_ORDERS, broker_id)
        return await self._backend.subscribe(channel, callback)

    async def publish_order_result(
        self,
        broker_id: str,
        success: bool,
        ticket: Optional[int] = None,
        price: Optional[float] = None,
        error: Optional[str] = None,
        correlation_id: Optional[str] = None
    ) -> bool:
        """Publish order result (from broker workers)"""
        channel = self._get_channel(self.CHANNEL_ORDERS, broker_id)
        message = IPCMessage.order_result(
            broker_id=broker_id,
            success=success,
            ticket=ticket,
            price=price,
            error=error,
            correlation_id=correlation_id
        )
        return await self._backend.publish(channel, message)

    # ==================== Commands ====================

    async def send_command(
        self,
        broker_id: str,
        command: CommandType,
        timeout: float = 10.0,
        **kwargs
    ) -> Optional[IPCMessage]:
        """Send command to broker worker"""
        channel = self._get_channel(self.CHANNEL_COMMANDS, broker_id)
        message = IPCMessage.command(broker_id, command, **kwargs)
        return await self._backend.request(channel, message, timeout=timeout)

    async def subscribe_commands(self, broker_id: str, callback: Callable[[IPCMessage], None]) -> bool:
        """Subscribe to commands (for broker workers)"""
        channel = self._get_channel(self.CHANNEL_COMMANDS, broker_id)
        return await self._backend.subscribe(channel, callback)

    # ==================== Status ====================

    async def publish_status(self, broker_id: str, status: str, latency_ms: Optional[float] = None) -> bool:
        """Publish broker status"""
        message = IPCMessage.heartbeat(broker_id, status, latency_ms)
        return await self._backend.publish(self.CHANNEL_STATUS, message)

    async def subscribe_status(self, callback: Callable[[IPCMessage], None]) -> bool:
        """Subscribe to broker status updates"""
        return await self._backend.subscribe(self.CHANNEL_STATUS, callback)

    # ==================== Broadcast ====================

    async def broadcast(self, message: IPCMessage) -> bool:
        """Broadcast message to all listeners"""
        return await self._backend.publish(self.CHANNEL_BROADCAST, message)

    async def subscribe_broadcast(self, callback: Callable[[IPCMessage], None]) -> bool:
        """Subscribe to broadcast channel"""
        return await self._backend.subscribe(self.CHANNEL_BROADCAST, callback)
