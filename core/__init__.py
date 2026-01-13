"""
Core Trading Module

Contains the main trading logic components:
- SignalGenerator: Z-score and Hurst exponent calculations
- OrderOrchestrator: Synchronized multi-broker order execution
- TradingEngine: Main trading loop and coordination
- IPCManager: Inter-process communication
"""

from .signals import SignalGenerator, Signal, SignalType, MarketRegime
from .orchestrator import OrderOrchestrator, SpreadOrder, LegOrder
from .trading_engine import TradingEngine, EngineState, MarketState
from .ipc import IPCManager, IPCMessage, MessageType, CommandType

__all__ = [
    'SignalGenerator',
    'Signal',
    'SignalType',
    'MarketRegime',
    'OrderOrchestrator',
    'SpreadOrder',
    'LegOrder',
    'TradingEngine',
    'EngineState',
    'MarketState',
    'IPCManager',
    'IPCMessage',
    'MessageType',
    'CommandType',
]
