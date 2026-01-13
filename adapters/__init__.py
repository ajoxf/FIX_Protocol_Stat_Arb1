"""
Broker Adapters Package

Provides a unified interface for connecting to different broker backends:
- MT5 (MetaTrader 5) - Retail brokers
- FIX Protocol - ECN/Prime brokers
- FlexTrade - Institutional
- Interactive Brokers - Multi-asset
"""

from .base import (
    BrokerAdapter,
    Tick,
    OrderResult,
    Position,
    AccountInfo,
    OrderType,
    OrderSide,
    PositionType,
)
from .mt5_adapter import MT5Adapter
from .fix_adapter import FIXAdapter

__all__ = [
    'BrokerAdapter',
    'Tick',
    'OrderResult',
    'Position',
    'AccountInfo',
    'OrderType',
    'OrderSide',
    'PositionType',
    'MT5Adapter',
    'FIXAdapter',
]
