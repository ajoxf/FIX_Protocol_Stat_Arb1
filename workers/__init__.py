"""
Worker Processes Module

Provides broker worker processes for multi-process architecture.
Each broker connection runs in its own process to support
multiple MT5 terminals and independent connections.
"""

from .broker_worker import BrokerWorker

__all__ = ['BrokerWorker']
