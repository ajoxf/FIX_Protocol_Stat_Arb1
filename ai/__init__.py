"""
AI Module for Multi-Broker Statistical Arbitrage System

Provides intelligent analysis and monitoring capabilities:
- TradeAnalyzer: Analyzes trade performance and provides recommendations
- LogMonitor: Real-time log analysis with pattern detection
"""

from .trade_analyzer import TradeAnalyzer
from .log_monitor import LogMonitor

__all__ = ['TradeAnalyzer', 'LogMonitor']
