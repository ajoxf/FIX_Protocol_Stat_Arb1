"""
Log Monitor

Provides real-time log analysis with pattern detection for:
- MT5 connection issues
- FIX protocol errors
- Order execution problems
- System health monitoring
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import deque
from enum import Enum


class LogLevel(Enum):
    """Log severity levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class IssueCategory(Enum):
    """Categories of detected issues"""
    MT5_CONNECTION = "MT5 Connection"
    MT5_ORDER = "MT5 Order Execution"
    FIX_SESSION = "FIX Session"
    FIX_ORDER = "FIX Order"
    DATABASE = "Database"
    NETWORK = "Network"
    SYSTEM = "System"
    TRADING = "Trading Logic"


@dataclass
class LogEntry:
    """Parsed log entry"""
    timestamp: datetime
    level: LogLevel
    source: str
    message: str
    raw: str


@dataclass
class DetectedIssue:
    """A detected issue from log analysis"""
    category: IssueCategory
    severity: LogLevel
    message: str
    count: int = 1
    first_seen: datetime = field(default_factory=datetime.now)
    last_seen: datetime = field(default_factory=datetime.now)
    suggested_action: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'category': self.category.value,
            'severity': self.severity.value,
            'message': self.message,
            'count': self.count,
            'first_seen': self.first_seen.isoformat(),
            'last_seen': self.last_seen.isoformat(),
            'suggested_action': self.suggested_action
        }


@dataclass
class SystemHealth:
    """Overall system health status"""
    status: str  # "Healthy", "Warning", "Critical"
    uptime_seconds: float
    error_count: int
    warning_count: int
    active_issues: List[DetectedIssue]
    last_check: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            'status': self.status,
            'uptime_seconds': self.uptime_seconds,
            'error_count': self.error_count,
            'warning_count': self.warning_count,
            'active_issues': [i.to_dict() for i in self.active_issues],
            'last_check': self.last_check.isoformat()
        }


class LogMonitor:
    """
    Real-time log monitoring and analysis.
    """

    # Known error patterns and their categories
    ERROR_PATTERNS = [
        # MT5 patterns
        (r'MT5.*init.*failed', IssueCategory.MT5_CONNECTION,
         "Check MT5 terminal is running and path is correct"),
        (r'MT5.*login.*failed', IssueCategory.MT5_CONNECTION,
         "Verify MT5 credentials (account, password, server)"),
        (r'MT5.*timeout', IssueCategory.MT5_CONNECTION,
         "MT5 terminal may be unresponsive, try restarting"),
        (r'order_send.*failed', IssueCategory.MT5_ORDER,
         "Check symbol, lot size, and account permissions"),
        (r'TRADE_RETCODE_(?!DONE)', IssueCategory.MT5_ORDER,
         "Order rejected by broker, check error code"),
        (r'Invalid.*volume', IssueCategory.MT5_ORDER,
         "Lot size may be outside allowed range"),
        (r'Not enough money', IssueCategory.MT5_ORDER,
         "Insufficient margin for trade"),

        # FIX patterns
        (r'FIX.*session.*disconnect', IssueCategory.FIX_SESSION,
         "FIX session lost, will attempt reconnect"),
        (r'FIX.*logon.*reject', IssueCategory.FIX_SESSION,
         "FIX credentials rejected, verify SenderCompID/TargetCompID"),
        (r'FIX.*heartbeat.*timeout', IssueCategory.FIX_SESSION,
         "Network latency issue or counterparty unresponsive"),
        (r'ExecutionReport.*Reject', IssueCategory.FIX_ORDER,
         "Order rejected, check reject reason in message"),
        (r'35=8.*150=8', IssueCategory.FIX_ORDER,
         "Order rejected by exchange/broker"),

        # Database patterns
        (r'database.*locked', IssueCategory.DATABASE,
         "Database lock conflict, ensure single writer"),
        (r'sqlite.*error', IssueCategory.DATABASE,
         "Database error, check disk space and permissions"),

        # Network patterns
        (r'connection.*refused', IssueCategory.NETWORK,
         "Target server not accepting connections"),
        (r'connection.*reset', IssueCategory.NETWORK,
         "Connection dropped, check network stability"),
        (r'timeout.*connect', IssueCategory.NETWORK,
         "Connection timeout, check firewall and network"),

        # Trading logic patterns
        (r'Cannot start engine', IssueCategory.TRADING,
         "Engine state issue, try stopping and restarting"),
        (r'No.*broker.*configured', IssueCategory.TRADING,
         "Configure at least one broker for each leg"),
        (r'spread.*invalid', IssueCategory.TRADING,
         "Price data incomplete, check broker connections"),
    ]

    def __init__(self, max_entries: int = 1000, issue_timeout_minutes: int = 30):
        """
        Initialize log monitor.

        Args:
            max_entries: Maximum log entries to keep in memory
            issue_timeout_minutes: Minutes after which resolved issues are cleared
        """
        self.max_entries = max_entries
        self.issue_timeout = timedelta(minutes=issue_timeout_minutes)

        self.log_buffer: deque = deque(maxlen=max_entries)
        self.issues: Dict[str, DetectedIssue] = {}
        self.error_count = 0
        self.warning_count = 0
        self.start_time = datetime.now()

        # Compile regex patterns
        self.compiled_patterns = [
            (re.compile(pattern, re.IGNORECASE), category, action)
            for pattern, category, action in self.ERROR_PATTERNS
        ]

        # Set up log handler
        self._setup_handler()

    def _setup_handler(self):
        """Set up logging handler to capture logs"""
        self.handler = LogCaptureHandler(self)
        self.handler.setLevel(logging.DEBUG)

        # Add to root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(self.handler)

    def process_log(self, record: logging.LogRecord) -> None:
        """
        Process a log record.

        Args:
            record: Python logging LogRecord
        """
        # Create log entry
        entry = LogEntry(
            timestamp=datetime.fromtimestamp(record.created),
            level=LogLevel[record.levelname] if record.levelname in LogLevel.__members__ else LogLevel.INFO,
            source=record.name,
            message=record.getMessage(),
            raw=record.getMessage()
        )

        # Add to buffer
        self.log_buffer.append(entry)

        # Update counts
        if entry.level == LogLevel.ERROR or entry.level == LogLevel.CRITICAL:
            self.error_count += 1
        elif entry.level == LogLevel.WARNING:
            self.warning_count += 1

        # Check for known patterns
        self._check_patterns(entry)

    def _check_patterns(self, entry: LogEntry) -> None:
        """Check log entry against known error patterns"""
        for pattern, category, action in self.compiled_patterns:
            if pattern.search(entry.message):
                issue_key = f"{category.value}:{pattern.pattern[:30]}"

                if issue_key in self.issues:
                    # Update existing issue
                    self.issues[issue_key].count += 1
                    self.issues[issue_key].last_seen = entry.timestamp
                else:
                    # Create new issue
                    self.issues[issue_key] = DetectedIssue(
                        category=category,
                        severity=entry.level,
                        message=entry.message[:200],
                        count=1,
                        first_seen=entry.timestamp,
                        last_seen=entry.timestamp,
                        suggested_action=action
                    )

    def get_health(self) -> SystemHealth:
        """
        Get current system health status.

        Returns:
            SystemHealth with current status
        """
        # Clean up old issues
        self._cleanup_old_issues()

        # Determine status
        active_issues = list(self.issues.values())
        critical_count = sum(1 for i in active_issues if i.severity == LogLevel.CRITICAL)
        error_count = sum(1 for i in active_issues if i.severity == LogLevel.ERROR)

        if critical_count > 0:
            status = "Critical"
        elif error_count > 0:
            status = "Warning"
        else:
            status = "Healthy"

        return SystemHealth(
            status=status,
            uptime_seconds=(datetime.now() - self.start_time).total_seconds(),
            error_count=self.error_count,
            warning_count=self.warning_count,
            active_issues=active_issues,
            last_check=datetime.now()
        )

    def _cleanup_old_issues(self) -> None:
        """Remove issues that haven't recurred recently"""
        now = datetime.now()
        to_remove = []

        for key, issue in self.issues.items():
            if now - issue.last_seen > self.issue_timeout:
                to_remove.append(key)

        for key in to_remove:
            del self.issues[key]

    def get_recent_logs(self, level: Optional[str] = None,
                        limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent log entries.

        Args:
            level: Filter by log level (ERROR, WARNING, etc.)
            limit: Maximum entries to return

        Returns:
            List of log entry dicts
        """
        logs = list(self.log_buffer)[-limit:]

        if level:
            try:
                filter_level = LogLevel[level.upper()]
                logs = [l for l in logs if l.level == filter_level]
            except KeyError:
                pass

        return [
            {
                'timestamp': l.timestamp.isoformat(),
                'level': l.level.value,
                'source': l.source,
                'message': l.message
            }
            for l in logs
        ]

    def get_issues(self, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get active issues.

        Args:
            category: Filter by issue category

        Returns:
            List of issue dicts
        """
        self._cleanup_old_issues()

        issues = list(self.issues.values())

        if category:
            try:
                filter_cat = IssueCategory[category.upper().replace(' ', '_')]
                issues = [i for i in issues if i.category == filter_cat]
            except KeyError:
                pass

        # Sort by severity and recency
        issues.sort(key=lambda x: (
            x.severity != LogLevel.CRITICAL,
            x.severity != LogLevel.ERROR,
            -x.count
        ))

        return [i.to_dict() for i in issues]

    def clear_issues(self) -> None:
        """Clear all tracked issues"""
        self.issues.clear()

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get log statistics.

        Returns:
            Dict with statistics
        """
        health = self.get_health()

        # Count by level
        level_counts = {level.value: 0 for level in LogLevel}
        for entry in self.log_buffer:
            level_counts[entry.level.value] += 1

        # Count by category
        category_counts = {}
        for issue in self.issues.values():
            cat = issue.category.value
            category_counts[cat] = category_counts.get(cat, 0) + issue.count

        return {
            'health': health.to_dict(),
            'total_entries': len(self.log_buffer),
            'level_distribution': level_counts,
            'issue_categories': category_counts,
            'uptime': str(timedelta(seconds=int(health.uptime_seconds)))
        }


class LogCaptureHandler(logging.Handler):
    """Custom log handler that feeds into LogMonitor"""

    def __init__(self, monitor: LogMonitor):
        super().__init__()
        self.monitor = monitor

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.monitor.process_log(record)
        except Exception:
            pass  # Don't let logging errors crash the app


# Global monitor instance
_monitor: Optional[LogMonitor] = None


def get_monitor() -> LogMonitor:
    """Get or create the global log monitor instance"""
    global _monitor
    if _monitor is None:
        _monitor = LogMonitor()
    return _monitor
