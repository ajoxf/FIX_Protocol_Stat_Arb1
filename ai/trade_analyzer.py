"""
Trade Analyzer

Provides AI-powered analysis of trades including:
- Exit reason classification
- Root cause detection
- Cost analysis
- Recommendations for improvement
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from enum import Enum


class ExitReason(Enum):
    """Trade exit reasons"""
    CLOSE = "Normal Close (Z-Score Exit)"
    STOP_LOSS = "Stop Loss (Max SD)"
    MAX_LOSS = "Max Loss Limit"
    OVERNIGHT_CLOSE = "Overnight Protection"
    TIME_STOP = "Time Stop Loss"
    MANUAL = "Manual Close"
    OPPOSITE_SD = "Opposite SD Touch"
    UNKNOWN = "Unknown"


@dataclass
class TradeAnalysis:
    """Analysis result for a trade"""
    trade_id: str
    exit_reason: ExitReason
    entry_quality: str  # "Good", "Fair", "Poor"
    exit_quality: str
    spread_cost: float
    commission_cost: float
    slippage_estimate: float
    hold_duration: timedelta
    max_favorable_excursion: float  # Best unrealized P&L
    max_adverse_excursion: float    # Worst unrealized P&L
    efficiency: float               # Actual P&L / MFE ratio
    root_causes: List[str]
    recommendations: List[str]
    risk_score: int  # 1-10

    def to_dict(self) -> Dict[str, Any]:
        return {
            'trade_id': self.trade_id,
            'exit_reason': self.exit_reason.value,
            'entry_quality': self.entry_quality,
            'exit_quality': self.exit_quality,
            'spread_cost': self.spread_cost,
            'commission_cost': self.commission_cost,
            'slippage_estimate': self.slippage_estimate,
            'hold_duration_seconds': self.hold_duration.total_seconds(),
            'hold_duration_str': str(self.hold_duration),
            'max_favorable_excursion': self.max_favorable_excursion,
            'max_adverse_excursion': self.max_adverse_excursion,
            'efficiency': self.efficiency,
            'root_causes': self.root_causes,
            'recommendations': self.recommendations,
            'risk_score': self.risk_score
        }


class TradeAnalyzer:
    """
    Analyzes trades and provides insights for improvement.
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize analyzer with optional configuration.

        Args:
            config: Trading configuration dict
        """
        self.config = config or {}
        self.entry_threshold = self.config.get('entry_std_dev', 2.0)
        self.exit_threshold = self.config.get('exit_std_dev', 0.5)
        self.stop_loss_threshold = self.config.get('stop_loss_std_dev', 4.0)
        self.commission_per_lot = self.config.get('commission_per_lot', 7.0)
        self.avg_spread_cost = self.config.get('avg_spread_cost', 0.50)

    def analyze_trade(self, trade: Dict, price_history: Optional[List] = None) -> TradeAnalysis:
        """
        Analyze a single trade.

        Args:
            trade: Trade data dict
            price_history: Optional price history during the trade

        Returns:
            TradeAnalysis with insights
        """
        # Determine exit reason
        exit_reason = self._classify_exit_reason(trade)

        # Calculate costs
        lot_size = trade.get('lot_size', 0.01)
        spread_cost = self.avg_spread_cost * lot_size * 2  # Entry + Exit
        commission_cost = self.commission_per_lot * lot_size * 2
        slippage = self._estimate_slippage(trade)

        # Calculate duration
        entry_time = self._parse_time(trade.get('entry_time') or trade.get('entry_date'))
        exit_time = self._parse_time(trade.get('exit_time') or trade.get('exit_date'))

        if entry_time and exit_time:
            duration = exit_time - entry_time
        else:
            duration = timedelta(seconds=0)

        # Assess entry/exit quality
        entry_quality = self._assess_entry_quality(trade)
        exit_quality = self._assess_exit_quality(trade, exit_reason)

        # Calculate excursions (simplified without full price history)
        pnl = trade.get('pnl', 0) or trade.get('net_pnl', 0) or 0
        mfe = max(pnl, 0) + abs(pnl) * 0.2  # Estimate
        mae = min(pnl, 0) - abs(pnl) * 0.1  # Estimate

        # If we have actual price history, calculate real excursions
        if price_history:
            mfe, mae = self._calculate_excursions(trade, price_history)

        # Calculate efficiency
        efficiency = (pnl / mfe * 100) if mfe > 0 else 0

        # Determine root causes for losing trades
        root_causes = self._identify_root_causes(trade, exit_reason, entry_quality, exit_quality)

        # Generate recommendations
        recommendations = self._generate_recommendations(trade, exit_reason, root_causes)

        # Calculate risk score
        risk_score = self._calculate_risk_score(trade, exit_reason, duration)

        return TradeAnalysis(
            trade_id=trade.get('trade_id', 'unknown'),
            exit_reason=exit_reason,
            entry_quality=entry_quality,
            exit_quality=exit_quality,
            spread_cost=spread_cost,
            commission_cost=commission_cost,
            slippage_estimate=slippage,
            hold_duration=duration,
            max_favorable_excursion=mfe,
            max_adverse_excursion=mae,
            efficiency=efficiency,
            root_causes=root_causes,
            recommendations=recommendations,
            risk_score=risk_score
        )

    def _classify_exit_reason(self, trade: Dict) -> ExitReason:
        """Classify why a trade was exited"""
        # Check if explicit close_reason is set
        close_reason = trade.get('close_reason', '')
        if close_reason:
            reason_map = {
                'CLOSE': ExitReason.CLOSE,
                'STOP_LOSS': ExitReason.STOP_LOSS,
                'MAX_LOSS': ExitReason.MAX_LOSS,
                'OVERNIGHT': ExitReason.OVERNIGHT_CLOSE,
                'TIME_STOP': ExitReason.TIME_STOP,
                'MANUAL': ExitReason.MANUAL,
                'OPPOSITE_SD': ExitReason.OPPOSITE_SD
            }
            return reason_map.get(close_reason.upper(), ExitReason.UNKNOWN)

        # Infer from trade data
        exit_zscore = trade.get('zscore_exit') or trade.get('exit_zscore')
        entry_zscore = trade.get('zscore_entry') or trade.get('entry_zscore')
        pnl = trade.get('pnl', 0) or trade.get('net_pnl', 0) or 0

        if exit_zscore is not None:
            # Check if hit stop loss
            if abs(exit_zscore) >= self.stop_loss_threshold:
                return ExitReason.STOP_LOSS

            # Check if normal exit (mean reversion)
            if abs(exit_zscore) <= self.exit_threshold:
                return ExitReason.CLOSE

            # Check for opposite SD touch
            if entry_zscore and exit_zscore:
                if (entry_zscore > 0 and exit_zscore < -self.entry_threshold) or \
                   (entry_zscore < 0 and exit_zscore > self.entry_threshold):
                    return ExitReason.OPPOSITE_SD

        # Check for max loss
        max_loss = self.config.get('max_loss_per_lot', 100) * trade.get('lot_size', 0.01)
        if pnl < -max_loss:
            return ExitReason.MAX_LOSS

        return ExitReason.UNKNOWN

    def _assess_entry_quality(self, trade: Dict) -> str:
        """Assess the quality of trade entry"""
        entry_zscore = trade.get('zscore_entry') or trade.get('entry_zscore')

        if entry_zscore is None:
            return "Unknown"

        abs_z = abs(entry_zscore)

        # Good entry: exactly at or slightly beyond threshold
        if self.entry_threshold <= abs_z <= self.entry_threshold + 0.5:
            return "Good"
        # Fair entry: within reasonable range
        elif self.entry_threshold - 0.3 <= abs_z <= self.entry_threshold + 1.0:
            return "Fair"
        else:
            return "Poor"

    def _assess_exit_quality(self, trade: Dict, exit_reason: ExitReason) -> str:
        """Assess the quality of trade exit"""
        if exit_reason == ExitReason.CLOSE:
            return "Good"  # Normal exit is always good
        elif exit_reason == ExitReason.STOP_LOSS:
            return "Poor"  # Hit stop loss
        elif exit_reason == ExitReason.MAX_LOSS:
            return "Poor"
        elif exit_reason == ExitReason.OVERNIGHT_CLOSE:
            return "Fair"  # Necessary protection
        elif exit_reason == ExitReason.TIME_STOP:
            return "Fair"  # Time-based, not ideal
        else:
            return "Unknown"

    def _estimate_slippage(self, trade: Dict) -> float:
        """Estimate slippage based on execution prices"""
        # This would ideally compare requested vs filled prices
        # For now, estimate based on lot size and volatility
        lot_size = trade.get('lot_size', 0.01)
        return lot_size * 0.10  # $0.10 per lot estimate

    def _calculate_excursions(self, trade: Dict, price_history: List) -> tuple:
        """Calculate MFE and MAE from price history"""
        entry_spread = (trade.get('entry_spot_price', 0) or 0) - (trade.get('entry_futures_price', 0) or 0)
        direction = trade.get('direction', '')

        max_favorable = 0
        max_adverse = 0

        for price in price_history:
            current_spread = price.get('spread', entry_spread)
            spread_diff = current_spread - entry_spread

            # Long spread profits when spread increases
            if 'LONG' in direction.upper():
                if spread_diff > max_favorable:
                    max_favorable = spread_diff
                if spread_diff < max_adverse:
                    max_adverse = spread_diff
            else:  # Short spread
                if -spread_diff > max_favorable:
                    max_favorable = -spread_diff
                if -spread_diff < max_adverse:
                    max_adverse = -spread_diff

        lot_size = trade.get('lot_size', 0.01)
        contract_size = self.config.get('contract_size', 100)

        return (max_favorable * lot_size * contract_size,
                max_adverse * lot_size * contract_size)

    def _identify_root_causes(self, trade: Dict, exit_reason: ExitReason,
                              entry_quality: str, exit_quality: str) -> List[str]:
        """Identify root causes for trade outcome"""
        causes = []
        pnl = trade.get('pnl', 0) or trade.get('net_pnl', 0) or 0

        if pnl < 0:  # Losing trade
            if exit_reason == ExitReason.STOP_LOSS:
                causes.append("Market moved against position significantly")
                causes.append("Z-score expanded beyond stop loss threshold")

            if entry_quality == "Poor":
                causes.append("Entry timing was suboptimal")

            if exit_reason == ExitReason.MAX_LOSS:
                causes.append("Position sizing may have been too aggressive")

            if exit_reason == ExitReason.TIME_STOP:
                causes.append("Trade held too long without mean reversion")
                causes.append("Market regime may have shifted to trending")

        elif pnl > 0:  # Winning trade
            if exit_quality == "Good":
                causes.append("Successful mean reversion trade")

            if entry_quality == "Good":
                causes.append("Well-timed entry at SD threshold")

        if not causes:
            causes.append("Standard trade execution")

        return causes

    def _generate_recommendations(self, trade: Dict, exit_reason: ExitReason,
                                   root_causes: List[str]) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        pnl = trade.get('pnl', 0) or trade.get('net_pnl', 0) or 0

        if pnl < 0:
            if exit_reason == ExitReason.STOP_LOSS:
                recommendations.append("Consider widening stop loss threshold slightly")
                recommendations.append("Review if entry threshold is optimal for current volatility")

            if exit_reason == ExitReason.MAX_LOSS:
                recommendations.append("Review position sizing relative to account")
                recommendations.append("Consider reducing lot size")

            if exit_reason == ExitReason.TIME_STOP:
                recommendations.append("Consider enabling Hurst filter to avoid trending markets")
                recommendations.append("Review lookback period for signal generation")

            if "Entry timing was suboptimal" in root_causes:
                recommendations.append("Consider using limit orders for better entry prices")
                recommendations.append("Wait for confirmation of SD touch before entry")

        elif pnl > 0:
            recommendations.append("Trade executed according to strategy parameters")
            if exit_reason == ExitReason.CLOSE:
                recommendations.append("Consider similar setups in the future")

        if not recommendations:
            recommendations.append("Continue monitoring trade performance")

        return recommendations

    def _calculate_risk_score(self, trade: Dict, exit_reason: ExitReason,
                              duration: timedelta) -> int:
        """Calculate risk score 1-10 (10 = highest risk)"""
        score = 5  # Start neutral
        pnl = trade.get('pnl', 0) or trade.get('net_pnl', 0) or 0

        # Adjust for exit reason
        if exit_reason == ExitReason.STOP_LOSS:
            score += 3
        elif exit_reason == ExitReason.MAX_LOSS:
            score += 4
        elif exit_reason == ExitReason.CLOSE:
            score -= 2

        # Adjust for P&L
        if pnl < -50:
            score += 2
        elif pnl < 0:
            score += 1
        elif pnl > 50:
            score -= 2
        elif pnl > 0:
            score -= 1

        # Adjust for duration
        if duration.total_seconds() > 86400:  # More than 1 day
            score += 1

        return max(1, min(10, score))

    def _parse_time(self, time_str: Optional[str]) -> Optional[datetime]:
        """Parse time string to datetime"""
        if not time_str:
            return None

        try:
            # Try ISO format first
            return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            pass

        try:
            # Try common formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%H:%M:%S']:
                try:
                    return datetime.strptime(time_str, fmt)
                except ValueError:
                    continue
        except:
            pass

        return None

    def analyze_batch(self, trades: List[Dict]) -> Dict[str, Any]:
        """
        Analyze a batch of trades and provide summary statistics.

        Args:
            trades: List of trade dicts

        Returns:
            Summary analysis with aggregate statistics
        """
        if not trades:
            return {'error': 'No trades to analyze'}

        analyses = [self.analyze_trade(t) for t in trades]

        # Aggregate statistics
        exit_reasons = {}
        total_spread_cost = 0
        total_commission = 0
        total_slippage = 0
        risk_scores = []

        for a in analyses:
            reason = a.exit_reason.value
            exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
            total_spread_cost += a.spread_cost
            total_commission += a.commission_cost
            total_slippage += a.slippage_estimate
            risk_scores.append(a.risk_score)

        return {
            'total_trades': len(trades),
            'exit_reason_distribution': exit_reasons,
            'total_costs': {
                'spread': total_spread_cost,
                'commission': total_commission,
                'slippage': total_slippage,
                'total': total_spread_cost + total_commission + total_slippage
            },
            'average_risk_score': sum(risk_scores) / len(risk_scores),
            'high_risk_trades': sum(1 for s in risk_scores if s >= 7)
        }
