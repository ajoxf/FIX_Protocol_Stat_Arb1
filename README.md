# Multi-Broker Statistical Arbitrage System

Multi-broker basis arbitrage trading system - **identical features to the single-broker system**, with the only difference being multi-broker backend connectivity.

## Documentation

- [**Full System Specification**](MULTI_BROKER_ARBITRAGE_SPEC.md) - Complete architecture, features, and requirements

## Key Difference

| Single-Broker System | Multi-Broker System |
|---------------------|---------------------|
| One MT5 terminal | Multiple MT5 terminals |
| Same broker for spot & futures | Dedicated broker per leg |
| Single margin pool | Per-broker + aggregate |

**Everything else (UI, settings, trading logic, analysis) is IDENTICAL.**

## All Features Included

### Trading
- Z-score based entry/exit signals
- Market and Pegged Limit orders
- Stop loss (Z-score, time, max loss, overnight)
- Hurst exponent regime filter

### Settings
- Lookback period (minutes/days)
- Entry/Exit/Stop thresholds (σ)
- Lot size, commission, profit targets
- Overnight protection
- Order execution (MARKET/LIMIT)

### Analysis
- SD Touch tracking (2σ - 4σ)
- Success rate analysis
- Limit order fill statistics
- Cost estimation tools

### UI Pages
- Dashboard with real-time monitoring
- Settings configuration
- Setup page (assets + brokers)
- SD Analysis page

## Architecture

```
Trading Engine
     │
Order Orchestrator (parallel execution)
     │
     ├──► MT5 Worker A (Spot Broker)
     │
     └──► MT5 Worker B (Futures Broker)
```

## Getting Started

See [MULTI_BROKER_ARBITRAGE_SPEC.md](MULTI_BROKER_ARBITRAGE_SPEC.md) for complete implementation details.

## License

Private - All rights reserved
