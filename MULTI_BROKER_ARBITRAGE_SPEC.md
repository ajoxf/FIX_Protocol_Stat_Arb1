# Multi-Broker Basis Arbitrage Trading System

## Overview

Build a basis arbitrage trading system that executes spot and futures legs on **separate MT5 platforms connected to different brokers**. This system mirrors ALL features from the existing single-broker Statistical Arbitrage system - the **ONLY difference is the backend connectivity layer** which supports multiple brokers instead of one.

---

## Key Difference from Single-Broker System

| Aspect | Single-Broker System | Multi-Broker System |
|--------|---------------------|---------------------|
| MT5 Connection | One MT5 terminal | Multiple MT5 terminals (one per broker) |
| Spot Execution | Same broker | Dedicated spot broker |
| Futures Execution | Same broker | Dedicated futures broker |
| Position Tracking | Single ticket per leg | Cross-broker ticket references |
| P&L Calculation | Same account | Aggregated across accounts |
| Risk Management | Single margin pool | Per-broker + aggregate monitoring |

**Everything else (UI, settings, trading logic, analysis) remains IDENTICAL.**

---

## Database Schema

### Table 1: `price_history`
Stores historical price data for mean calculation
```sql
CREATE TABLE price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    asset TEXT DEFAULT 'ACTIVE',
    spot_price REAL,
    futures_price REAL,
    spread REAL,
    swap_diff REAL
);
```

### Table 2: `trading_config`
Master configuration table with all user-adjustable settings
```sql
CREATE TABLE trading_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),

    -- Asset Configuration
    asset_name TEXT DEFAULT 'GOLD',
    spot_symbol TEXT DEFAULT 'XAUUSD',
    futures_symbol TEXT DEFAULT 'GC0226',
    futures_expiry TEXT,
    contract_size REAL DEFAULT 100,
    swap_charge REAL DEFAULT 0,

    -- Signal Parameters
    lookback_period INTEGER DEFAULT 90,
    lookback_unit TEXT DEFAULT 'minutes',
    entry_std_dev REAL DEFAULT 2.0,
    exit_std_dev REAL DEFAULT 0.5,
    stop_loss_std_dev REAL DEFAULT 3.0,

    -- Risk Management
    time_stop_loss_days REAL DEFAULT 0,
    max_positions INTEGER DEFAULT 3,
    lot_size REAL DEFAULT 0.1,
    commission_per_lot REAL DEFAULT 0,
    min_profit_per_lot REAL DEFAULT 50,
    max_loss_per_lot REAL DEFAULT 100,

    -- Hurst Exponent Filter
    hurst_enabled INTEGER DEFAULT 1,
    hurst_threshold REAL DEFAULT 0.5,
    trending_duration_minutes INTEGER DEFAULT 15,

    -- Overnight Protection
    close_before_overnight INTEGER DEFAULT 0,
    overnight_close_hour INTEGER DEFAULT 16,
    overnight_close_minute INTEGER DEFAULT 55,

    -- Order Execution
    order_type TEXT DEFAULT 'MARKET',
    limit_order_timeout INTEGER DEFAULT 60,
    limit_peg_interval REAL DEFAULT 1.5,
    exit_at_opposite_sd REAL DEFAULT 0,

    -- Mode Settings
    algo_enabled INTEGER DEFAULT 0,
    paper_mode INTEGER DEFAULT 1,
    selected_asset TEXT DEFAULT 'GOLD'
);
```

### Table 3: `trades`
Comprehensive trade journal - **EXTENDED for multi-broker**
```sql
CREATE TABLE trades (
    trade_id TEXT PRIMARY KEY,
    asset TEXT,
    direction TEXT,  -- 'Long Spread' or 'Short Spread'

    -- Timestamps
    entry_date TEXT,
    exit_date TEXT,
    days_held REAL,

    -- Z-Scores
    entry_zscore REAL,
    exit_zscore REAL,

    -- Entry Prices
    entry_spot_price REAL,
    entry_futures_price REAL,

    -- Exit Prices
    exit_spot_price REAL,
    exit_futures_price REAL,

    -- P&L Components
    spot_pnl REAL,
    futures_pnl REAL,
    gross_pnl REAL,
    swap_cost REAL,
    commission REAL,
    spread_cost REAL,
    net_pnl REAL,
    return_pct REAL,

    -- Position Details
    lot_size REAL,

    -- MT5 Tickets (EXTENDED: now includes broker reference)
    spot_broker_id TEXT,           -- NEW: Which broker for spot
    mt5_spot_ticket INTEGER,
    futures_broker_id TEXT,        -- NEW: Which broker for futures
    mt5_futures_ticket INTEGER,

    -- Status
    order_status TEXT,
    status TEXT  -- 'OPEN' or 'CLOSED'
);
```

### Table 4: `sd_touch_log`
Tracks spread touches at various standard deviation levels
```sql
CREATE TABLE sd_touch_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT,
    touch_date TEXT,
    touch_time TEXT,
    sd_level TEXT,  -- '2σ', '2.5σ', '3σ', '3.5σ', '4σ'
    direction TEXT,  -- 'HIGH' or 'LOW'
    touch_spread REAL,
    touch_zscore REAL,
    mean_at_touch REAL,
    std_at_touch REAL,
    reached_mean INTEGER DEFAULT 0,
    mean_reached_time TEXT,
    spread_at_mean REAL,
    potential_profit REAL,
    max_adverse_move REAL,
    status TEXT DEFAULT 'PENDING',
    entry_spot_spread REAL,
    entry_futures_spread REAL,
    exit_spot_spread REAL,
    exit_futures_spread REAL
);
```

### Table 5: `limit_order_log`
Tracks all limit order execution attempts - **EXTENDED for multi-broker**
```sql
CREATE TABLE limit_order_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    broker_id TEXT,              -- NEW: Which broker executed this order
    symbol TEXT,
    order_type TEXT,             -- 'PEGGED_LIMIT', 'PEGGED_LIMIT_CLOSE'
    side TEXT,                   -- 'BUY' or 'SELL'
    volume REAL,
    target_price REAL,
    fill_price REAL,
    status TEXT,                 -- 'FILLED', 'TIMEOUT', 'ERROR', 'CANCELLED'
    elapsed_seconds REAL,
    iterations INTEGER,
    error_message TEXT,
    context TEXT                 -- 'ENTRY', 'EXIT', 'TEST'
);
```

### Table 6: `brokers` (NEW)
Multi-broker configuration
```sql
CREATE TABLE brokers (
    broker_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    broker_type TEXT DEFAULT 'MT5',  -- 'MT5', 'FIX', 'IB'
    role TEXT NOT NULL,              -- 'SPOT' or 'FUTURES'
    mt5_path TEXT,
    account INTEGER,
    server TEXT,
    status TEXT DEFAULT 'DISCONNECTED',
    last_heartbeat TEXT,
    latency_ms INTEGER,
    config JSON
);
```

---

## Configuration Settings (ALL from existing system)

### Signal Parameters
| Setting | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `lookback_period` | int | 90 | 1-10000 | Data points for mean calculation |
| `lookback_unit` | str | 'minutes' | minutes/days | Time unit for lookback |
| `entry_std_dev` | float | 2.0 | 0.5-5.0 | Entry threshold in σ |
| `exit_std_dev` | float | 0.5 | -5.0-5.0 | Exit threshold in σ |
| `stop_loss_std_dev` | float | 3.0 | 2.0-6.0 | Stop loss threshold in σ |
| `exit_at_opposite_sd` | float | 0 | 0-5.0 | Exit at opposite SD instead of mean |

### Risk Management
| Setting | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `time_stop_loss_days` | float | 0 | 0-365 | Auto-close after X days (0=disabled) |
| `max_positions` | int | 3 | 1-10 | Maximum concurrent positions |
| `lot_size` | float | 0.1 | 0.01-700 | Position size in lots |
| `commission_per_lot` | float | 0 | 0-100 | Commission per lot in USD |
| `min_profit_per_lot` | float | 50 | 0-50000 | Target profit per lot in USD |
| `max_loss_per_lot` | float | 100 | 0-50000 | Max loss per lot in USD |

### Hurst Exponent Filter
| Setting | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `hurst_enabled` | bool | true | - | Enable/disable Hurst filter |
| `hurst_threshold` | float | 0.5 | 0.3-0.9 | Threshold for trending regime |
| `trending_duration_minutes` | int | 15 | 0-120 | Minutes to confirm trending |

### Overnight Protection
| Setting | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `close_before_overnight` | bool | false | - | Auto-close before swap time |
| `overnight_close_hour` | int | 16 | 0-23 | Close hour (24h format) |
| `overnight_close_minute` | int | 55 | 0-59 | Close minute |

### Order Execution
| Setting | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `order_type` | str | 'MARKET' | MARKET/LIMIT | Order execution type |
| `limit_order_timeout` | int | 60 | 10-300 | Seconds to wait for limit fill |
| `limit_peg_interval` | float | 1.5 | 0.5-10 | Seconds between price updates |

### Mode Settings
| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `algo_enabled` | bool | false | Trading enabled/disabled |
| `paper_mode` | bool | true | Paper or live trading |

---

## Trading Features (ALL from existing system)

### Entry/Exit Logic

**Entry Conditions:**
- Z-score crosses entry_std_dev threshold (e.g., ±2σ)
- Hurst filter allows entry (not in trending regime)
- Max positions not reached
- Entry types:
  - `SELL_BASIS`: Buy spot + sell futures (spread too expensive)
  - `BUY_BASIS`: Sell spot + buy futures (spread too cheap)

**Exit Conditions:**
1. **Statistical Exit**: Z-score returns to exit_std_dev threshold
2. **Stop Loss**: Z-score exceeds stop_loss_std_dev
3. **Time Stop**: Position held longer than time_stop_loss_days
4. **Overnight Close**: Before configured swap time
5. **Max Loss Stop**: Unrealized loss exceeds max_loss_per_lot × lot_size
6. **Opposite SD Exit**: Optional exit at opposite SD (if exit_at_opposite_sd > 0)

### Hurst Exponent Regime Filter

**Regimes:**
- `H < 0.4`: MEAN_REVERTING (allow entries)
- `0.4 ≤ H < 0.6`: RANDOM_WALK (neutral)
- `H ≥ 0.6`: TRENDING (block entries)

**Filter Logic:**
```python
if hurst >= hurst_threshold and trending_for >= trending_duration_minutes:
    block_new_entries()
    display_regime_filter_reason()
```

### Order Types

**Market Orders:**
- Guaranteed fill at market price
- Pays bid-ask spread cost
- Fastest execution

**Pegged Limit Orders:**
- Places limit order at current bid/ask
- Updates price every `limit_peg_interval` seconds
- Timeout after `limit_order_timeout` seconds
- Zero spread cost if filled
- Fallback to market order on exit timeout

### Position Management

**On Entry:**
1. Calculate hedge-equivalent volumes for equal dollar exposure
2. **NEW: Execute on BOTH brokers simultaneously (parallel)**
3. Handle partial fills with rollback
4. Record cross-broker ticket references

**On Exit:**
1. Send close orders to BOTH brokers simultaneously
2. Handle partial closes
3. Calculate combined P&L across brokers

---

## UI Pages (ALL from existing system)

### 1. Dashboard / Monitor Page (`/`)

**Header & Controls:**
- Algo Trading toggle (ON/OFF)
- Mode toggle (PAPER/LIVE)
- Current thresholds display
- Market session indicator
- Links to Settings and SD Analysis

**Account Information Section:**
- **EXTENDED: Show per-broker account info**
  - Spot Broker: Balance, Equity, Margin
  - Futures Broker: Balance, Equity, Margin
  - Combined totals

**Market Summary:**
- CHEAP / FAIR / EXPENSIVE spread counts

**Asset Card:**
- Prices: Spot, Futures, Basis
- Z-score (large display)
- Signal type and reason
- Statistics (mean, std dev)
- Hurst value and regime

**Active Positions Section:**
- Per position: Symbol, Direction, Entry Date, P&L, Return %, Z-score
- **EXTENDED: Show broker for each leg**

**Trade History Section:**
- Table with all trade details
- Download CSV
- Summary statistics

**Charts:**
- Z-score history with threshold lines
- Price chart (spot vs futures)

### 2. Settings Page (`/settings`)

**Signal Parameters Card:**
- Lookback Period (with unit selector)
- Entry Threshold
- Exit Threshold (supports negative values)
- Stop Loss Threshold
- Time-Based Stop Loss

**Overnight Swap Protection Card:**
- Enable checkbox
- Close Hour/Minute

**Regime Filter Card:**
- Enable Hurst Filter checkbox
- Hurst Threshold slider
- Trending Duration

**Position Sizing Card:**
- Max Positions
- Lot Size
- Commission per Lot
- Min Profit per Lot
- Max Loss per Lot

**Order Execution Settings:**
- Order Type dropdown (MARKET/LIMIT)
- Limit Order Timeout
- Peg Interval

**Tools:**
- Max Loss Calculator
- Cost Estimator
- Order Connectivity Test
- Limit Order Test

### 3. Setup Page (`/setup`)

**Asset Configuration:**
- Asset Name
- Spot Symbol
- Futures Symbol
- Futures Expiry Date
- Contract Size
- Daily Swap Charge

**NEW: Broker Configuration:**
- Spot Broker selection/config
- Futures Broker selection/config
- Connection test per broker

### 4. SD Touch Analysis Page (`/sd_analysis`)

**Tab 1: Summary by SD Level**
- Statistics per SD level (2σ, 2.5σ, 3σ, 3.5σ, 4σ)
- Success rates and profitability

**Tab 2: Daily Breakdown**
- Date-by-date analysis

**Tab 3: Recent Touches**
- Last 100 touches with details

**Tab 4: Limit Orders**
- Order statistics and fill rates
- **EXTENDED: Show per-broker breakdown**

---

## API Endpoints (ALL from existing system)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/` | GET | Main monitoring dashboard |
| `/setup` | GET, POST | Asset and broker configuration |
| `/settings` | GET, POST | Trading settings management |
| `/api/data` | GET | Real-time market data and config |
| `/api/toggle_algo` | POST | Enable/disable algorithmic trading |
| `/api/toggle_paper` | POST | Switch paper/live mode |
| `/api/reset_statistics` | POST | Clear price history |
| `/api/trades` | GET | Get trade history |
| `/api/trades/csv` | GET | Download trades as CSV |
| `/api/clear_trades` | POST | Clear all trade records |
| `/api/close_position` | POST | Manually close position |
| `/api/search_symbols` | GET | Search MT5 symbols |
| `/api/test_orders` | POST | Test order connectivity |
| `/api/test_limit_orders` | POST | Test limit order execution |
| `/api/estimate_costs` | GET | Calculate round-trip costs |
| `/api/calculate_max_loss` | GET | Analyze max loss settings |
| `/api/sd_touches` | GET | Get SD touch statistics |
| `/api/sd_touches/pause` | POST | Pause/resume SD tracking |
| `/api/sd_touches/reset` | POST | Reset SD touch data |
| `/api/sd_touches/delete` | POST | Delete specific touches |
| `/api/limit_orders` | GET | Get limit order statistics |
| `/sd_analysis` | GET | SD analysis dashboard |

**NEW Multi-Broker Endpoints:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/brokers` | GET | List all configured brokers |
| `/api/brokers/<id>/status` | GET | Get broker connection status |
| `/api/brokers/<id>/connect` | POST | Connect to specific broker |
| `/api/brokers/<id>/disconnect` | POST | Disconnect from broker |
| `/api/brokers/test_all` | POST | Test all broker connections |

---

## Multi-Broker Architecture

### System Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                    Trading Engine (Main Process)                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Signal    │  │  Position   │  │    Risk Manager         │  │
│  │  Generator  │  │   Manager   │  │  (Cross-Broker)         │  │
│  │             │  │             │  │  - Per-broker limits    │  │
│  │  (Identical │  │  (Extended  │  │  - Aggregate exposure   │  │
│  │   logic)    │  │   for multi │  │  - Hedge ratio check    │  │
│  │             │  │   broker)   │  │                         │  │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘  │
│         │                │                     │                 │
│  ┌──────▼─────────────────▼─────────────────────▼──────────────┐ │
│  │              Order Orchestrator (NEW)                        │ │
│  │  - Parallel execution across brokers                         │ │
│  │  - Partial fill handling                                     │ │
│  │  - Rollback on failure                                       │ │
│  │  - Latency monitoring                                        │ │
│  └──────┬───────────────────────────────────┬──────────────────┘ │
└─────────┼───────────────────────────────────┼───────────────────┘
          │                                   │
          │  IPC (Redis/ZeroMQ)               │  IPC (Redis/ZeroMQ)
          │                                   │
┌─────────▼─────────┐               ┌─────────▼─────────┐
│  MT5 Worker       │               │  MT5 Worker       │
│  Process A        │               │  Process B        │
│  (Spot Broker)    │               │  (Futures Broker) │
│                   │               │                   │
│  mt5.initialize() │               │  mt5.initialize() │
│  Order execution  │               │  Order execution  │
│  Position sync    │               │  Position sync    │
└───────────────────┘               └───────────────────┘
          │                                   │
          ▼                                   ▼
    ┌──────────┐                        ┌──────────┐
    │ Broker A │                        │ Broker B │
    │  (Spot)  │                        │(Futures) │
    └──────────┘                        └──────────┘
```

### Why Separate Processes?

**MT5 Limitation:** Only ONE `mt5.initialize()` per Python process.

**Solution:** Run separate Python processes per MT5 terminal, communicate via:
- Redis pub/sub (recommended)
- ZeroMQ
- REST API between processes

### Broker Configuration Format
```yaml
brokers:
  spot_broker:
    broker_id: "broker_a_spot"
    name: "Broker A - Spot"
    role: "SPOT"
    mt5_path: "C:/Program Files/MT5_BrokerA/terminal64.exe"
    account: 12345678
    server: "BrokerA-Live"
    instruments:
      - symbol: "XAUUSD"
        type: "spot"
        contract_size: 100
        min_volume: 0.01
        commission_per_lot: 7.00

  futures_broker:
    broker_id: "broker_b_futures"
    name: "Broker B - Futures"
    role: "FUTURES"
    mt5_path: "C:/Program Files/MT5_BrokerB/terminal64.exe"
    account: 87654321
    server: "BrokerB-Live"
    instruments:
      - symbol: "GC0226"
        type: "futures"
        contract_size: 100
        min_volume: 0.1
        commission_per_lot: 2.50
```

---

## Synchronized Order Execution (NEW)

### Entry Flow
```python
async def open_spread_position(signal, lot_size):
    # 1. Pre-flight checks on BOTH brokers
    spot_ready = await spot_broker.check_ready(symbol, lot_size)
    futures_ready = await futures_broker.check_ready(symbol, lot_size)

    if not (spot_ready and futures_ready):
        return {'success': False, 'error': 'Pre-flight failed'}

    # 2. Calculate hedge-equivalent volumes
    volumes = calculate_hedge_volumes(lot_size)

    # 3. Execute BOTH legs simultaneously (parallel)
    spot_task = spot_broker.place_order(spot_order)
    futures_task = futures_broker.place_order(futures_order)

    spot_result, futures_result = await asyncio.gather(
        spot_task, futures_task, return_exceptions=True
    )

    # 4. Handle partial fills
    if spot_result.success and not futures_result.success:
        # Rollback: Close spot leg immediately
        await spot_broker.close_position(spot_result.ticket)
        return {'success': False, 'error': 'Futures leg failed, rolled back spot'}

    if futures_result.success and not spot_result.success:
        # Rollback: Close futures leg immediately
        await futures_broker.close_position(futures_result.ticket)
        return {'success': False, 'error': 'Spot leg failed, rolled back futures'}

    # 5. Record combined position
    save_cross_broker_position(spot_result, futures_result)
    return {'success': True}
```

### Exit Flow
```python
async def close_spread_position(position):
    # Send close orders to BOTH brokers simultaneously
    spot_close = spot_broker.close_position(position.spot_ticket)
    futures_close = futures_broker.close_position(position.futures_ticket)

    results = await asyncio.gather(spot_close, futures_close)

    # Calculate combined P&L across brokers
    combined_pnl = calculate_cross_broker_pnl(results)

    return combined_pnl
```

---

## Risk Management (Extended for Multi-Broker)

### Per-Broker Limits
```yaml
risk_management:
  per_broker:
    max_position_size: 1.0  # lots per broker
    margin_buffer_percent: 20
    max_latency_ms: 500

  aggregate:
    max_total_exposure: 5.0  # lots across all brokers
    max_hedge_imbalance: 0.05  # 5% volume difference
```

### Kill Switch
```yaml
kill_switch:
  enabled: true
  triggers:
    - max_drawdown_usd: 1000
    - max_drawdown_percent: 5
    - connection_loss_seconds: 30
    - hedge_imbalance_percent: 10
  action: close_all_positions_all_brokers()
```

### Hedge Ratio Monitoring
- Alert if spot volume ≠ futures volume (beyond tolerance)
- Auto-rebalance option
- Emergency close if severely imbalanced

---

## Broker Adapter Interface

For future extensibility to FIX protocol, Interactive Brokers, etc:

```python
from abc import ABC, abstractmethod
from typing import List, Optional
from dataclasses import dataclass

@dataclass
class Tick:
    symbol: str
    bid: float
    ask: float
    timestamp: float

@dataclass
class OrderResult:
    success: bool
    ticket: Optional[int] = None
    price: Optional[float] = None
    volume: Optional[float] = None
    error: Optional[str] = None

@dataclass
class Position:
    ticket: int
    symbol: str
    volume: float
    entry_price: float
    profit: float
    order_type: str

@dataclass
class AccountInfo:
    balance: float
    equity: float
    margin: float
    free_margin: float


class BrokerAdapter(ABC):
    """Abstract base class for broker connections"""

    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to broker"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection"""
        pass

    @abstractmethod
    async def get_tick(self, symbol: str) -> Tick:
        """Get current bid/ask for symbol"""
        pass

    @abstractmethod
    async def place_order(
        self,
        symbol: str,
        order_type: str,
        volume: float,
        price: Optional[float] = None
    ) -> OrderResult:
        """Place an order (market or limit)"""
        pass

    @abstractmethod
    async def close_position(self, ticket: int) -> OrderResult:
        """Close an existing position"""
        pass

    @abstractmethod
    async def get_positions(self) -> List[Position]:
        """Get all open positions"""
        pass

    @abstractmethod
    async def get_account_info(self) -> AccountInfo:
        """Get account balance, margin, etc."""
        pass

    @abstractmethod
    async def execute_pegged_limit_order(
        self,
        symbol: str,
        order_type: str,
        volume: float,
        timeout: int,
        peg_interval: float,
        ticket: Optional[int] = None  # For closing
    ) -> OrderResult:
        """Execute pegged limit order with price tracking"""
        pass


# Implementations
class MT5Adapter(BrokerAdapter):
    """MetaTrader 5 implementation"""
    pass

class FIXAdapter(BrokerAdapter):
    """FIX protocol implementation (for ECN/FlexTrade)"""
    pass

class IBAdapter(BrokerAdapter):
    """Interactive Brokers implementation"""
    pass
```

---

## Implementation Phases

### Phase 1: Multi-MT5 Foundation
- [ ] Broker configuration system (YAML/database)
- [ ] MT5 worker process per broker
- [ ] IPC layer (Redis pub/sub)
- [ ] Connect to multiple MT5 terminals
- [ ] Route spot orders to Broker A, futures to Broker B
- [ ] Synchronized entry/exit execution
- [ ] Cross-broker position tracking
- [ ] Combined P&L calculation
- [ ] All existing UI pages working with multi-broker backend

### Phase 2: Robustness
- [ ] Connection health monitoring per broker
- [ ] Auto-reconnect with position sync
- [ ] Partial fill handling and rollback
- [ ] Latency monitoring and alerts
- [ ] Kill switch for emergency close-all
- [ ] Hedge ratio monitoring

### Phase 3: Optimization
- [ ] Smart order routing (best execution)
- [ ] Latency-aware execution timing
- [ ] Spread cost optimization per broker
- [ ] Historical slippage analysis per broker

### Phase 4: Platform Migration
- [ ] Abstract broker adapter interface
- [ ] FIX protocol adapter for ECN
- [ ] FlexTrade integration
- [ ] Support mixed platforms (MT5 + FIX)

---

## Key Calculations (Identical to existing system)

### Z-Score Calculation
```python
z_score = (current_spread - mean) / std_dev
```

### P&L Calculation
```python
# Short Spread (Sell Futures, Buy Spot)
spot_pnl = (exit_spot - entry_spot) * lot_size * contract_size
futures_pnl = -(exit_futures - entry_futures) * lot_size * contract_size

# Long Spread (Buy Futures, Sell Spot)
futures_pnl = (exit_futures - entry_futures) * lot_size * contract_size
spot_pnl = -(exit_spot - entry_spot) * lot_size * contract_size

gross_pnl = spot_pnl + futures_pnl
net_pnl = gross_pnl - swap_costs - commission - spread_costs
```

### Hurst Exponent (R/S Analysis)
```python
def calculate_hurst(spread_data, min_points=20):
    # R/S (Rescaled Range) Analysis
    # Returns exponent 0.0-1.0
    # H < 0.4: Mean reverting
    # H >= 0.6: Trending
```

---

## Signal Types (Identical to existing system)

| Signal | Description | Action |
|--------|-------------|--------|
| `SELL_BASIS` | Spread expensive (Z > entry_sd) | Buy spot + Sell futures |
| `BUY_BASIS` | Spread cheap (Z < -entry_sd) | Sell spot + Buy futures |
| `CLOSE` | Normal exit | Close both legs |
| `STOP_LOSS` | Z exceeds stop_loss_sd | Force close |
| `TIME_STOP` | Held too long | Close position |
| `OVERNIGHT_CLOSE` | Before swap time | Close position |
| `MAX_LOSS` | Loss limit exceeded | Force close |
| `REGIME_FILTER` | Trending market | Block entry |
| `HOLD` | No action needed | Wait |

---

## Deliverables

| Component | Description |
|-----------|-------------|
| **Broker Configuration** | YAML/JSON config for multiple brokers |
| **MT5 Worker Processes** | Separate process per MT5 terminal |
| **IPC Layer** | Redis/ZeroMQ communication |
| **Order Orchestrator** | Synchronized cross-broker execution |
| **Position Manager** | Cross-broker position tracking |
| **Risk Manager** | Per-broker + aggregate monitoring |
| **Web Dashboard** | Same UI, extended for multi-broker display |
| **All Existing Features** | SD analysis, Hurst filter, limit orders, etc. |
| **Broker Adapter Interface** | Pluggable for future platforms |

---

## Summary

**What stays the same:**
- ALL trading logic (entry/exit conditions)
- ALL configuration settings
- ALL UI pages and features
- ALL analysis tools (SD touches, Hurst filter)
- ALL order types (market, pegged limit)
- ALL risk management features
- ALL API endpoints

**What changes:**
- Backend connects to multiple MT5 terminals (separate processes)
- Orders execute on different brokers for spot vs futures
- Position tracking includes broker references
- P&L aggregated across broker accounts
- Additional broker health monitoring
- IPC layer for cross-process communication
