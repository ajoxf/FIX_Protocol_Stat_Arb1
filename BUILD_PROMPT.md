# Project: Multi-Broker Statistical Arbitrage Trading System

## Overview
Build a basis arbitrage trading system identical to the existing single-broker Statistical_Arbitrage_trade system, but with a **pluggable multi-broker backend** that supports different connectivity types per broker.

## Reference Implementation
Use the existing codebase at: https://github.com/ajoxf/Statistical_Arbitrage_trade
- Copy ALL features, UI, settings, and trading logic exactly
- The ONLY change is the backend broker connectivity layer

---

## Core Requirement: Broker Backend Selection

### Setup Page - Broker Configuration
Add a **Backend Type** dropdown for each broker (Spot and Futures):

```
┌─────────────────────────────────────────────────────────┐
│  SPOT BROKER CONFIGURATION                              │
├─────────────────────────────────────────────────────────┤
│  Backend Type:  [  MT5  ▼  ]                            │
│                  ├── MT5 (MetaTrader 5)                 │
│                  ├── FIX Protocol (ECN/Prime Broker)    │
│                  ├── FlexTrade                          │
│                  └── Interactive Brokers                │
│                                                         │
│  ── MT5 Settings (shown when MT5 selected) ──          │
│  MT5 Path:      [C:/Program Files/MT5/terminal64.exe]  │
│  Account:       [12345678                           ]  │
│  Server:        [BrokerA-Live                       ]  │
│  Symbol:        [XAUUSD                             ]  │
│                                                         │
│  ── FIX Settings (shown when FIX selected) ──          │
│  Host:          [fix.primebroker.com                ]  │
│  Port:          [9823                               ]  │
│  Sender Comp:   [CLIENT123                          ]  │
│  Target Comp:   [BROKER                             ]  │
│  Username:      [trader1                            ]  │
│  Password:      [••••••••                           ]  │
│  Symbol:        [XAU/USD                            ]  │
│                                                         │
│  [Test Connection]                                      │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  FUTURES BROKER CONFIGURATION                           │
├─────────────────────────────────────────────────────────┤
│  Backend Type:  [  FIX Protocol  ▼  ]                   │
│  ... (same fields based on selection)                   │
└─────────────────────────────────────────────────────────┘
```

### Supported Backend Types

| Backend | Use Case | Implementation Priority |
|---------|----------|------------------------|
| **MT5** | Retail brokers | Phase 1 (existing code) |
| **FIX Protocol** | ECN, Prime brokers | Phase 2 |
| **FlexTrade** | Institutional | Phase 3 |
| **Interactive Brokers** | Multi-asset | Phase 4 |

---

## Architecture

### Broker Adapter Pattern
```python
# Abstract interface - all backends implement this
class BrokerAdapter(ABC):
    @abstractmethod
    async def connect(self) -> bool: pass

    @abstractmethod
    async def disconnect(self) -> None: pass

    @abstractmethod
    async def get_tick(self, symbol: str) -> Tick: pass

    @abstractmethod
    async def place_market_order(self, symbol, side, volume) -> OrderResult: pass

    @abstractmethod
    async def place_limit_order(self, symbol, side, volume, price) -> OrderResult: pass

    @abstractmethod
    async def execute_pegged_limit_order(
        self, symbol, side, volume, timeout, peg_interval, ticket=None
    ) -> OrderResult: pass

    @abstractmethod
    async def close_position(self, ticket) -> OrderResult: pass

    @abstractmethod
    async def get_positions(self) -> List[Position]: pass

    @abstractmethod
    async def get_account_info(self) -> AccountInfo: pass


# Implementations
class MT5Adapter(BrokerAdapter): ...      # Wrap existing MT5 code
class FIXAdapter(BrokerAdapter): ...      # QuickFIX/simplefix
class FlexTradeAdapter(BrokerAdapter): ...
class IBAdapter(BrokerAdapter): ...       # ib_insync library
```

### Multi-Process Architecture (Required for MT5)
```
┌──────────────────────────────────────────────────────────┐
│              Main Trading Process (Flask App)             │
│  - Web UI, Signal Generation, Position Manager           │
│  - Order Orchestrator (coordinates both brokers)         │
└────────────────────────┬─────────────────────────────────┘
                         │ IPC (Redis pub/sub or ZeroMQ)
         ┌───────────────┴───────────────┐
         │                               │
┌────────▼────────┐            ┌─────────▼────────┐
│  Broker Worker  │            │  Broker Worker   │
│  (Spot)         │            │  (Futures)       │
│                 │            │                  │
│  Adapter: MT5   │            │  Adapter: FIX    │
│  or FIX or IB   │            │  or MT5 or IB    │
└────────┬────────┘            └─────────┬────────┘
         │                               │
         ▼                               ▼
    [Spot Broker]                  [Futures Broker]
```

---

## Database Schema Updates

### brokers table
```sql
CREATE TABLE brokers (
    broker_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    role TEXT NOT NULL,  -- 'SPOT' or 'FUTURES'

    -- Backend type
    backend_type TEXT NOT NULL,  -- 'MT5', 'FIX', 'FLEXTRADE', 'IB'

    -- MT5 specific
    mt5_path TEXT,
    mt5_account INTEGER,
    mt5_server TEXT,

    -- FIX specific
    fix_host TEXT,
    fix_port INTEGER,
    fix_sender_comp TEXT,
    fix_target_comp TEXT,
    fix_username TEXT,
    fix_password TEXT,  -- encrypted

    -- FlexTrade specific
    flex_host TEXT,
    flex_port INTEGER,
    flex_api_key TEXT,

    -- IB specific
    ib_host TEXT,
    ib_port INTEGER,
    ib_client_id INTEGER,

    -- Common
    symbol TEXT NOT NULL,
    contract_size REAL DEFAULT 100,
    commission_per_lot REAL DEFAULT 0,
    min_volume REAL DEFAULT 0.01,

    -- Status
    status TEXT DEFAULT 'DISCONNECTED',
    last_heartbeat TEXT,
    latency_ms INTEGER
);
```

---

## Implementation Phases

### Phase 1: MT5 Multi-Broker (Week 1-2)
- [ ] Create BrokerAdapter abstract interface
- [ ] Wrap existing MT5 code into MT5Adapter class
- [ ] Implement broker worker processes (IPC with Redis)
- [ ] Setup page: MT5 backend configuration for spot + futures
- [ ] Order orchestrator: parallel execution across brokers
- [ ] Test with 2 different MT5 brokers

### Phase 2: FIX Protocol (Week 3-4)
- [ ] Implement FIXAdapter using QuickFIX or simplefix library
- [ ] FIX session management (logon, heartbeat, logout)
- [ ] Market data subscription (35=V)
- [ ] Order entry (35=D), cancel (35=F), status (35=H)
- [ ] Execution reports (35=8) handling
- [ ] Setup page: FIX configuration fields
- [ ] Test with FIX simulator, then live ECN

### Phase 3: Mixed Mode Testing (Week 5)
- [ ] Test MT5 spot + FIX futures combination
- [ ] Test FIX spot + MT5 futures combination
- [ ] Latency comparison and optimization
- [ ] Rollback handling across different backends

### Phase 4: Additional Backends (Future)
- [ ] FlexTrade adapter
- [ ] Interactive Brokers adapter (ib_insync)
- [ ] Generic REST API adapter

---

## Configuration File Format

```yaml
# config/brokers.yaml
spot_broker:
  name: "Spot Broker"
  role: "SPOT"
  backend_type: "MT5"  # or "FIX", "IB", "FLEXTRADE"

  # MT5 config (used if backend_type == "MT5")
  mt5:
    path: "C:/Program Files/MT5_BrokerA/terminal64.exe"
    account: 12345678
    server: "BrokerA-Live"

  # FIX config (used if backend_type == "FIX")
  fix:
    host: "fix.ecnbroker.com"
    port: 9823
    sender_comp: "CLIENT123"
    target_comp: "ECNBROKER"
    username: "trader1"
    password: "encrypted_password"

  symbol: "XAUUSD"
  contract_size: 100
  commission_per_lot: 7.00

futures_broker:
  name: "Futures Broker"
  role: "FUTURES"
  backend_type: "FIX"

  fix:
    host: "fix.cmefutures.com"
    port: 9824
    sender_comp: "MYCLIENT"
    target_comp: "CME"
    username: "futurestrader"
    password: "encrypted_password"

  symbol: "GCG25"
  contract_size: 100
  commission_per_lot: 2.50
```

---

## Key Features to Preserve (from existing system)

### ALL Settings
- Lookback period (minutes/days)
- Entry/Exit/Stop thresholds (σ)
- Hurst filter (enabled, threshold, duration)
- Overnight protection
- Order type (MARKET/LIMIT)
- Limit order timeout & peg interval
- Lot size, commission, profit/loss targets

### ALL Trading Logic
- Z-score signal generation
- Entry: SELL_BASIS / BUY_BASIS
- Exit: Statistical, Stop Loss, Time Stop, Overnight, Max Loss
- Hurst regime filter
- Pegged limit orders

### ALL UI Pages
- Dashboard (real-time monitoring)
- Settings page (all parameters)
- Setup page (extended with backend selection)
- SD Analysis page (all 4 tabs)

### ALL Analysis Tools
- SD Touch tracking (2σ - 4σ)
- Limit order statistics
- Cost estimator
- Max loss calculator

---

## Files to Create

```
multi_broker_arbitrage/
├── main.py                    # Flask app entry point
├── config/
│   └── brokers.yaml          # Broker configuration
├── adapters/
│   ├── __init__.py
│   ├── base.py               # BrokerAdapter ABC
│   ├── mt5_adapter.py        # MT5 implementation
│   ├── fix_adapter.py        # FIX implementation
│   ├── ib_adapter.py         # IB implementation
│   └── flextrade_adapter.py  # FlexTrade implementation
├── workers/
│   ├── __init__.py
│   ├── broker_worker.py      # Subprocess for each broker
│   └── ipc.py                # Redis/ZeroMQ communication
├── core/
│   ├── __init__.py
│   ├── signal_generator.py   # Z-score, Hurst calculations
│   ├── position_manager.py   # Cross-broker positions
│   ├── order_orchestrator.py # Parallel execution
│   └── risk_manager.py       # Per-broker + aggregate
├── database/
│   ├── __init__.py
│   ├── models.py             # SQLite tables
│   └── manager.py            # Database operations
├── web/
│   ├── __init__.py
│   ├── routes.py             # Flask routes
│   └── templates/            # HTML templates (embedded or separate)
└── tests/
    ├── test_mt5_adapter.py
    ├── test_fix_adapter.py
    └── test_orchestrator.py
```

---

## Start Command

Begin with Phase 1:
1. Create the BrokerAdapter interface
2. Wrap existing MT5 code into MT5Adapter
3. Update Setup page with backend type dropdown
4. Implement broker worker process with Redis IPC
5. Test with single MT5 broker first, then add second

---

## Reference Documents

- [MULTI_BROKER_ARBITRAGE_SPEC.md](MULTI_BROKER_ARBITRAGE_SPEC.md) - Full system specification with all features
- [Statistical_Arbitrage_trade](https://github.com/ajoxf/Statistical_Arbitrage_trade) - Reference implementation (single-broker)
