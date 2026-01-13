# StatArb Pro

## Multi-Broker Statistical Arbitrage System

Professional trading system for basis arbitrage between spot and futures markets, supporting multiple broker backends (MT5, FIX Protocol, FlexTrade, Interactive Brokers).

![Dashboard Preview](docs/dashboard-preview.png)

---

## Installation (For Users)

### Option 1: Windows Installer (Recommended)

1. Download `StatArbPro_Setup_1.0.0.exe` from [Releases](../../releases)
2. Double-click to install
3. Launch from desktop shortcut
4. Follow the setup wizard to connect your brokers

**No Python or technical knowledge required!**

### Option 2: Run from Source

```bash
# Clone repository
git clone https://github.com/ajoxf/FIX_Protocol_Stat_Arb1.git
cd FIX_Protocol_Stat_Arb1

# Install dependencies
pip install -r requirements.txt

# Initialize database
python main.py init --with-defaults

# Start application
python main.py web
```

Then open http://localhost:5000 in your browser.

---

## Quick Start Guide

### Step 1: Add Your Brokers

Go to **Setup** page and configure:

| Broker | Role | Example |
|--------|------|---------|
| Spot Broker | Trade spot gold (XAUUSD) | IC Markets MT5 |
| Futures Broker | Trade gold futures (GC) | AMP Futures MT5 |

### Step 2: Configure Settings

Go to **Settings** page:

- **Entry SD**: 2.0σ (enter when spread deviates 2 standard deviations)
- **Exit SD**: 0.5σ (exit near mean)
- **Lot Size**: Start with 0.01 for testing

### Step 3: Start Trading

On **Dashboard**:
1. Click **Connect** to connect both brokers
2. Toggle **Algorithm** to enable
3. Monitor Z-score and trades in real-time

---

## Features

### Trading Logic
| Feature | Description |
|---------|-------------|
| Z-Score Signals | Entry at ±2σ, exit at mean |
| Hurst Filter | Block trades in trending markets (H > 0.5) |
| Pegged Limit Orders | Better fills by tracking bid/ask |
| Stop Loss | Z-score, time, and P&L based stops |
| Overnight Protection | Auto-close before market close |

### Supported Brokers

| Backend | Status | Use Case |
|---------|--------|----------|
| MetaTrader 5 | ✅ Full | Retail brokers |
| FIX Protocol | ✅ Full | ECN/Prime brokers |
| FlexTrade | 🔄 Planned | Institutional |
| Interactive Brokers | 🔄 Planned | Multi-asset |

### Analysis Tools
- SD Touch tracking (2σ - 4σ success rates)
- Limit order fill statistics
- Cost calculator with breakeven analysis

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Web UI (Flask)                     │
│    Dashboard │ Settings │ Setup │ SD Analysis       │
└─────────────────────────┬───────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────┐
│                  Trading Engine                      │
│   SignalGenerator │ OrderOrchestrator │ RiskMgr     │
└─────────────────────────┬───────────────────────────┘
                          │
              ┌───────────┴───────────┐
              │     IPC Manager       │
              │   (Redis / Memory)    │
              └───────────┬───────────┘
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│   Worker    │   │   Worker    │   │   Worker    │
│  (MT5 Spot) │   │(MT5 Futures)│   │   (FIX)     │
└─────────────┘   └─────────────┘   └─────────────┘
```

---

## Building the Installer

To create the Windows installer yourself:

```bash
cd installer
build.bat
```

Output: `installer/output/StatArbPro_Setup_1.0.0.exe`

See [installer/BUILD_INSTRUCTIONS.md](installer/BUILD_INSTRUCTIONS.md) for details.

---

## Documentation

- [System Specification](MULTI_BROKER_ARBITRAGE_SPEC.md) - Full technical spec
- [Build Prompt](BUILD_PROMPT.md) - AI development guide
- [Build Instructions](installer/BUILD_INSTRUCTIONS.md) - Create installer

---

## Project Structure

```
FIX_Protocol_Stat_Arb1/
├── adapters/           # Broker connectivity (MT5, FIX)
├── core/               # Trading logic (signals, orchestrator)
├── database/           # SQLite persistence
├── web/                # Flask UI
├── workers/            # Multi-process broker workers
├── installer/          # Windows installer build
├── config/             # YAML configuration
├── main.py             # Entry point
└── requirements.txt    # Python dependencies
```

---

## License

Private - All rights reserved

---

## Support

For issues and questions:
- GitHub Issues: [Report a bug](../../issues)
- Documentation: [Wiki](../../wiki)
