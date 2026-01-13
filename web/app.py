"""
Flask Web Application

Provides the web UI for the Multi-Broker Arbitrage System.

Pages:
- Dashboard: Real-time monitoring
- Settings: Trading parameters configuration
- Setup: Broker configuration
- SD Analysis: Standard deviation touch analysis
"""

import asyncio
import threading
from datetime import datetime
from functools import wraps
from typing import Optional
import logging

from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_socketio import SocketIO, emit

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.manager import DatabaseManager
from database.models import TradingConfig, Broker, Trade
from core.trading_engine import TradingEngine, EngineState

logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'multi-broker-arb-secret-key'

# SocketIO for real-time updates
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global instances
db: Optional[DatabaseManager] = None
engine: Optional[TradingEngine] = None
engine_loop: Optional[asyncio.AbstractEventLoop] = None


def init_app(db_path: str = "trading.db"):
    """Initialize application components"""
    global db, engine

    db = DatabaseManager(db_path)
    db.initialize()

    # Engine will be initialized on first start
    engine = None


def get_db() -> DatabaseManager:
    """Get database manager"""
    global db
    if db is None:
        db = DatabaseManager("trading.db")
        db.initialize()
    return db


# ==================== Routes ====================

@app.route('/')
def index():
    """Redirect to dashboard"""
    return redirect(url_for('dashboard'))


@app.route('/dashboard')
def dashboard():
    """Dashboard page - real-time monitoring"""
    database = get_db()
    config = database.get_config()
    brokers = database.get_brokers()
    open_trades = database.get_open_trades()
    recent_trades = database.get_trades(limit=10)
    stats = database.get_trade_statistics()

    return render_template(
        'dashboard.html',
        config=config,
        brokers=brokers,
        open_trades=open_trades,
        recent_trades=recent_trades,
        stats=stats,
        engine_state=engine.state.value if engine else "STOPPED"
    )


@app.route('/settings')
def settings():
    """Settings page - trading parameters"""
    database = get_db()
    config = database.get_config()

    return render_template('settings.html', config=config)


@app.route('/setup')
def setup():
    """Setup page - broker configuration"""
    database = get_db()
    config = database.get_config()
    brokers = database.get_brokers()

    return render_template('setup.html', config=config, brokers=brokers)


@app.route('/analysis')
def analysis():
    """SD Analysis page"""
    database = get_db()
    config = database.get_config()
    sd_stats = database.get_sd_touch_stats()
    sd_touches = database.get_sd_touches(limit=100)
    limit_stats = database.get_limit_order_stats()

    return render_template(
        'analysis.html',
        config=config,
        sd_stats=sd_stats,
        sd_touches=sd_touches,
        limit_stats=limit_stats
    )


# ==================== API Routes ====================

@app.route('/api/status')
def api_status():
    """Get current system status"""
    if engine:
        return jsonify(engine.get_status())
    else:
        database = get_db()
        config = database.get_config()
        brokers = database.get_brokers()

        return jsonify({
            'state': 'STOPPED',
            'config': config.to_dict(),
            'brokers': {b.broker_id: {'status': b.status, 'role': b.role} for b in brokers},
            'market': {},
            'position': {'has_position': False}
        })


@app.route('/api/config', methods=['GET', 'POST'])
def api_config():
    """Get or update trading configuration"""
    database = get_db()

    if request.method == 'POST':
        data = request.get_json()

        config = database.get_config()

        # Update fields from request
        for key, value in data.items():
            if hasattr(config, key):
                # Type conversion
                field_type = type(getattr(config, key))
                if field_type == bool:
                    value = bool(value)
                elif field_type == int:
                    value = int(value)
                elif field_type == float:
                    value = float(value)
                setattr(config, key, value)

        database.update_config(config)

        # Reload config in engine if running
        if engine:
            engine.reload_config()

        return jsonify({'success': True, 'config': config.to_dict()})

    else:
        config = database.get_config()
        return jsonify(config.to_dict())


@app.route('/api/brokers', methods=['GET', 'POST'])
def api_brokers():
    """Get or add broker configuration"""
    database = get_db()

    if request.method == 'POST':
        data = request.get_json()

        broker = Broker(
            broker_id=data.get('broker_id'),
            name=data.get('name'),
            broker_type=data.get('broker_type', 'MT5'),
            role=data.get('role'),
            mt5_path=data.get('mt5_path'),
            mt5_account=data.get('mt5_account'),
            mt5_server=data.get('mt5_server'),
            mt5_password=data.get('mt5_password'),
            fix_host=data.get('fix_host'),
            fix_port=data.get('fix_port'),
            fix_sender_comp=data.get('fix_sender_comp'),
            fix_target_comp=data.get('fix_target_comp'),
            fix_username=data.get('fix_username'),
            fix_password=data.get('fix_password'),
            symbol=data.get('symbol', ''),
            contract_size=data.get('contract_size', 100.0),
            commission_per_lot=data.get('commission_per_lot', 0.0)
        )

        database.add_broker(broker)
        return jsonify({'success': True, 'broker': broker.to_dict()})

    else:
        brokers = database.get_brokers()
        return jsonify([b.to_dict() for b in brokers])


@app.route('/api/brokers/<broker_id>', methods=['GET', 'PUT', 'DELETE'])
def api_broker(broker_id):
    """Get, update, or delete a specific broker"""
    database = get_db()

    if request.method == 'DELETE':
        database.delete_broker(broker_id)
        return jsonify({'success': True})

    elif request.method == 'PUT':
        data = request.get_json()
        broker = database.get_broker(broker_id)
        if broker:
            for key, value in data.items():
                if hasattr(broker, key):
                    setattr(broker, key, value)
            database.add_broker(broker)
            return jsonify({'success': True, 'broker': broker.to_dict()})
        return jsonify({'success': False, 'error': 'Broker not found'}), 404

    else:
        broker = database.get_broker(broker_id)
        if broker:
            return jsonify(broker.to_dict())
        return jsonify({'error': 'Broker not found'}), 404


@app.route('/api/brokers/<broker_id>/test-order', methods=['POST'])
def api_broker_test_order(broker_id):
    """Place a test order on a broker"""
    import time

    database = get_db()
    broker = database.get_broker(broker_id)

    if not broker:
        return jsonify({'success': False, 'error': 'Broker not found'})

    data = request.get_json()
    order_type = data.get('order_type', 'MARKET')  # MARKET or LIMIT
    side = data.get('side', 'BUY')  # BUY or SELL
    volume = float(data.get('volume', 0.01))
    price = data.get('price')  # For LIMIT orders

    if broker.broker_type == 'MT5':
        try:
            import MetaTrader5 as mt5

            # Initialize and login
            if broker.mt5_path:
                if not mt5.initialize(broker.mt5_path):
                    return jsonify({'success': False, 'error': f'MT5 init failed: {mt5.last_error()}'})
            else:
                if not mt5.initialize():
                    return jsonify({'success': False, 'error': f'MT5 init failed: {mt5.last_error()}'})

            if broker.mt5_account and broker.mt5_password and broker.mt5_server:
                if not mt5.login(int(broker.mt5_account), password=broker.mt5_password, server=broker.mt5_server):
                    mt5.shutdown()
                    return jsonify({'success': False, 'error': f'Login failed: {mt5.last_error()}'})

            # Get symbol info
            symbol = broker.symbol
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                mt5.shutdown()
                return jsonify({'success': False, 'error': f'Symbol {symbol} not found'})

            if not symbol_info.visible:
                if not mt5.symbol_select(symbol, True):
                    mt5.shutdown()
                    return jsonify({'success': False, 'error': f'Failed to select symbol {symbol}'})

            # Get current price
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                mt5.shutdown()
                return jsonify({'success': False, 'error': 'Failed to get tick data'})

            # Prepare order request
            if order_type == 'MARKET':
                order_type_mt5 = mt5.ORDER_TYPE_BUY if side == 'BUY' else mt5.ORDER_TYPE_SELL
                order_price = tick.ask if side == 'BUY' else tick.bid
                request_dict = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "symbol": symbol,
                    "volume": volume,
                    "type": order_type_mt5,
                    "price": order_price,
                    "deviation": 20,
                    "magic": 123456,
                    "comment": "StatArb Test Order",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                }
            else:  # LIMIT
                order_type_mt5 = mt5.ORDER_TYPE_BUY_LIMIT if side == 'BUY' else mt5.ORDER_TYPE_SELL_LIMIT
                if price is None:
                    # Default: place limit order 50 points away from market
                    point = symbol_info.point
                    price = tick.ask - (50 * point) if side == 'BUY' else tick.bid + (50 * point)
                request_dict = {
                    "action": mt5.TRADE_ACTION_PENDING,
                    "symbol": symbol,
                    "volume": volume,
                    "type": order_type_mt5,
                    "price": float(price),
                    "deviation": 20,
                    "magic": 123456,
                    "comment": "StatArb Test Limit",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_RETURN,
                }

            # Send order
            start_time = time.time()
            result = mt5.order_send(request_dict)
            execution_time = int((time.time() - start_time) * 1000)

            mt5.shutdown()

            if result is None:
                return jsonify({'success': False, 'error': 'Order send returned None'})

            if result.retcode != mt5.TRADE_RETCODE_DONE:
                return jsonify({
                    'success': False,
                    'error': f'Order failed: {result.comment}',
                    'retcode': result.retcode
                })

            return jsonify({
                'success': True,
                'order_id': result.order,
                'deal_id': result.deal,
                'volume': result.volume,
                'price': result.price,
                'execution_time_ms': execution_time,
                'comment': result.comment
            })

        except ImportError:
            return jsonify({'success': False, 'error': 'MetaTrader5 package not installed'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    else:
        return jsonify({'success': False, 'error': f'Test orders not implemented for {broker.broker_type}'})


@app.route('/api/brokers/<broker_id>/test', methods=['POST'])
def api_broker_test(broker_id):
    """Test broker connection"""
    import time

    database = get_db()
    broker = database.get_broker(broker_id)

    if not broker:
        return jsonify({'success': False, 'error': 'Broker not found'})

    if broker.broker_type == 'MT5':
        try:
            import MetaTrader5 as mt5

            # Initialize MT5
            start_time = time.time()

            if broker.mt5_path:
                init_result = mt5.initialize(broker.mt5_path)
            else:
                init_result = mt5.initialize()

            if not init_result:
                error = mt5.last_error()
                return jsonify({
                    'success': False,
                    'error': f'MT5 initialization failed: {error}'
                })

            # Login to account
            if broker.mt5_account and broker.mt5_password and broker.mt5_server:
                login_result = mt5.login(
                    int(broker.mt5_account),
                    password=broker.mt5_password,
                    server=broker.mt5_server
                )

                if not login_result:
                    error = mt5.last_error()
                    mt5.shutdown()
                    return jsonify({
                        'success': False,
                        'error': f'Login failed: {error}'
                    })

            # Get account info
            account_info = mt5.account_info()
            latency_ms = int((time.time() - start_time) * 1000)

            # Update broker status in database
            broker.status = 'CONNECTED'
            broker.latency_ms = latency_ms
            database.add_broker(broker)

            result = {
                'success': True,
                'latency_ms': latency_ms,
                'account_info': {
                    'login': account_info.login if account_info else None,
                    'balance': account_info.balance if account_info else None,
                    'equity': account_info.equity if account_info else None,
                    'currency': account_info.currency if account_info else None,
                    'server': account_info.server if account_info else None,
                    'company': account_info.company if account_info else None
                } if account_info else None
            }

            mt5.shutdown()
            return jsonify(result)

        except ImportError:
            return jsonify({
                'success': False,
                'error': 'MetaTrader5 package not installed. Run: pip install MetaTrader5'
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            })

    elif broker.broker_type == 'FIX':
        return jsonify({
            'success': False,
            'error': 'FIX connection test not yet implemented'
        })

    else:
        return jsonify({
            'success': False,
            'error': f'Unknown broker type: {broker.broker_type}'
        })


@app.route('/api/trades')
def api_trades():
    """Get trade history"""
    database = get_db()
    status = request.args.get('status')
    limit = int(request.args.get('limit', 100))

    trades = database.get_trades(status=status, limit=limit)
    return jsonify([t.to_dict() for t in trades])


@app.route('/api/trades/stats')
def api_trade_stats():
    """Get trade statistics"""
    database = get_db()
    stats = database.get_trade_statistics()
    return jsonify(stats)


@app.route('/api/sd-touches')
def api_sd_touches():
    """Get SD touch log"""
    database = get_db()
    limit = int(request.args.get('limit', 100))
    sd_level = request.args.get('sd_level')

    touches = database.get_sd_touches(sd_level=sd_level, limit=limit)
    return jsonify([t.to_dict() for t in touches])


@app.route('/api/sd-touches/stats')
def api_sd_stats():
    """Get SD touch statistics"""
    database = get_db()
    stats = database.get_sd_touch_stats()
    return jsonify(stats)


@app.route('/api/limit-orders/stats')
def api_limit_stats():
    """Get limit order statistics"""
    database = get_db()
    stats = database.get_limit_order_stats()
    return jsonify(stats)


@app.route('/api/trades/export')
def api_trades_export():
    """Export trades as CSV"""
    from flask import Response
    import csv
    import io

    database = get_db()
    trades = database.get_trades(limit=10000)

    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow([
        'Trade ID', 'Entry Time', 'Exit Time', 'Direction', 'Status',
        'Spot Broker', 'Spot Entry Price', 'Spot Exit Price', 'Spot Volume',
        'Futures Broker', 'Futures Entry Price', 'Futures Exit Price', 'Futures Volume',
        'Entry Spread', 'Exit Spread', 'PnL', 'Zscore Entry', 'Zscore Exit',
        'Duration (s)', 'Notes'
    ])

    for t in trades:
        writer.writerow([
            t.trade_id, t.entry_time, t.exit_time, t.direction, t.status,
            t.spot_broker_id, t.spot_entry_price, t.spot_exit_price, t.spot_volume,
            t.futures_broker_id, t.futures_entry_price, t.futures_exit_price, t.futures_volume,
            t.entry_spread, t.exit_spread, t.pnl, t.zscore_entry, t.zscore_exit,
            t.duration_seconds, t.notes
        ])

    output.seek(0)

    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=trades_export.csv'}
    )


@app.route('/api/sd-touches/export')
def api_sd_touches_export():
    """Export SD touches as CSV"""
    from flask import Response
    import csv
    import io

    database = get_db()
    touches = database.get_sd_touches(limit=10000)

    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow([
        'ID', 'Timestamp', 'SD Level', 'Touch Price', 'Spread',
        'Zscore', 'Direction', 'Bounce %', 'Time to Bounce (s)',
        'Order Placed', 'Order Filled', 'PnL'
    ])

    for touch in touches:
        writer.writerow([
            touch.id, touch.timestamp, touch.sd_level, touch.touch_price, touch.spread,
            touch.zscore, touch.direction, touch.bounce_percent, touch.time_to_bounce_seconds,
            touch.order_placed, touch.order_filled, touch.pnl
        ])

    output.seek(0)

    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=sd_touches_export.csv'}
    )


@app.route('/api/engine/start', methods=['POST'])
def api_engine_start():
    """Start trading engine"""
    global engine, engine_loop

    # Check if engine is already running or starting
    if engine:
        if engine.state == EngineState.RUNNING:
            return jsonify({'success': False, 'error': 'Engine already running'})
        if engine.state == EngineState.STARTING:
            return jsonify({'success': False, 'error': 'Engine is starting'})

    try:
        # Create event loop in background thread
        def run_engine():
            global engine, engine_loop
            engine_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(engine_loop)

            engine = TradingEngine(db_path="trading.db")

            # Register callbacks for SocketIO updates
            engine.on_tick(lambda m: socketio.emit('tick', {
                'spread': m.spread,
                'zscore': m.zscore,
                'spot_bid': m.spot_bid,
                'spot_ask': m.spot_ask,
                'futures_bid': m.futures_bid,
                'futures_ask': m.futures_ask
            }))

            engine.on_signal(lambda s: socketio.emit('signal', s.to_dict()))

            engine.on_trade(lambda action, t: socketio.emit('trade', {
                'action': action,
                'trade': t.to_dict()
            }))

            async def run_async():
                await engine.initialize()
                await engine.start()
                # Keep running until stopped
                while engine.state == EngineState.RUNNING:
                    await asyncio.sleep(0.1)

            try:
                engine_loop.run_until_complete(run_async())
            except Exception as e:
                logger.error(f"Engine error: {e}")
            finally:
                engine_loop.close()

        thread = threading.Thread(target=run_engine, daemon=True)
        thread.start()

        return jsonify({'success': True, 'message': 'Engine starting'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/engine/stop', methods=['POST'])
def api_engine_stop():
    """Stop trading engine"""
    global engine, engine_loop

    if not engine:
        return jsonify({'success': False, 'error': 'Engine not running'})

    try:
        if engine_loop:
            asyncio.run_coroutine_threadsafe(engine.stop(), engine_loop)
            engine_loop.call_soon_threadsafe(engine_loop.stop)

        return jsonify({'success': True, 'message': 'Engine stopping'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/engine/toggle-algo', methods=['POST'])
def api_toggle_algo():
    """Toggle algorithm on/off"""
    database = get_db()
    data = request.get_json()
    enabled = data.get('enabled', False)

    database.update_config_field('algo_enabled', enabled)

    if engine:
        engine.reload_config()

    return jsonify({'success': True, 'algo_enabled': enabled})


@app.route('/api/clear-data', methods=['POST'])
def api_clear_data():
    """Clear historical data"""
    database = get_db()
    data = request.get_json()
    data_type = data.get('type', 'all')

    if data_type == 'prices' or data_type == 'all':
        database.clear_price_history()

    if data_type == 'trades' or data_type == 'all':
        database.clear_trades()

    if data_type == 'sd_touches' or data_type == 'all':
        database.clear_sd_touches()

    return jsonify({'success': True})


@app.route('/api/detect-mt5', methods=['GET'])
def api_detect_mt5():
    """Auto-detect MT5 installations on the system"""
    import os
    import glob

    found_installations = []

    # Common installation paths to check
    common_paths = [
        # Standard Program Files locations
        r"C:\Program Files\MetaTrader 5\terminal64.exe",
        r"C:\Program Files (x86)\MetaTrader 5\terminal64.exe",
        # Broker-specific installations (common patterns)
        r"C:\Program Files\*MT5*\terminal64.exe",
        r"C:\Program Files\*MetaTrader*\terminal64.exe",
        r"C:\Program Files (x86)\*MT5*\terminal64.exe",
        r"C:\Program Files (x86)\*MetaTrader*\terminal64.exe",
        # User profile locations
        os.path.expandvars(r"%LOCALAPPDATA%\Programs\*MT5*\terminal64.exe"),
        os.path.expandvars(r"%APPDATA%\*MetaTrader*\terminal64.exe"),
        # Portable installations on common drives
        r"D:\*MT5*\terminal64.exe",
        r"D:\*MetaTrader*\terminal64.exe",
    ]

    checked_paths = set()

    for pattern in common_paths:
        try:
            # Use glob for wildcard patterns
            if '*' in pattern:
                matches = glob.glob(pattern)
                for match in matches:
                    if match not in checked_paths and os.path.isfile(match):
                        checked_paths.add(match)
                        # Extract broker name from path
                        folder_name = os.path.basename(os.path.dirname(match))
                        found_installations.append({
                            'path': match,
                            'name': folder_name
                        })
            else:
                # Direct path check
                if pattern not in checked_paths and os.path.isfile(pattern):
                    checked_paths.add(pattern)
                    folder_name = os.path.basename(os.path.dirname(pattern))
                    found_installations.append({
                        'path': pattern,
                        'name': folder_name
                    })
        except Exception:
            pass

    # Also check Windows Registry for MT5 installations
    try:
        import winreg

        # Check uninstall registry keys
        reg_paths = [
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall"),
            (winreg.HKEY_CURRENT_USER, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
        ]

        for hkey, reg_path in reg_paths:
            try:
                with winreg.OpenKey(hkey, reg_path) as key:
                    i = 0
                    while True:
                        try:
                            subkey_name = winreg.EnumKey(key, i)
                            if 'metatrader' in subkey_name.lower() or 'mt5' in subkey_name.lower():
                                with winreg.OpenKey(key, subkey_name) as subkey:
                                    try:
                                        install_path = winreg.QueryValueEx(subkey, "InstallLocation")[0]
                                        terminal_path = os.path.join(install_path, "terminal64.exe")
                                        if terminal_path not in checked_paths and os.path.isfile(terminal_path):
                                            checked_paths.add(terminal_path)
                                            display_name = winreg.QueryValueEx(subkey, "DisplayName")[0]
                                            found_installations.append({
                                                'path': terminal_path,
                                                'name': display_name
                                            })
                                    except (FileNotFoundError, OSError):
                                        pass
                            i += 1
                        except OSError:
                            break
            except (FileNotFoundError, OSError):
                pass
    except ImportError:
        # winreg not available (non-Windows)
        pass

    return jsonify({
        'success': True,
        'installations': found_installations,
        'count': len(found_installations)
    })


@app.route('/api/detect-mt5-accounts', methods=['POST'])
def api_detect_mt5_accounts():
    """Detect saved MT5 accounts for a given terminal path"""
    import os
    import hashlib
    import configparser
    import re

    data = request.get_json()
    mt5_path = data.get('mt5_path', '')

    if not mt5_path or not os.path.isfile(mt5_path):
        return jsonify({'success': False, 'error': 'Invalid MT5 path'})

    found_servers = []
    found_accounts = []

    try:
        # MT5 stores data in %APPDATA%\MetaQuotes\Terminal\<hash>\
        # The hash is MD5 of the uppercase terminal path
        terminal_path = os.path.dirname(mt5_path).upper()
        path_hash = hashlib.md5(terminal_path.encode('utf-16-le')).hexdigest().upper()

        appdata = os.environ.get('APPDATA', '')
        data_dir = os.path.join(appdata, 'MetaQuotes', 'Terminal', path_hash)

        if os.path.isdir(data_dir):
            # Look for server list in config
            config_dir = os.path.join(data_dir, 'config')

            # Check for servers in the origin.txt or common config files
            # Also check the 'servers' subdirectory in the main MT5 folder
            servers_dir = os.path.join(os.path.dirname(mt5_path), 'config', 'servers')
            if os.path.isdir(servers_dir):
                for server_file in os.listdir(servers_dir):
                    if server_file.endswith('.srv'):
                        server_name = server_file[:-4]  # Remove .srv extension
                        found_servers.append(server_name)

            # Also check the history folder for server names
            history_dir = os.path.join(data_dir, 'history')
            if os.path.isdir(history_dir):
                for item in os.listdir(history_dir):
                    item_path = os.path.join(history_dir, item)
                    if os.path.isdir(item_path) and item not in found_servers:
                        # Server folders in history
                        found_servers.append(item)

            # Look for saved accounts in the accounts config
            # Accounts are stored in accounts.dat (binary) but we can check lastlogin info
            profiles_dir = os.path.join(data_dir, 'config')
            if os.path.isdir(profiles_dir):
                # Check for recent login info in ini files
                for f in os.listdir(profiles_dir):
                    if f.endswith('.ini'):
                        try:
                            ini_path = os.path.join(profiles_dir, f)
                            with open(ini_path, 'r', encoding='utf-8', errors='ignore') as fp:
                                content = fp.read()
                                # Look for login patterns
                                login_match = re.search(r'Login=(\d+)', content)
                                server_match = re.search(r'Server=([^\r\n]+)', content)
                                if login_match:
                                    account_info = {
                                        'account': login_match.group(1),
                                        'server': server_match.group(1) if server_match else ''
                                    }
                                    if account_info not in found_accounts:
                                        found_accounts.append(account_info)
                        except Exception:
                            pass

            # Check common.ini for last used account
            common_ini = os.path.join(data_dir, 'config', 'common.ini')
            if os.path.isfile(common_ini):
                try:
                    with open(common_ini, 'r', encoding='utf-8', errors='ignore') as fp:
                        content = fp.read()
                        login_match = re.search(r'Login=(\d+)', content)
                        server_match = re.search(r'Server=([^\r\n]+)', content)
                        if login_match:
                            account_info = {
                                'account': login_match.group(1),
                                'server': server_match.group(1) if server_match else ''
                            }
                            if account_info not in found_accounts:
                                found_accounts.insert(0, account_info)  # Most recent first
                except Exception:
                    pass

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

    return jsonify({
        'success': True,
        'servers': sorted(set(found_servers)),
        'accounts': found_accounts,
        'data_dir': data_dir if 'data_dir' in dir() else None
    })


@app.route('/api/shutdown', methods=['POST'])
def api_shutdown():
    """Shutdown the application gracefully"""
    import os
    import signal

    def shutdown():
        # Give time for response to be sent
        import time
        time.sleep(0.5)
        # Use os._exit to force shutdown (works with PyInstaller)
        os._exit(0)

    # Run shutdown in background thread
    shutdown_thread = threading.Thread(target=shutdown)
    shutdown_thread.start()

    return jsonify({'success': True, 'message': 'Shutting down...'})


# ==================== SocketIO Events ====================

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    logger.info("Client connected")
    if engine:
        emit('status', engine.get_status())


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    logger.info("Client disconnected")


@socketio.on('request_status')
def handle_request_status():
    """Send current status to client"""
    if engine:
        emit('status', engine.get_status())


# ==================== Entry Point ====================

def run_server(host: str = '0.0.0.0', port: int = 5000, debug: bool = False):
    """Run the Flask server"""
    init_app()
    socketio.run(app, host=host, port=port, debug=debug)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run_server(debug=True)
