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


@app.route('/api/engine/start', methods=['POST'])
def api_engine_start():
    """Start trading engine"""
    global engine, engine_loop

    if engine and engine.state == EngineState.RUNNING:
        return jsonify({'success': False, 'error': 'Engine already running'})

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

            engine_loop.run_until_complete(engine.initialize())
            engine_loop.run_until_complete(engine.start())
            engine_loop.run_forever()

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
