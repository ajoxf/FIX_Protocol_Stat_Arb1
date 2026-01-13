#!/usr/bin/env python3
"""
Multi-Broker Statistical Arbitrage System

Main entry point for the trading system.

Usage:
    # Start web server
    python main.py web

    # Start trading engine only
    python main.py engine

    # Start broker worker
    python main.py worker --broker-id spot_broker

    # Initialize database
    python main.py init

Commands:
    web     - Start Flask web server (includes trading engine)
    engine  - Start trading engine without web UI
    worker  - Start broker worker process
    init    - Initialize database with default configuration
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def cmd_web(args):
    """Start web server"""
    from web.app import run_server

    logger.info(f"Starting web server on {args.host}:{args.port}")
    run_server(
        host=args.host,
        port=args.port,
        debug=args.debug
    )


def cmd_engine(args):
    """Start trading engine"""
    from core.trading_engine import TradingEngine

    async def run():
        engine = TradingEngine(db_path=args.db)
        if await engine.initialize():
            logger.info("Trading engine initialized")
            await engine.start()
        else:
            logger.error("Failed to initialize trading engine")

    asyncio.run(run())


def cmd_worker(args):
    """Start broker worker"""
    from workers.broker_worker import run_worker

    logger.info(f"Starting broker worker: {args.broker_id}")
    asyncio.run(run_worker(
        broker_id=args.broker_id,
        config_path=args.config,
        db_path=args.db
    ))


def cmd_init(args):
    """Initialize database"""
    from database.manager import DatabaseManager
    from database.models import Broker

    logger.info(f"Initializing database: {args.db}")

    db = DatabaseManager(args.db)
    db.initialize()

    # Add default brokers if requested
    if args.with_defaults:
        logger.info("Adding default broker configurations")

        spot_broker = Broker(
            broker_id="spot_broker",
            name="Spot Broker (MT5)",
            broker_type="MT5",
            role="SPOT",
            symbol="XAUUSD",
            contract_size=100.0,
            commission_per_lot=7.0
        )
        db.add_broker(spot_broker)

        futures_broker = Broker(
            broker_id="futures_broker",
            name="Futures Broker (MT5)",
            broker_type="MT5",
            role="FUTURES",
            symbol="GC0226",
            contract_size=100.0,
            commission_per_lot=4.0
        )
        db.add_broker(futures_broker)

    db.close()
    logger.info("Database initialization complete")


def main():
    parser = argparse.ArgumentParser(
        description='Multi-Broker Statistical Arbitrage System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Web command
    web_parser = subparsers.add_parser('web', help='Start web server')
    web_parser.add_argument('--host', default='0.0.0.0', help='Host to bind (default: 0.0.0.0)')
    web_parser.add_argument('--port', type=int, default=5000, help='Port to bind (default: 5000)')
    web_parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    web_parser.set_defaults(func=cmd_web)

    # Engine command
    engine_parser = subparsers.add_parser('engine', help='Start trading engine')
    engine_parser.add_argument('--db', default='trading.db', help='Database path')
    engine_parser.set_defaults(func=cmd_engine)

    # Worker command
    worker_parser = subparsers.add_parser('worker', help='Start broker worker')
    worker_parser.add_argument('--broker-id', required=True, help='Broker identifier')
    worker_parser.add_argument('--config', default='config/brokers.yaml', help='Config file')
    worker_parser.add_argument('--db', default='trading.db', help='Database path')
    worker_parser.set_defaults(func=cmd_worker)

    # Init command
    init_parser = subparsers.add_parser('init', help='Initialize database')
    init_parser.add_argument('--db', default='trading.db', help='Database path')
    init_parser.add_argument('--with-defaults', action='store_true', help='Add default brokers')
    init_parser.set_defaults(func=cmd_init)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == '__main__':
    main()
