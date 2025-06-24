"""
QuickBooks Sync - Main Orchestration with Iterator Support
"""
import sys
import os
import time
import logging
import datetime
import argparse
from typing import Optional, List, Dict, Any

from config import (
    DATABASE_CONFIG, QB_CONFIG, LOGGING_CONFIG, SYNC_CONFIG,
    TABLE_CONFIGS, SYNC_SCHEDULE_DEFAULTS
)
from database.base import DatabaseInterface
from database.sqlite_db import SQLiteDatabase
from quickbooks.connection import QuickBooksConnection
from sync.record_sync import RecordSyncHandler
from sync.price_analysis import PriceAnalyzer


def setup_logging():
    """Configure logging"""
    # Configure root logger
    logging.basicConfig(
        filename=LOGGING_CONFIG['filename'],
        level=getattr(logging, LOGGING_CONFIG['level']),
        format=LOGGING_CONFIG['format']
    )

    # Add console handler
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, LOGGING_CONFIG['console_level']))
    formatter = logging.Formatter(LOGGING_CONFIG['format'])
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def progress_callback(table_name: str, batch_number: int, total_records: int, remaining_count: Optional[int]):
    """Callback function to display sync progress"""
    if remaining_count is not None and remaining_count > 0:
        total_estimated = total_records + remaining_count
        percentage = (total_records / total_estimated) * 100 if total_estimated > 0 else 0
        print(f"\r{table_name}: Batch {batch_number} - Processing records {total_records:,} of ~{total_estimated:,} ({percentage:.1f}%)", end='', flush=True)
    else:
        print(f"\r{table_name}: Batch {batch_number} - Processed {total_records:,} records", end='', flush=True)


def initialize_database(db_type: str = None) -> DatabaseInterface:
    """
    Initialize database connection

    Args:
        db_type: Override database type from config

    Returns:
        Database interface instance
    """
    if db_type is None:
        db_type = DATABASE_CONFIG['type']

    if db_type == 'sqlite':
        return SQLiteDatabase(DATABASE_CONFIG['sqlite'])
    else:
        # Future: SQL Express implementation
        raise NotImplementedError(f"Database type '{db_type}' not implemented yet")


def verify_database(db: DatabaseInterface) -> None:
    """Verify database and display statistics"""
    logging.info("==== VERIFYING DATABASE ====")

    stats = db.verify_database()

    if not stats['exists']:
        logging.info("Database does not exist yet. Will be created on first sync.")
        return

    logging.info(f"Database: {stats.get('database_path', 'N/A')}")
    logging.info(f"Size: {stats['size_mb']:.2f} MB")
    logging.info(f"Tables: {len(stats['tables'])}")

    # Show table counts
    for table in stats['tables'][:20]:  # First 20 tables
        if table['record_count'] >= 0:
            logging.info(f"  - {table['name']}: {table['record_count']:,} records")
        else:
            logging.info(f"  - {table['name']}: Error reading")

    if len(stats['tables']) > 20:
        logging.info(f"  ... and {len(stats['tables']) - 20} more tables")

    # Show sync log summary
    if stats['sync_log']:
        logging.info("\nRecent sync activity:")
        for log in stats['sync_log'][:10]:
            status = log.get('last_status', 'UNKNOWN')
            if status == 'SUCCESS':
                logging.info(f"  - {log['table_name']}: {log.get('record_count', 0):,} records")
            else:
                logging.info(f"  - {log['table_name']}: {status}")


def sync_tables(qb: QuickBooksConnection, db: DatabaseInterface,
               tables: Optional[List[str]] = None, full_sync: bool = False,
               skip_auto_analysis: bool = False, batch_size: int = None,
               show_progress: bool = True, check_orphaned: bool = True,
               auto_fix_orphaned: bool = False) -> None:
    """
    Sync tables from QuickBooks to database using iterators

    Args:
        qb: QuickBooks connection
        db: Database interface
        tables: Specific tables to sync (None = all)
        full_sync: Force full sync (ignore last sync time)
        skip_auto_analysis: Skip automatic open order analysis after sales_orders sync
        batch_size: Override default batch size for iterators
        show_progress: Show progress during sync
        check_orphaned: Check for orphaned records after sync
    """
    # Create sync handler
    sync_handler = RecordSyncHandler(qb, db)

    # Configure batch size if specified
    if batch_size:
        sync_handler.set_batch_size(batch_size)

    # Configure progress display
    sync_handler.set_progress_display(show_progress)

    # Use progress callback if progress is enabled
    callback = progress_callback if show_progress else None

    # Track if sales_orders was synced and had updates
    sales_orders_updated = False

    # Track which tables with line items were synced
    synced_tables_with_line_items = []

    # Determine which tables to sync
    if tables:
        # Filter to requested tables
        tables_to_sync = [
            config for config in TABLE_CONFIGS
            if config['name'] in tables
        ]
        if not tables_to_sync:
            logging.error(f"No valid tables found in: {tables}")
            return
    else:
        # Sync all tables
        tables_to_sync = TABLE_CONFIGS

    total_tables = len(tables_to_sync)
    logging.info(f"Syncing {total_tables} tables...")

    # Process each table
    for i, table_config in enumerate(tables_to_sync, 1):
        table_name = table_config['name']

        logging.info(f"\n===== Table {i}/{total_tables}: {table_name.upper()} =====")

        try:
            # Get record count before sync
            record_count_before = 0
            if table_name == 'sales_orders':
                try:
                    record_count_before = db.get_record_count('sales_orders')
                except:
                    record_count_before = 0

            # Sync with iterator support
            sync_handler.sync_table(
                table_config,
                force_full_sync=full_sync,
                batch_size=batch_size,
                progress_callback=callback
            )

            # Clear the progress line
            if show_progress:
                print()  # New line after progress

            # Track tables with line items that were synced
            if table_config.get('has_line_items', False) and table_name in [
                'invoices', 'sales_orders', 'bills', 'purchase_orders',
                'estimates', 'credit_memos', 'sales_receipts'
            ]:
                synced_tables_with_line_items.append(table_name)

            # Check if sales_orders was updated
            if table_name == 'sales_orders':
                try:
                    record_count_after = db.get_record_count('sales_orders')
                    if record_count_after != record_count_before:
                        sales_orders_updated = True
                        logging.info(f"Sales orders updated: {record_count_before} -> {record_count_after}")
                except:
                    # Assume updates happened if we can't check
                    sales_orders_updated = True

        except Exception as e:
            logging.error(f"Error syncing {table_name}: {str(e)}", exc_info=True)
            # Continue with next table
            continue

    logging.info("\nSync process completed")

    # Check for orphaned records if enabled and tables with line items were synced
    if check_orphaned and synced_tables_with_line_items:
        logging.info("\n===== CHECKING FOR ORPHANED RECORDS (QB METADATA BUG) =====")
        try:
            # Only check tables that were actually synced
            total_orphaned = 0
            tables_with_orphaned = []

            for table_name in synced_tables_with_line_items:
                orphaned_records = db.detect_orphaned_records(table_name)
                if orphaned_records:
                    logging.info(f"{table_name}: Found {len(orphaned_records)} orphaned records")
                    total_orphaned += len(orphaned_records)
                    tables_with_orphaned.append(table_name)
                else:
                    logging.info(f"{table_name}: No orphaned records found")

            if total_orphaned > 0:
                logging.info(f"\nTotal orphaned records found: {total_orphaned}")

                # Auto-fix if enabled
                if auto_fix_orphaned:
                    logging.info("\n===== AUTO-FIXING ORPHANED RECORDS =====")
                    sync_handler = RecordSyncHandler(qb, db)

                    for table_name in tables_with_orphaned:
                        logging.info(f"\nFixing orphaned records for {table_name}...")
                        stats = sync_handler.fix_orphaned_records(table_name)

                        logging.info(f"Results for {table_name}:")
                        logging.info(f"  - Detected: {stats['detected']}")
                        logging.info(f"  - Fixed: {stats['fixed']}")
                        logging.info(f"  - Failed: {stats['failed']}")
                        logging.info(f"  - Skipped: {stats['skipped']}")
                else:
                    logging.info("Run with --fix-orphaned to attempt automatic fixes")
            else:
                logging.info("\nNo orphaned records detected - all records have proper line items")

        except Exception as e:
            logging.error(f"Error checking/fixing orphaned records: {str(e)}", exc_info=True)

    # Auto-analyze open orders if configured and sales_orders was updated
    if sales_orders_updated and not skip_auto_analysis:
        # Check config for auto-analysis setting
        auto_analyze = SYNC_CONFIG.get('auto_analyze_open_orders', True)

        if auto_analyze:
            logging.info("\n===== AUTO-ANALYZING OPEN ORDERS (sales_orders was updated) =====")
            try:
                price_analyzer = PriceAnalyzer(qb, db)
                price_analyzer.analyze_open_orders()
                logging.info("Auto-analysis of open orders completed")
            except Exception as e:
                logging.error(f"Error during auto-analysis: {str(e)}", exc_info=True)
                # Don't fail the sync if analysis fails
        else:
            logging.info("Auto-analysis is disabled in configuration")


def fix_orphaned_records(qb: QuickBooksConnection, db: DatabaseInterface,
                        tables: Optional[List[str]] = None, force_retry: bool = False) -> None:
    """
    Fix orphaned records (QB metadata bug)

    Args:
        qb: QuickBooks connection
        db: Database interface
        tables: Specific tables to fix (None = all supported tables)
        force_retry: If True, retry all records regardless of previous attempts
    """
    sync_handler = RecordSyncHandler(qb, db)

    if tables:
        # Fix specific tables
        for table_name in tables:
            if table_name in ['invoices', 'sales_orders', 'bills', 'purchase_orders',
                            'estimates', 'credit_memos', 'sales_receipts']:
                logging.info(f"\n===== FIXING ORPHANED RECORDS FOR {table_name.upper()} =====")
                stats = sync_handler.fix_orphaned_records(table_name, force_retry=force_retry)

                logging.info(f"Results for {table_name}:")
                logging.info(f"  - Detected: {stats['detected']}")
                logging.info(f"  - Fixed: {stats['fixed']}")
                logging.info(f"  - Failed: {stats['failed']}")
                logging.info(f"  - Skipped: {stats['skipped']}")
            else:
                logging.warning(f"Table {table_name} does not support orphaned record fixing")
    else:
        # Fix all supported tables
        stats = sync_handler.fix_orphaned_records_all_tables(force_retry=force_retry)


def analyze_prices(qb: QuickBooksConnection, db: DatabaseInterface,
                  analyze_open: bool = False, analyze_history: bool = False,
                  history_months: int = 6, max_orders: Optional[int] = None) -> None:
    """
    Analyze customer pricing patterns

    Args:
        qb: QuickBooks connection
        db: Database interface
        analyze_open: Analyze open sales orders
        analyze_history: Extract historical pricing
        history_months: Number of months for historical analysis
        max_orders: Maximum number of orders to analyze (for testing)
    """
    price_analyzer = PriceAnalyzer(qb, db)

    if analyze_open:
        logging.info("\n===== OPEN ORDER PRICE ANALYSIS =====")
        price_analyzer.analyze_open_orders(max_orders=max_orders)

    if analyze_history:
        logging.info(f"\n===== HISTORICAL PRICE EXTRACTION ({history_months} months) =====")
        price_analyzer.extract_historical_prices(months=history_months)


def test_connection(qb_config: Dict[str, Any]) -> bool:
    """Test QuickBooks connection"""
    logging.info("Testing QuickBooks connection...")

    qb = QuickBooksConnection(qb_config)
    if qb.connect():
        logging.info("Connection successful!")
        logging.info(f"Max QBXML Version: {qb.max_qbxml_version}")
        qb.disconnect()
        return True
    else:
        logging.error("Connection failed!")
        return False


def show_orphaned_report(db: DatabaseInterface) -> None:
    """Show report of failed orphaned record fixes"""
    logging.info("\n===== ORPHANED RECORDS REPORT =====")

    # Check each table for orphaned records
    tables_to_check = ['invoices', 'sales_orders', 'bills', 'purchase_orders',
                      'estimates', 'credit_memos', 'sales_receipts']

    total_orphaned = 0

    for table_name in tables_to_check:
        orphaned = db.detect_orphaned_records(table_name)
        if orphaned:
            logging.info(f"\n{table_name.upper()}: {len(orphaned)} orphaned records")
            for record in orphaned[:5]:  # Show first 5
                logging.info(f"  - {record['RefNumber']} (Amount: ${record['Amount']:.2f})")
            if len(orphaned) > 5:
                logging.info(f"  ... and {len(orphaned) - 5} more")
            total_orphaned += len(orphaned)

    if total_orphaned == 0:
        logging.info("\nNo orphaned records found!")
    else:
        logging.info(f"\nTotal orphaned records: {total_orphaned}")

    # Show failed fix attempts
    failed_attempts = db.get_failed_fix_attempts()
    if failed_attempts:
        logging.info(f"\n\nFAILED FIX ATTEMPTS (after 3 tries):")
        for attempt in failed_attempts:
            logging.info(f"  - {attempt['TableName']} {attempt['RefNumber']} - {attempt['LastError']}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='QuickBooks Sync with Iterator Support')
    parser.add_argument('--table', help='Sync specific table only')
    parser.add_argument('--tables', nargs='+', help='Sync specific tables')
    parser.add_argument('--full', action='store_true', help='Force full sync (ignore last sync time)')
    parser.add_argument('--test-connection', action='store_true', help='Test QB connection only')
    parser.add_argument('--verify-db', action='store_true', help='Verify database only')
    parser.add_argument('--list-tables', action='store_true', help='List available tables')

    # Iterator options
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of records per batch (default: 100)')
    parser.add_argument('--no-progress', action='store_true',
                       help='Disable progress display')

    # Price analysis options
    parser.add_argument('--analyze-open-orders', action='store_true',
                       help='Analyze pricing on open sales orders')
    parser.add_argument('--extract-historical-prices', action='store_true',
                       help='Extract customer-item prices from historical sales orders')
    parser.add_argument('--months', type=int, default=6,
                       help='Number of months for historical price extraction (default: 6)')
    parser.add_argument('--max-orders', type=int,
                       help='Maximum number of orders to analyze (for testing)')

    # Alternate QuickBooks file options
    parser.add_argument('--qb-file',
                       help='Path to alternate QuickBooks file (for offline price extraction)')
    parser.add_argument('--rebuild-prices', action='store_true',
                       help='Clear and rebuild entire customer_price_pages table')

    # Auto-analysis control
    parser.add_argument('--skip-auto-analysis', action='store_true',
                       help='Skip automatic open order analysis after sales_orders sync')

    # Orphaned record options
    parser.add_argument('--skip-orphaned-check', action='store_true',
                       help='Skip checking for orphaned records after sync')
    parser.add_argument('--fix-orphaned', action='store_true',
                       help='Fix orphaned records (QB metadata bug)')
    parser.add_argument('--fix-orphaned-force', action='store_true',
                       help='Force retry all orphaned records regardless of previous attempts')
    parser.add_argument('--orphaned-report', action='store_true',
                       help='Show report of orphaned records')

    args = parser.parse_args()

    # Setup logging
    setup_logging()

    overall_start = time.time()
    logging.info(f"===== QUICKBOOKS SYNC STARTED AT {datetime.datetime.now()} =====")
    logging.info(f"Using iterator-based sync with batch size: {args.batch_size}")

    try:
        # Handle special commands
        if args.list_tables:
            logging.info("Available tables:")
            for config in TABLE_CONFIGS:
                logging.info(f"  - {config['name']}")
            return

        # Initialize database
        db = initialize_database()
        db.connect()

        # Initialize sync schedules
        db.initialize_sync_schedule(SYNC_SCHEDULE_DEFAULTS)

        # Initialize metadata bug tracker table
        db.initialize_metadata_bug_tracker()

        if args.verify_db:
            verify_database(db)
            return

        if args.orphaned_report:
            show_orphaned_report(db)
            return

        # Override QB file if specified
        qb_config = QB_CONFIG.copy()  # Make a copy to avoid modifying global config

        if args.qb_file:
            # Normalize the path to use backslashes on Windows
            normalized_path = os.path.normpath(args.qb_file)

            if not os.path.exists(normalized_path):
                logging.error(f"QuickBooks file not found: {normalized_path}")
                return

            qb_config['company_file'] = normalized_path
            logging.info(f"Using alternate QuickBooks file: {normalized_path}")

        # Initialize QuickBooks with potentially modified config
        qb = QuickBooksConnection(qb_config)

        if args.test_connection:
            test_connection(qb_config)
            return

        # Connect to QuickBooks
        logging.info("Connecting to QuickBooks...")
        if not qb.connect():
            logging.error("Failed to connect to QuickBooks")

            # Update sync status for all tables
            for config in TABLE_CONFIGS:
                db.update_sync_timestamp(
                    config['name'],
                    status="NO_CONNECTION",
                    error_message="Could not connect to QuickBooks"
                )

            return

        try:
            # Verify database before sync
            logging.info("\n==== PRE-SYNC DATABASE VERIFICATION ====")
            verify_database(db)

            # Handle price rebuild if requested
            if args.rebuild_prices and args.extract_historical_prices:
                logging.warning("Rebuilding customer_price_pages table...")
                try:
                    db.execute_query("DELETE FROM customer_price_pages")
                    logging.info("Cleared all existing customer price data")
                except Exception as e:
                    logging.warning(f"Could not clear customer_price_pages: {e}")
                    # Continue anyway - table might not exist yet

            # Handle orphaned record fixing
            if args.fix_orphaned or args.fix_orphaned_force:
                tables_to_fix = None
                if args.table:
                    tables_to_fix = [args.table]
                elif args.tables:
                    tables_to_fix = args.tables

                fix_orphaned_records(qb, db, tables_to_fix, force_retry=args.fix_orphaned_force)

            # Handle price analysis
            elif args.analyze_open_orders or args.extract_historical_prices:
                analyze_prices(qb, db,
                             analyze_open=args.analyze_open_orders,
                             analyze_history=args.extract_historical_prices,
                             history_months=args.months,
                             max_orders=args.max_orders)
            else:
                # Regular table sync
                tables_to_sync = None
                if args.table:
                    tables_to_sync = [args.table]
                elif args.tables:
                    tables_to_sync = args.tables

                # Perform sync with iterators
                sync_tables(
                    qb, db,
                    tables_to_sync,
                    args.full,
                    args.skip_auto_analysis,
                    batch_size=args.batch_size,
                    show_progress=not args.no_progress,
                    check_orphaned=not args.skip_orphaned_check,
                    auto_fix_orphaned=True  # Always fix orphaned records
                )

            # Verify database after sync
            logging.info("\n==== POST-SYNC DATABASE VERIFICATION ====")
            verify_database(db)

        finally:
            # Always disconnect
            qb.disconnect()

    except KeyboardInterrupt:
        logging.info("Sync interrupted by user")
    except Exception as e:
        logging.critical(f"Unhandled exception: {str(e)}", exc_info=True)
    finally:
        # Cleanup
        if 'db' in locals():
            db.disconnect()

        overall_duration = time.time() - overall_start
        logging.info(
            f"\n===== SYNC COMPLETED IN {overall_duration:.2f} SECONDS "
            f"({overall_duration/60:.2f} MINUTES) ====="
        )


if __name__ == "__main__":
    main()