"""
SQLite implementation of database interface
Modified to store the maximum TimeModified value as last_sync_time
"""
import sqlite3
import logging
import os
from typing import List, Dict, Any, Optional, Set, Tuple
import datetime
from contextlib import contextmanager

from .base import DatabaseInterface, SyncStatus, FieldTypes


class SQLiteDatabase(DatabaseInterface):
    """SQLite implementation of database interface"""

    def __init__(self, config: Dict[str, Any]):
        self.db_path = config['path']
        self.connection = None
        self.in_transaction = False

        # Ensure directory exists
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            logging.info(f"Created directory for database: {db_dir}")

    def connect(self) -> None:
        """Establish database connection"""
        if self.connection is None:
            self.connection = sqlite3.connect(
                self.db_path,
                timeout=30.0,  # 30 second timeout
                check_same_thread=False  # Allow multi-threaded access
            )
            self.connection.row_factory = sqlite3.Row

            # Optimize SQLite for better performance
            self.connection.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging
            self.connection.execute("PRAGMA synchronous=NORMAL")  # Faster writes
            self.connection.execute("PRAGMA cache_size=10000")  # Larger cache
            self.connection.execute("PRAGMA temp_store=MEMORY")  # Use memory for temp tables
            self.connection.execute("PRAGMA busy_timeout=30000")  # 30 second busy timeout

            logging.debug(f"Connected to SQLite database: {self.db_path}")

    def disconnect(self) -> None:
        """Close database connection"""
        if self.connection:
            if self.in_transaction:
                self.rollback_transaction()
            self.connection.close()
            self.connection = None
            logging.debug("Disconnected from SQLite database")

    @contextmanager
    def _get_cursor(self):
        """Context manager for database cursor"""
        self.connect()
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    # In sqlite_db.py, update the create_table method around line 64:

    def create_table(self, table_name: str, fields_dict: Dict[str, str], primary_key: str) -> None:
        """Create table with specified schema"""
        with self._get_cursor() as cursor:
            # Check if table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )

            if cursor.fetchone():
                # Table exists, check for missing columns
                logging.info(f"Table '{table_name}' exists. Checking for missing columns...")
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                existing_columns = {row[1]: row[2] for row in cursor.fetchall()}

                for field_name, field_type in fields_dict.items():
                    if field_name not in existing_columns:
                        self.add_column(table_name, field_name, field_type)
            else:
                # Create new table
                logging.info(f"Creating table '{table_name}'...")

                # Ensure we have at least the primary key
                if not fields_dict:
                    logging.warning(f"No fields provided for table '{table_name}', creating with primary key only")
                    fields_dict = {primary_key: 'TEXT'}
                elif primary_key not in fields_dict:
                    logging.warning(f"Primary key '{primary_key}' not in fields for table '{table_name}', adding it")
                    fields_dict[primary_key] = 'TEXT'

                column_defs = []
                for field_name, field_type in fields_dict.items():
                    col_def = f'"{field_name}" {field_type}'
                    if field_name == primary_key:
                        col_def += " PRIMARY KEY"
                    column_defs.append(col_def)

                # Debug logging
                logging.debug(f"Creating table with columns: {column_defs}")

                create_sql = f'CREATE TABLE "{table_name}" ({", ".join(column_defs)})'

                # Log the SQL for debugging
                logging.debug(f"CREATE SQL: {create_sql}")

                cursor.execute(create_sql)

                # Create index on primary key
                if primary_key in fields_dict:
                    index_name = f"idx_{table_name}_{primary_key}"
                    cursor.execute(
                        f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ("{primary_key}")'
                    )

                # For line item tables, index the parent key
                if "_line_items" in table_name:
                    parent_key = 'TxnID' if 'TxnID' in fields_dict else 'ListID'
                    if parent_key in fields_dict and parent_key != primary_key:
                        fk_index_name = f"idx_{table_name}_{parent_key}_fk"
                        cursor.execute(
                            f'CREATE INDEX IF NOT EXISTS "{fk_index_name}" ON "{table_name}" ("{parent_key}")'
                        )

                self.connection.commit()
                logging.info(f"Table '{table_name}' created successfully")


    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        with self._get_cursor() as cursor:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            return cursor.fetchone() is not None

    def add_column(self, table_name: str, column_name: str, column_type: str) -> None:
        """Add column to existing table"""
        with self._get_cursor() as cursor:
            # Check if column already exists
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            if any(col[1] == column_name for col in cursor.fetchall()):
                logging.debug(f"Column '{column_name}' already exists in '{table_name}'")
                return

            try:
                alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type}'
                cursor.execute(alter_sql)
                self.connection.commit()
                logging.info(f"Added column '{column_name}' to table '{table_name}'")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e).lower():
                    logging.warning(f"Column '{column_name}' already exists in '{table_name}'")
                else:
                    raise

    def insert_records(self, table_name: str, records: List[Dict[str, Any]],
                       fields_dict: Dict[str, str], primary_key: str,
                       modified_field: str, force_update: bool = False) -> Tuple[int, int, int]:
        """
        Insert or update records

        Args:
            table_name: Name of the table
            records: List of record dictionaries
            fields_dict: Field name to type mapping
            primary_key: Primary key field name
            modified_field: Field containing modification timestamp
            force_update: If True, update all records regardless of TimeModified

        Returns:
            Tuple of (insert_count, update_count, skip_count)
        """
        insert_count = 0
        update_count = 0
        skip_count = 0

        with self._get_cursor() as cursor:
            for record in records:
                # Ensure all fields exist
                for field in record.keys():
                    if field not in fields_dict:
                        logging.warning(f"Field '{field}' not in schema, adding as TEXT")
                        fields_dict[field] = FieldTypes.TEXT
                        self.add_column(table_name, field, FieldTypes.TEXT)

                pk_value = record.get(primary_key)
                if pk_value is None:
                    logging.warning(f"Record missing primary key '{primary_key}', skipping")
                    skip_count += 1
                    continue

                # Check if record exists
                cursor.execute(
                    f'SELECT "{modified_field}" FROM "{table_name}" WHERE "{primary_key}" = ?',
                    (pk_value,)
                )
                existing = cursor.fetchone()

                # Prepare values
                columns = list(fields_dict.keys())
                values = []
                for col in columns:
                    val = record.get(col)
                    if isinstance(val, bool):
                        values.append(1 if val else 0)
                    elif isinstance(val, str) and fields_dict.get(col) == FieldTypes.INTEGER and val.lower() in ['true',
                                                                                                                 'false']:
                        values.append(1 if val.lower() == 'true' else 0)
                    else:
                        values.append(val)

                if existing:
                    # Update existing record
                    if force_update:
                        # Force update during full sync - update regardless of TimeModified
                        set_clause = ', '.join([f'"{col}" = ?' for col in columns])
                        update_sql = f'UPDATE "{table_name}" SET {set_clause} WHERE "{primary_key}" = ?'
                        cursor.execute(update_sql, values + [pk_value])
                        update_count += 1
                    else:
                        # Normal update logic - check TimeModified
                        qb_modified = record.get(modified_field)
                        db_modified = existing[0] if existing else None

                        if qb_modified and db_modified:
                            try:
                                if qb_modified > db_modified:
                                    # Update record
                                    set_clause = ', '.join([f'"{col}" = ?' for col in columns])
                                    update_sql = f'UPDATE "{table_name}" SET {set_clause} WHERE "{primary_key}" = ?'
                                    cursor.execute(update_sql, values + [pk_value])
                                    update_count += 1
                                else:
                                    skip_count += 1
                            except TypeError:
                                # Can't compare, default to update
                                set_clause = ', '.join([f'"{col}" = ?' for col in columns])
                                update_sql = f'UPDATE "{table_name}" SET {set_clause} WHERE "{primary_key}" = ?'
                                cursor.execute(update_sql, values + [pk_value])
                                update_count += 1
                        else:
                            # No modified field comparison possible, update
                            set_clause = ', '.join([f'"{col}" = ?' for col in columns])
                            update_sql = f'UPDATE "{table_name}" SET {set_clause} WHERE "{primary_key}" = ?'
                            cursor.execute(update_sql, values + [pk_value])
                            update_count += 1
                else:
                    # Insert new record
                    columns_sql = ', '.join([f'"{col}"' for col in columns])
                    placeholders = ', '.join(['?' for _ in columns])
                    insert_sql = f'INSERT INTO "{table_name}" ({columns_sql}) VALUES ({placeholders})'
                    cursor.execute(insert_sql, values)
                    insert_count += 1

                # Commit periodically for large datasets
                if (insert_count + update_count) % 1000 == 0:
                    self.connection.commit()

            self.connection.commit()
            logging.info(f"Table '{table_name}': {insert_count} inserted, {update_count} updated, {skip_count} skipped")

        return insert_count, update_count, skip_count

    def insert_records_batch(self, table_name: str, records: List[Dict[str, Any]],
                             fields_dict: Dict[str, str], primary_key: str) -> int:
        """Insert multiple records in a single transaction"""
        if not records:
            return 0

        inserted = 0
        with self._get_cursor() as cursor:
            # Ensure all fields exist
            all_fields = set()
            for record in records:
                all_fields.update(record.keys())

            for field in all_fields:
                if field not in fields_dict:
                    fields_dict[field] = FieldTypes.TEXT
                    self.add_column(table_name, field, FieldTypes.TEXT)

            # Prepare SQL
            columns = list(fields_dict.keys())
            columns_sql = ', '.join([f'"{col}"' for col in columns])
            placeholders = ', '.join(['?' for _ in columns])
            sql = f'INSERT OR REPLACE INTO "{table_name}" ({columns_sql}) VALUES ({placeholders})'

            # Insert all records in one transaction
            try:
                cursor.execute("BEGIN IMMEDIATE")

                for record in records:
                    values = []
                    for col in columns:
                        val = record.get(col)
                        if isinstance(val, bool):
                            values.append(1 if val else 0)
                        elif isinstance(val, str) and fields_dict.get(col) == FieldTypes.INTEGER and val.lower() in [
                            'true', 'false']:
                            values.append(1 if val.lower() == 'true' else 0)
                        else:
                            values.append(val)

                    cursor.execute(sql, values)
                    inserted += 1

                cursor.execute("COMMIT")
            except Exception as e:
                cursor.execute("ROLLBACK")
                raise

        return inserted

    def insert_single_record(self, table_name: str, record: Dict[str, Any],
                             fields_dict: Dict[str, str], primary_key: str) -> None:
        """Insert or replace single record"""
        with self._get_cursor() as cursor:
            # Ensure all fields exist
            for field in record.keys():
                if field not in fields_dict:
                    fields_dict[field] = FieldTypes.TEXT
                    self.add_column(table_name, field, FieldTypes.TEXT)

            columns = list(fields_dict.keys())
            values = []
            for col in columns:
                val = record.get(col)
                if isinstance(val, bool):
                    values.append(1 if val else 0)
                elif isinstance(val, str) and fields_dict.get(col) == FieldTypes.INTEGER and val.lower() in ['true',
                                                                                                             'false']:
                    values.append(1 if val.lower() == 'true' else 0)
                else:
                    values.append(val)

            columns_sql = ', '.join([f'"{col}"' for col in columns])
            placeholders = ', '.join(['?' for _ in columns])
            sql = f'INSERT OR REPLACE INTO "{table_name}" ({columns_sql}) VALUES ({placeholders})'
            cursor.execute(sql, values)
            self.connection.commit()

    def delete_records(self, table_name: str, where_field: str, where_value: Any) -> int:
        """Delete records matching condition"""
        with self._get_cursor() as cursor:
            cursor.execute(f'DELETE FROM "{table_name}" WHERE "{where_field}" = ?', (where_value,))
            deleted = cursor.rowcount
            self.connection.commit()
            return deleted

    def get_record_count(self, table_name: str) -> int:
        """Get total record count for table"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                return cursor.fetchone()[0]
        except sqlite3.Error:
            return 0

    def get_last_sync_time(self, table_name: str) -> Optional[str]:
        """
        Get last successful sync time for table
        Returns the maximum TimeModified value from the last successful sync
        """
        with self._get_cursor() as cursor:
            # Check sync_log for last successful sync
            if self.table_exists('sync_log'):
                cursor.execute(
                    "SELECT last_sync_time FROM sync_log WHERE table_name = ? AND last_status = ?",
                    (table_name, SyncStatus.SUCCESS)
                )
                result = cursor.fetchone()
                if result and result[0]:
                    # Return the stored timestamp directly
                    # This is now the MAX(TimeModified) from the previous sync
                    return result[0]

            # Fallback for initial sync - get MAX(TimeModified) from table
            if self.table_exists(table_name):
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                columns = [info[1] for info in cursor.fetchall()]

                if 'TimeModified' in columns:
                    cursor.execute(f'SELECT MAX(TimeModified) FROM "{table_name}" WHERE TimeModified IS NOT NULL')
                    result = cursor.fetchone()
                    if result and result[0]:
                        # Add 1 second buffer to avoid re-processing the same record
                        try:
                            dt = datetime.datetime.fromisoformat(result[0])
                            dt += datetime.timedelta(seconds=1)
                            return dt.isoformat(timespec='seconds')
                        except ValueError:
                            return result[0]

            return None

    def update_sync_timestamp(self, table_name: str, duration: Optional[float] = None,
                              status: str = "SUCCESS", error_message: Optional[str] = None,
                              max_time_modified: Optional[str] = None) -> None:
        """
        Update sync log with latest sync information

        Args:
            table_name: Name of the table
            duration: Sync duration in seconds
            status: Sync status (SUCCESS, ERROR, etc.)
            error_message: Error message if any
            max_time_modified: Maximum TimeModified value from the synced records
        """
        with self._get_cursor() as cursor:
            # Create sync_log table if needed
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_log (
                table_name TEXT PRIMARY KEY,
                last_sync_time TEXT,
                record_count INTEGER,
                sync_duration_seconds REAL,
                last_status TEXT DEFAULT 'SUCCESS',
                last_error_message TEXT,
                consecutive_failures INTEGER DEFAULT 0
            )
            ''')

            # Get record count
            record_count = self.get_record_count(table_name) if self.table_exists(table_name) else 0

            # Get current consecutive failures
            cursor.execute(
                "SELECT consecutive_failures FROM sync_log WHERE table_name = ?",
                (table_name,)
            )
            result = cursor.fetchone()
            current_failures = result[0] if result else 0

            # Update consecutive failures
            if status in [SyncStatus.LOCKED, SyncStatus.BUSY, SyncStatus.ERROR, SyncStatus.EDITING]:
                consecutive_failures = current_failures + 1
            else:
                consecutive_failures = 0

            # Determine what timestamp to store
            if status == SyncStatus.SUCCESS and max_time_modified:
                # Store the maximum TimeModified from the sync
                sync_timestamp = max_time_modified
            elif status == SyncStatus.SUCCESS and self.table_exists(table_name):
                # Fallback: get MAX(TimeModified) from the table
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                columns = [info[1] for info in cursor.fetchall()]

                if 'TimeModified' in columns:
                    cursor.execute(f'SELECT MAX(TimeModified) FROM "{table_name}" WHERE TimeModified IS NOT NULL')
                    result = cursor.fetchone()
                    sync_timestamp = result[0] if result and result[0] else datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
                else:
                    # No TimeModified field, use current time
                    sync_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
            else:
                # For non-success statuses, keep the previous timestamp
                cursor.execute(
                    "SELECT last_sync_time FROM sync_log WHERE table_name = ?",
                    (table_name,)
                )
                result = cursor.fetchone()
                sync_timestamp = result[0] if result and result[0] else datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')

            # Update sync_log
            cursor.execute('''
            INSERT INTO sync_log (table_name, last_sync_time, record_count, sync_duration_seconds, 
                                  last_status, last_error_message, consecutive_failures)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(table_name) DO UPDATE SET
                last_sync_time = CASE WHEN excluded.last_status = 'SUCCESS' THEN excluded.last_sync_time ELSE last_sync_time END,
                record_count = CASE WHEN excluded.last_status = 'SUCCESS' THEN excluded.record_count ELSE record_count END,
                sync_duration_seconds = excluded.sync_duration_seconds,
                last_status = excluded.last_status,
                last_error_message = excluded.last_error_message,
                consecutive_failures = excluded.consecutive_failures
            ''', (table_name, sync_timestamp, record_count, duration, status, error_message, consecutive_failures))

            self.connection.commit()

            status_msg = f"Updated sync timestamp for {table_name} - Status: {status}"
            if status == SyncStatus.SUCCESS:
                status_msg += f" with {record_count} records, last modified: {sync_timestamp}"
            else:
                status_msg += f", Consecutive failures: {consecutive_failures}"
            if duration is not None:
                status_msg += f" in {duration:.2f} seconds"
            logging.info(status_msg)

    def get_max_time_modified(self, table_name: str, modified_field: str = "TimeModified") -> Optional[str]:
        """
        Get the maximum TimeModified value from a table

        Args:
            table_name: Name of the table
            modified_field: Name of the timestamp field (default: TimeModified)

        Returns:
            Maximum timestamp as ISO string, or None if not found
        """
        if not self.table_exists(table_name):
            return None

        with self._get_cursor() as cursor:
            # Check if the field exists
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns = [info[1] for info in cursor.fetchall()]

            if modified_field not in columns:
                return None

            # Get the maximum value
            cursor.execute(f'SELECT MAX("{modified_field}") FROM "{table_name}" WHERE "{modified_field}" IS NOT NULL')
            result = cursor.fetchone()

            return result[0] if result and result[0] else None

    def initialize_sync_schedule(self, schedules: List[Tuple]) -> None:
        """Initialize sync schedule table with default values"""
        with self._get_cursor() as cursor:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_schedule (
                table_name TEXT PRIMARY KEY,
                business_hours_start TEXT DEFAULT '06:00',
                business_hours_end TEXT DEFAULT '22:00',
                business_days TEXT DEFAULT 'Mon-Fri',
                business_hours_interval_minutes INTEGER DEFAULT 60,
                after_hours_interval_minutes INTEGER DEFAULT 360,
                weekend_interval_minutes INTEGER DEFAULT 720,
                priority INTEGER DEFAULT 5,
                is_enabled INTEGER DEFAULT 1
            )
            ''')

            for table_name, bh_interval, ah_interval, we_interval, priority in schedules:
                cursor.execute('''
                INSERT OR IGNORE INTO sync_schedule 
                (table_name, business_hours_interval_minutes, after_hours_interval_minutes, 
                 weekend_interval_minutes, priority)
                VALUES (?, ?, ?, ?, ?)
                ''', (table_name, bh_interval, ah_interval, we_interval, priority))

            self.connection.commit()

    def get_sync_schedule(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get sync schedule for specific table"""
        with self._get_cursor() as cursor:
            cursor.execute(
                "SELECT * FROM sync_schedule WHERE table_name = ?",
                (table_name,)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

    def track_custom_fields(self, table_name: str, header_fields: Set[str],
                            line_fields: Optional[Set[str]] = None) -> None:
        """Track discovered custom fields"""
        with self._get_cursor() as cursor:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS custom_fields_registry (
                table_name TEXT,
                field_name TEXT,
                field_level TEXT,
                last_seen TEXT,
                PRIMARY KEY (table_name, field_name)
            )
            ''')

            current_time = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')

            # Track header fields
            for field_name in header_fields:
                cursor.execute('''
                INSERT INTO custom_fields_registry (table_name, field_name, field_level, last_seen)
                VALUES (?, ?, 'HEADER', ?)
                ON CONFLICT(table_name, field_name) DO UPDATE SET last_seen = excluded.last_seen
                ''', (table_name, field_name, current_time))

            # Track line fields
            if line_fields:
                line_table = f"{table_name}_line_items"
                for field_name in line_fields:
                    cursor.execute('''
                    INSERT INTO custom_fields_registry (table_name, field_name, field_level, last_seen)
                    VALUES (?, ?, 'LINE', ?)
                    ON CONFLICT(table_name, field_name) DO UPDATE SET last_seen = excluded.last_seen
                    ''', (line_table, field_name, current_time))

            self.connection.commit()

    def get_known_custom_fields(self, table_name: str) -> Tuple[Set[str], Set[str]]:
        """Get known custom fields for table"""
        header_fields = set()
        line_fields = set()

        if not self.table_exists('custom_fields_registry'):
            return header_fields, line_fields

        with self._get_cursor() as cursor:
            # Get header fields
            cursor.execute(
                "SELECT field_name FROM custom_fields_registry WHERE table_name = ? AND field_level = 'HEADER'",
                (table_name,)
            )
            for row in cursor.fetchall():
                header_fields.add(row[0])

            # Get line fields
            line_table = f"{table_name}_line_items"
            cursor.execute(
                "SELECT field_name FROM custom_fields_registry WHERE table_name = ? AND field_level = 'LINE'",
                (line_table,)
            )
            for row in cursor.fetchall():
                line_fields.add(row[0])

        return header_fields, line_fields

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute raw SQL query"""
        with self._get_cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()

    def begin_transaction(self) -> None:
        """Begin database transaction"""
        if not self.in_transaction:
            self.connect()
            self.connection.execute("BEGIN")
            self.in_transaction = True

    def commit_transaction(self) -> None:
        """Commit database transaction"""
        if self.in_transaction:
            self.connection.commit()
            self.in_transaction = False

    def rollback_transaction(self) -> None:
        """Rollback database transaction"""
        if self.in_transaction:
            self.connection.rollback()
            self.in_transaction = False

    def save_customer_prices(self, price_records: List[Dict[str, Any]]) -> None:
        """Save customer-specific pricing data"""
        with self._get_cursor() as cursor:
            # Create table if needed
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_price_pages (
                CustomerListID TEXT NOT NULL,
                CustomerName TEXT,
                ItemListID TEXT NOT NULL,
                ItemName TEXT,
                ItemFullName TEXT,
                Price REAL,
                LastUpdated TEXT,
                PRIMARY KEY (CustomerListID, ItemListID)
            )
            ''')

            # Create indexes
            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_customer_prices_customer 
            ON customer_price_pages (CustomerListID)
            ''')

            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_customer_prices_item 
            ON customer_price_pages (ItemListID)
            ''')

            current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()

            for price_info in price_records:
                cursor.execute('''
                INSERT INTO customer_price_pages 
                (CustomerListID, CustomerName, ItemListID, ItemName, ItemFullName, Price, LastUpdated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(CustomerListID, ItemListID) DO UPDATE SET
                    CustomerName = excluded.CustomerName,
                    ItemName = excluded.ItemName,
                    ItemFullName = excluded.ItemFullName,
                    Price = excluded.Price,
                    LastUpdated = excluded.LastUpdated
                ''', (
                    price_info['CustomerListID'],
                    price_info['CustomerName'],
                    price_info['ItemListID'],
                    price_info['ItemName'],
                    price_info['ItemFullName'],
                    price_info['Rate'],
                    current_time
                ))

            self.connection.commit()
            logging.info(f"Saved {len(price_records)} customer prices to database")

    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Get list of all tables with basic info"""
        tables = []
        with self._get_cursor() as cursor:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            for row in cursor.fetchall():
                table_name = row[0]
                try:
                    count = self.get_record_count(table_name)
                    tables.append({
                        'name': table_name,
                        'record_count': count
                    })
                except sqlite3.Error:
                    tables.append({
                        'name': table_name,
                        'record_count': -1
                    })
        return tables

    def verify_database(self) -> Dict[str, Any]:
        """Verify database integrity and return stats"""
        stats = {
            'database_path': self.db_path,
            'exists': os.path.exists(self.db_path),
            'size_mb': 0,
            'tables': [],
            'sync_log': [],
            'custom_fields_count': 0
        }

        if stats['exists']:
            stats['size_mb'] = os.path.getsize(self.db_path) / (1024 * 1024)

            # Get all tables
            stats['tables'] = self.get_all_tables()

            # Get sync log info
            if self.table_exists('sync_log'):
                with self._get_cursor() as cursor:
                    cursor.execute(
                        "SELECT table_name, last_sync_time, record_count, sync_duration_seconds, "
                        "last_status, consecutive_failures FROM sync_log ORDER BY table_name"
                    )
                    for row in cursor.fetchall():
                        stats['sync_log'].append(dict(row))

            # Get custom fields count
            if self.table_exists('custom_fields_registry'):
                stats['custom_fields_count'] = self.get_record_count('custom_fields_registry')

        return stats