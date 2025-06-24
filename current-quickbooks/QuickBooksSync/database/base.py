"""
Base database interface for QuickBooks sync
Provides abstract interface that can be implemented by SQLite, SQL Express, etc.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Set, Tuple
import datetime


class DatabaseInterface(ABC):
    """Abstract base class for database operations"""

    @abstractmethod
    def connect(self) -> None:
        """Establish database connection"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection"""
        pass

    @abstractmethod
    def create_table(self, table_name: str, fields_dict: Dict[str, str], primary_key: str) -> None:
        """Create table with specified schema"""
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        pass

    @abstractmethod
    def add_column(self, table_name: str, column_name: str, column_type: str) -> None:
        """Add column to existing table"""
        pass

    @abstractmethod
    def insert_records(self, table_name: str, records: List[Dict[str, Any]],
                       fields_dict: Dict[str, str], primary_key: str,
                       modified_field: str) -> Tuple[int, int, int]:
        """
        Insert or update records
        Returns: (insert_count, update_count, skip_count)
        """
        pass

    @abstractmethod
    def insert_single_record(self, table_name: str, record: Dict[str, Any],
                             fields_dict: Dict[str, str], primary_key: str) -> None:
        """Insert or replace single record"""
        pass

    @abstractmethod
    def delete_records(self, table_name: str, where_field: str, where_value: Any) -> int:
        """Delete records matching condition"""
        pass

    @abstractmethod
    def get_record_count(self, table_name: str) -> int:
        """Get total record count for table"""
        pass

    @abstractmethod
    def get_last_sync_time(self, table_name: str) -> Optional[str]:
        """Get last successful sync time for table"""
        pass

    @abstractmethod
    def update_sync_timestamp(self, table_name: str, duration: Optional[float] = None,
                              status: str = "SUCCESS", error_message: Optional[str] = None) -> None:
        """Update sync log with latest sync information"""
        pass

    @abstractmethod
    def initialize_sync_schedule(self, schedules: List[Tuple]) -> None:
        """Initialize sync schedule table with default values"""
        pass

    @abstractmethod
    def get_sync_schedule(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get sync schedule for specific table"""
        pass

    @abstractmethod
    def track_custom_fields(self, table_name: str, header_fields: Set[str],
                            line_fields: Optional[Set[str]] = None) -> None:
        """Track discovered custom fields"""
        pass

    @abstractmethod
    def get_known_custom_fields(self, table_name: str) -> Tuple[Set[str], Set[str]]:
        """Get known custom fields for table"""
        pass

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute raw SQL query"""
        pass

    @abstractmethod
    def begin_transaction(self) -> None:
        """Begin database transaction"""
        pass

    @abstractmethod
    def commit_transaction(self) -> None:
        """Commit database transaction"""
        pass

    @abstractmethod
    def rollback_transaction(self) -> None:
        """Rollback database transaction"""
        pass

    # Price extraction specific methods
    @abstractmethod
    def save_customer_prices(self, price_records: List[Dict[str, Any]]) -> None:
        """Save customer-specific pricing data"""
        pass

    @abstractmethod
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Get list of all tables with basic info"""
        pass

    @abstractmethod
    def verify_database(self) -> Dict[str, Any]:
        """Verify database integrity and return stats"""
        pass

    # Metadata bug tracking methods
    @abstractmethod
    def initialize_metadata_bug_tracker(self) -> None:
        """Initialize the metadata bug tracking table"""
        pass

    @abstractmethod
    def detect_orphaned_records(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Detect records with missing line items
        Returns list of dicts with: TxnID, RefNumber, EditSequence, Amount
        """
        pass

    @abstractmethod
    def get_fix_attempt_status(self, txn_id: str, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the fix attempt status for a specific record
        Returns dict with: AttemptCount, Status, LastAttemptDate, etc.
        """
        pass

    @abstractmethod
    def record_fix_attempt(self, txn_id: str, table_name: str,
                          success: bool, error_message: Optional[str] = None) -> None:
        """Record a fix attempt for a specific transaction"""
        pass

    @abstractmethod
    def get_failed_fix_attempts(self) -> List[Dict[str, Any]]:
        """Get all records that failed after 3 attempts"""
        pass


class SyncStatus:
    """Constants for sync status values"""
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    LOCKED = "LOCKED"
    BUSY = "BUSY"
    EDITING = "EDITING"
    NOT_FOUND = "NOT_FOUND"
    NO_CONNECTION = "NO_CONNECTION"


class FieldTypes:
    """SQL field type constants"""
    TEXT = "TEXT"
    INTEGER = "INTEGER"
    REAL = "REAL"
    BLOB = "BLOB"

    @staticmethod
    def determine_type(value: Any, current_types: Set[str]) -> str:
        """Determine SQL type for a value"""
        if value is None or value == '':
            current_types.add(FieldTypes.TEXT)
        elif isinstance(value, bool):
            current_types.add(FieldTypes.INTEGER)
        elif isinstance(value, int):
            current_types.add(FieldTypes.INTEGER)
        elif isinstance(value, float):
            current_types.add(FieldTypes.REAL)
        elif isinstance(value, str):
            if value.lower() in ['true', 'false']:
                current_types.add(FieldTypes.INTEGER)
            elif FieldTypes._is_int_str(value):
                current_types.add(FieldTypes.INTEGER)
            elif FieldTypes._is_float_str(value):
                current_types.add(FieldTypes.REAL)
            else:
                current_types.add(FieldTypes.TEXT)
        else:
            current_types.add(FieldTypes.TEXT)

        # Resolve to single type
        if FieldTypes.TEXT in current_types:
            return FieldTypes.TEXT
        elif FieldTypes.REAL in current_types:
            return FieldTypes.REAL
        elif FieldTypes.INTEGER in current_types:
            return FieldTypes.INTEGER
        else:
            return FieldTypes.TEXT

    @staticmethod
    def _is_int_str(s: str) -> bool:
        """Check if string represents an integer"""
        try:
            int(s)
            return True
        except ValueError:
            return False

    @staticmethod
    def _is_float_str(s: str) -> bool:
        """Check if string represents a float"""
        try:
            float(s)
            return '.' in s or 'e' in s.lower()
        except ValueError:
            return False


class MetadataBugStatus:
    """Constants for metadata bug fix status"""
    PENDING = "PENDING"
    FIXED = "FIXED"
    FAILED = "FAILED"