"""
Utility functions for QuickBooks sync
"""
import datetime
import pywintypes
import logging
from typing import Any, Optional, Dict, Set


def get_com_value(com_obj: Any, prop_name: str) -> Any:
    """
    Safely get a value from a COM object's property

    Args:
        com_obj: COM object
        prop_name: Property name

    Returns:
        Property value or None
    """
    try:
        if hasattr(com_obj, prop_name):
            prop = getattr(com_obj, prop_name)
            if hasattr(prop, 'GetValue'):
                return prop.GetValue()
            elif prop is not None:
                return prop
        return None
    except pywintypes.com_error as ce:
        logging.debug(
            f"COM error getting value for {prop_name}: "
            f"{ce.excepinfo[2] if ce.excepinfo else str(ce)}"
        )
        return None
    except Exception as e:
        logging.debug(f"Error getting value for {prop_name}: {e}")
        return None


def convert_com_datetime(com_datetime: Any) -> Optional[str]:
    """
    Convert COM datetime to ISO string

    Args:
        com_datetime: COM datetime object

    Returns:
        ISO formatted datetime string
    """
    if com_datetime is None:
        return None

    try:
        if hasattr(com_datetime, 'year'):  # pywintypes.datetime
            return datetime.datetime(
                com_datetime.year,
                com_datetime.month,
                com_datetime.day,
                com_datetime.hour,
                com_datetime.minute,
                com_datetime.second,
                com_datetime.microsecond if hasattr(com_datetime, 'microsecond') else 0
            ).isoformat()
        else:
            return str(com_datetime)
    except Exception as e:
        logging.warning(f"Could not convert COM datetime: {e}")
        return str(com_datetime) if com_datetime else None


def format_datetime_for_qb(dt_str: str, as_date_only: bool = False) -> str:
    """
    Format datetime string for QuickBooks

    Args:
        dt_str: ISO format datetime string
        as_date_only: If True, format as date only

    Returns:
        Formatted datetime string for QB
    """
    try:
        dt = datetime.datetime.fromisoformat(dt_str.replace('Z', '+00:00'))

        if as_date_only:
            return dt.strftime('%Y-%m-%d')
        else:
            return dt.strftime('%m/%d/%Y %H:%M:%S')
    except ValueError as e:
        logging.error(f"Error formatting datetime {dt_str}: {e}")
        raise


def create_pywin_time(dt_str: str, as_date_only: bool = False) -> pywintypes.Time:
    """
    Create pywintypes.Time object from datetime string

    Args:
        dt_str: ISO format datetime string
        as_date_only: If True, set time to midnight

    Returns:
        pywintypes.Time object
    """
    try:
        dt = datetime.datetime.fromisoformat(dt_str.replace('Z', '+00:00'))

        if as_date_only:
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

        return pywintypes.Time(dt)
    except ValueError as e:
        logging.error(f"Error creating pywintypes.Time from {dt_str}: {e}")
        raise


def is_date_iso_str(s: str) -> bool:
    """Check if string is ISO format date/datetime"""
    if not isinstance(s, str) or len(s) < 10:
        return False

    try:
        if 'T' in s:
            datetime.datetime.fromisoformat(s.replace('Z', '+00:00'))
        else:
            datetime.date.fromisoformat(s)
        return True
    except ValueError:
        return False


def clean_field_name(field_name: str, prefix: str = "") -> str:
    """
    Clean field name for use as database column

    Args:
        field_name: Raw field name
        prefix: Optional prefix to add

    Returns:
        Cleaned field name
    """
    # Add prefix if provided
    if prefix:
        field_name = prefix + field_name

    # Replace non-alphanumeric with underscore
    cleaned = ''.join(c if c.isalnum() else '_' for c in field_name)

    # Remove multiple underscores
    cleaned = '_'.join(filter(None, cleaned.split('_')))

    return cleaned


def get_transaction_type_description(type_number: int) -> str:
    """Get transaction type description from number"""
    type_mapping = {
        1: "Bill",
        2: "BillPaymentCheck",
        3: "BillPaymentCreditCard",
        4: "BuildAssembly",
        5: "Charge",
        6: "Check",
        7: "CreditCardCharge",
        8: "CreditCardCredit",
        9: "CreditMemo",
        10: "Deposit",
        11: "Estimate",
        12: "InventoryAdjustment",
        13: "Invoice",
        14: "ItemReceipt",
        15: "JournalEntry",
        16: "PurchaseOrder",
        17: "ReceivePayment",
        18: "SalesOrder",
        19: "SalesReceipt",
        20: "SalesTaxPaymentCheck",
        21: "Transfer",
        22: "VendorCredit",
        23: "YTDAdjustment",
        24: "TimeTracking"
    }
    return type_mapping.get(type_number, f"TxnType_{type_number}")


def get_list_type_description(type_number: int) -> str:
    """Get list type description from number"""
    type_mapping = {
        1: "Customer",
        2: "Vendor",
        3: "Employee",
        4: "OtherName",
        5: "Account",
        6: "BillingRate",
        7: "Class",
        8: "Currency",
        9: "CustomerMsg",
        10: "CustomerType",
        11: "DateDrivenTerms",
        12: "JobType",
        13: "Item",
        14: "ItemInventory",
        15: "ItemService",
        16: "ItemNonInventory",
        17: "ItemFixedAsset",
        18: "ItemOtherCharge",
        19: "ItemSubtotal",
        20: "ItemDiscount",
        21: "ItemPayment",
        22: "ItemGroup",
        23: "ItemSalesTax",
        24: "ItemSalesTaxGroup"
    }
    return type_mapping.get(type_number, f"ListType_{type_number}")


def determine_field_types(records: list, current_types: Dict[str, Set[str]]) -> None:
    """
    Determine field types from records

    Args:
        records: List of record dictionaries
        current_types: Dictionary to update with field types
    """
    for record in records:
        for field, value in record.items():
            if value is None or value == '':
                current_types[field].add('TEXT')
            elif isinstance(value, bool):
                current_types[field].add('INTEGER')
            elif isinstance(value, int):
                current_types[field].add('INTEGER')
            elif isinstance(value, float):
                current_types[field].add('REAL')
            elif isinstance(value, str):
                if value.lower() in ['true', 'false']:
                    current_types[field].add('INTEGER')
                elif _is_int_str(value):
                    current_types[field].add('INTEGER')
                elif _is_float_str(value):
                    current_types[field].add('REAL')
                elif is_date_iso_str(value):
                    current_types[field].add('TEXT')
                else:
                    current_types[field].add('TEXT')
            else:
                current_types[field].add('TEXT')


def resolve_field_types(field_names: Set[str], field_types: Dict[str, Set[str]]) -> Dict[str, str]:
    """
    Resolve field types to single type per field

    Args:
        field_names: Set of all field names
        field_types: Dictionary of field names to sets of possible types

    Returns:
        Dictionary of field names to resolved types
    """
    resolved = {}

    for field in field_names:
        types = field_types.get(field, {'TEXT'})
        if not types:
            types = {'TEXT'}

        # Priority: TEXT > REAL > INTEGER
        if 'TEXT' in types:
            resolved[field] = 'TEXT'
        elif 'REAL' in types:
            resolved[field] = 'REAL'
        elif 'INTEGER' in types:
            resolved[field] = 'INTEGER'
        else:
            resolved[field] = 'TEXT'

    return resolved


def _is_int_str(s: str) -> bool:
    """Check if string represents an integer"""
    try:
        int(s)
        return True
    except ValueError:
        return False


def _is_float_str(s: str) -> bool:
    """Check if string represents a float"""
    try:
        float(s)
        return '.' in s or 'e' in s.lower()
    except ValueError:
        return False


def log_com_error(error: Exception, context: str) -> None:
    """
    Log COM error with detailed information

    Args:
        error: The exception
        context: Context where error occurred
    """
    if isinstance(error, pywintypes.com_error):
        hresult = getattr(error, 'hresult', 0)

        if error.excepinfo and len(error.excepinfo) > 2:
            error_msg = error.excepinfo[2]
        else:
            error_msg = str(error)

        logging.error(
            f"COM Error in {context}: "
            f"HRESULT={hresult} (0x{hresult:08X}), "
            f"Message: {error_msg}"
        )

        # Check for specific error types
        if hresult == -2147220472 or "0x80040408" in str(error):
            logging.info("This indicates QuickBooks is busy")
        elif hresult == -2147220467:
            logging.critical("This indicates an invalid session ticket - restart required")
    else:
        logging.error(f"Error in {context}: {str(error)}", exc_info=True)