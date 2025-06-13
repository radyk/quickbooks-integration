"""
QuickBooks Sync Configuration Module
Provides all configuration settings with optional override capability
"""
import os
import json
from pathlib import Path

# Default configuration (hardcoded)
DEFAULT_CONFIG = {
    'database': {
        'type': 'sqlite',  # Future options: 'sqlexpress'
        'sqlite': {
            'path': r'C:\QuickBooksSync\data\quickbooks_data.db'
        },
        'sqlexpress': {
            'server': 'localhost\\SQLEXPRESS',
            'database': 'QuickBooksData',
            'trusted_connection': True,
            'driver': 'ODBC Driver 17 for SQL Server'
        }
    },
    'quickbooks': {
        'company_file': r"C:\Users\Public\Documents\Intuit\QuickBooks\Company Files\Fromm Packaging Systems Canada Inc..QBW",
        'app_name': "Fromm Packaging QuickBooks Integration",
        'qbfc_version': 16,
        'connection_mode': 2,  # 2 = Multi-user mode
        'max_wait_seconds': 10
    },
    'logging': {
        'filename': 'quickbooks_sync.log',
        'level': 'DEBUG',
        'console_level': 'DEBUG',
        'format': '%(asctime)s - %(levelname)s - %(message)s'
    },
    'sync': {
        'auto_analyze_open_orders': True,
        'batch_size': 100,
        'max_retries': 3,
        'retry_delay': 60,
        'enable_price_extraction': False,
        'price_extraction_interval_days': 7,
        'price_extraction_test_mode': False,
        'price_extraction_test_limits': {
            'customers': 1,
            'items': 500
        }
    }
}

def deep_merge(base_dict, override_dict):
    """Recursively merge override_dict into base_dict"""
    result = base_dict.copy()
    for key, value in override_dict.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result

# Load configuration
def load_config():
    """Load configuration with optional overrides from JSON file"""
    config = DEFAULT_CONFIG.copy()

    # Check for override file
    config_file = os.environ.get('QBSYNC_CONFIG', 'qbsync_config.json')
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                overrides = json.load(f)
                config = deep_merge(config, overrides)
                print(f"Loaded configuration overrides from {config_file}")
        except Exception as e:
            print(f"Warning: Could not load config file {config_file}: {e}")

    return config

# Load config on module import
CONFIG = load_config()

# Export individual config sections for convenience
DATABASE_CONFIG = CONFIG['database']
QB_CONFIG = CONFIG['quickbooks']
LOGGING_CONFIG = CONFIG['logging']
SYNC_CONFIG = CONFIG['sync']

# Ensure directory for database exists (SQLite only)
if DATABASE_CONFIG['type'] == 'sqlite':
    db_path = Path(DATABASE_CONFIG['sqlite']['path'])
    db_dir = db_path.parent
    if not db_dir.exists():
        db_dir.mkdir(parents=True, exist_ok=True)

# Table configurations - these define the schema and should not be user-configurable
TABLE_CONFIGS = [
    # Transactions
    {
        "name": "invoices",
        "query_fn_name": "AppendInvoiceQueryRq",
        "xml_tag": "InvoiceRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORInvoiceLineRetList",
        "specific_line_ret_prop_name": "InvoiceLineRet",
        "table_type": "transaction"
    },
    {
        "name": "sales_orders",
        "query_fn_name": "AppendSalesOrderQueryRq",
        "xml_tag": "SalesOrderRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORSalesOrderLineRetList",
        "specific_line_ret_prop_name": "SalesOrderLineRet",
        "table_type": "transaction"
    },
    {
        "name": "sales_receipts",
        "query_fn_name": "AppendSalesReceiptQueryRq",
        "xml_tag": "SalesReceiptRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORSalesReceiptLineRetList",
        "specific_line_ret_prop_name": "SalesReceiptLineRet",
        "table_type": "transaction"
    },
    {
        "name": "estimates",
        "query_fn_name": "AppendEstimateQueryRq",
        "xml_tag": "EstimateRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "OREstimateLineRetList",
        "specific_line_ret_prop_name": "EstimateLineRet",
        "table_type": "transaction"
    },
    {
        "name": "credit_memos",
        "query_fn_name": "AppendCreditMemoQueryRq",
        "xml_tag": "CreditMemoRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORCreditMemoLineRetList",
        "specific_line_ret_prop_name": "CreditMemoLineRet",
        "table_type": "transaction"
    },
    {
        "name": "bills",
        "query_fn_name": "AppendBillQueryRq",
        "xml_tag": "BillRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "table_type": "transaction",
        "special_handling": "bill_lines"  # Has multiple line types
    },
    {
        "name": "purchase_orders",
        "query_fn_name": "AppendPurchaseOrderQueryRq",
        "xml_tag": "PurchaseOrderRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORPurchaseOrderLineRetList",
        "specific_line_ret_prop_name": "PurchaseOrderLineRet",
        "table_type": "transaction"
    },
    {
        "name": "item_receipts",
        "query_fn_name": "AppendItemReceiptQueryRq",
        "xml_tag": "ItemReceiptRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORItemReceiptLineRetList",
        "specific_line_ret_prop_name": "ItemReceiptLineRet",
        "table_type": "transaction"
    },
    {
        "name": "receive_payments",
        "query_fn_name": "AppendReceivePaymentQueryRq",
        "xml_tag": "ReceivePaymentRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "transaction"
    },
    {
        "name": "bill_payment_checks",
        "query_fn_name": "AppendBillPaymentCheckQueryRq",
        "xml_tag": "BillPaymentCheckRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORAppliedToTxnRetList",
        "specific_line_ret_prop_name": "AppliedToTxnRet",
        "table_type": "transaction"
    },
    {
        "name": "bill_payment_credit_cards",
        "query_fn_name": "AppendBillPaymentCreditCardQueryRq",
        "xml_tag": "BillPaymentCreditCardRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORAppliedToTxnRetList",
        "specific_line_ret_prop_name": "AppliedToTxnRet",
        "table_type": "transaction"
    },
    {
        "name": "checks",
        "query_fn_name": "AppendCheckQueryRq",
        "xml_tag": "CheckRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORExpenseLineRetList",
        "specific_line_ret_prop_name": "ExpenseLineRet",
        "table_type": "transaction"
    },
    {
        "name": "deposits",
        "query_fn_name": "AppendDepositQueryRq",
        "xml_tag": "DepositRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "DepositLineRetList",
        "specific_line_ret_prop_name": None,  # Direct line items
        "table_type": "transaction"
    },
    {
        "name": "transfers",
        "query_fn_name": "AppendTransferQueryRq",
        "xml_tag": "TransferRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "transaction"
    },
    {
        "name": "credit_card_charges",
        "query_fn_name": "AppendCreditCardChargeQueryRq",
        "xml_tag": "CreditCardChargeRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORExpenseLineRetList",
        "specific_line_ret_prop_name": "ExpenseLineRet",
        "table_type": "transaction"
    },
    {
        "name": "credit_card_credits",
        "query_fn_name": "AppendCreditCardCreditQueryRq",
        "xml_tag": "CreditCardCreditRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORExpenseLineRetList",
        "specific_line_ret_prop_name": "ExpenseLineRet",
        "table_type": "transaction"
    },
    {
        "name": "journal_entries",
        "query_fn_name": "AppendJournalEntryQueryRq",
        "xml_tag": "JournalEntryRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORJournalLineList",
        "specific_line_ret_prop_name": "JournalLine",
        "table_type": "transaction"
    },
    {
        "name": "inventory_adjustments",
        "query_fn_name": "AppendInventoryAdjustmentQueryRq",
        "xml_tag": "InventoryAdjustmentRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ORInventoryAdjustmentLineRetList",
        "specific_line_ret_prop_name": "InventoryAdjustmentLineRet",
        "table_type": "transaction"
    },
    {
        "name": "build_assemblies",
        "query_fn_name": "AppendBuildAssemblyQueryRq",
        "xml_tag": "BuildAssemblyRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": True,
        "or_line_list_prop_name": "ComponentItemLineRetList",
        "specific_line_ret_prop_name": None,  # Direct line items
        "table_type": "transaction"
    },
    {
        "name": "time_trackings",
        "query_fn_name": "AppendTimeTrackingQueryRq",
        "xml_tag": "TimeTrackingRet",
        "key_field": "TxnID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "transaction"
    },

    # Lists
    {
        "name": "customers",
        "query_fn_name": "AppendCustomerQueryRq",
        "xml_tag": "CustomerRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "vendors",
        "query_fn_name": "AppendVendorQueryRq",
        "xml_tag": "VendorRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "employees",
        "query_fn_name": "AppendEmployeeQueryRq",
        "xml_tag": "EmployeeRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "other_names",
        "query_fn_name": "AppendOtherNameQueryRq",
        "xml_tag": "OtherNameRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "accounts",
        "query_fn_name": "AppendAccountQueryRq",
        "xml_tag": "AccountRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_all_types",
        "query_fn_name": "AppendItemQueryRq",
        "xml_tag": "ItemRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_inventory",
        "query_fn_name": "AppendItemInventoryQueryRq",
        "xml_tag": "ItemInventoryRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_service",
        "query_fn_name": "AppendItemServiceQueryRq",
        "xml_tag": "ItemServiceRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_noninventory",
        "query_fn_name": "AppendItemNonInventoryQueryRq",
        "xml_tag": "ItemNonInventoryRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_fixed_asset",
        "query_fn_name": "AppendItemFixedAssetQueryRq",
        "xml_tag": "ItemFixedAssetRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_other_charge",
        "query_fn_name": "AppendItemOtherChargeQueryRq",
        "xml_tag": "ItemOtherChargeRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_discount",
        "query_fn_name": "AppendItemDiscountQueryRq",
        "xml_tag": "ItemDiscountRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_group",
        "query_fn_name": "AppendItemGroupQueryRq",
        "xml_tag": "ItemGroupRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_inventory_assembly",
        "query_fn_name": "AppendItemInventoryAssemblyQueryRq",
        "xml_tag": "ItemInventoryAssemblyRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_sales_tax",
        "query_fn_name": "AppendItemSalesTaxQueryRq",
        "xml_tag": "ItemSalesTaxRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_sales_tax_group",
        "query_fn_name": "AppendItemSalesTaxGroupQueryRq",
        "xml_tag": "ItemSalesTaxGroupRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "items_payment",
        "query_fn_name": "AppendItemPaymentQueryRq",
        "xml_tag": "ItemPaymentRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "classes",
        "query_fn_name": "AppendClassQueryRq",
        "xml_tag": "ClassRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "customer_types",
        "query_fn_name": "AppendCustomerTypeQueryRq",
        "xml_tag": "CustomerTypeRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "vendor_types",
        "query_fn_name": "AppendVendorTypeQueryRq",
        "xml_tag": "VendorTypeRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "terms",
        "query_fn_name": "AppendTermsQueryRq",
        "xml_tag": "TermsRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list",
        "special_handling": "terms"  # Has OR structure
    },
    {
        "name": "shipping_methods",
        "query_fn_name": "AppendShipMethodQueryRq",
        "xml_tag": "ShipMethodRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },
    {
        "name": "sales_tax_codes",
        "query_fn_name": "AppendSalesTaxCodeQueryRq",
        "xml_tag": "SalesTaxCodeRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "list"
    },

    # Special tables
    {
        "name": "qb_txn_deleted_data",
        "query_fn_name": "AppendTxnDeletedQueryRq",
        "xml_tag": "TxnDeletedRet",
        "key_field": "TxnID",
        "modified_field": "TimeDeleted",
        "has_line_items": False,
        "table_type": "special",
        "special_handling": "transaction_deletions"
    },
    {
        "name": "qb_list_deleted_data",
        "query_fn_name": "AppendListDeletedQueryRq",
        "xml_tag": "ListDeletedRet",
        "key_field": "ListID",
        "modified_field": "TimeDeleted",
        "has_line_items": False,
        "table_type": "special",
        "special_handling": "list_deletions"
    },
    {
        "name": "company_info",
        "query_fn_name": "AppendCompanyQueryRq",
        "xml_tag": "CompanyRet",
        "key_field": "ListID",
        "modified_field": "TimeModified",
        "has_line_items": False,
        "table_type": "special"
    }
]

# Sync schedule defaults
SYNC_SCHEDULE_DEFAULTS = [
    # High-frequency transaction tables
    ('invoices', 60, 360, 720, 1),
    ('sales_orders', 60, 360, 720, 1),
    ('bills', 60, 360, 720, 1),
    ('purchase_orders', 60, 360, 720, 1),
    ('deposits', 60, 360, 720, 2),
    ('receive_payments', 60, 360, 720, 2),

    # Medium-frequency transaction tables
    ('bill_payment_checks', 120, 720, 1440, 3),
    ('bill_payment_credit_cards', 120, 720, 1440, 3),
    ('sales_receipts', 120, 720, 1440, 3),
    ('credit_memos', 120, 720, 1440, 3),

    # Low-frequency transaction tables
    ('estimates', 180, 720, 1440, 4),
    ('checks', 180, 720, 1440, 4),
    ('transfers', 180, 720, 1440, 4),
    ('journal_entries', 180, 720, 1440, 4),

    # Inventory/Build tables
    ('inventory_adjustments', 120, 720, 1440, 3),
    ('build_assemblies', 120, 720, 1440, 3),
    ('item_receipts', 120, 720, 1440, 3),

    # Master data - less frequent
    ('customers', 240, 1440, 2880, 5),
    ('vendors', 240, 1440, 2880, 5),
    ('items_inventory', 240, 1440, 2880, 5),
    ('accounts', 360, 1440, 2880, 6),

    # Reference data - infrequent
    ('terms', 720, None, None, 7),
    ('shipping_methods', 720, None, None, 7),
    ('sales_tax_codes', 720, None, None, 7),
    ('classes', 720, None, None, 7),

    # Deletion tracking
    ('qb_txn_deleted_data', 360, 1440, 2880, 6),
    ('qb_list_deleted_data', 360, 1440, 2880, 6),

    # Price extraction - special weekly task
    ('customer_price_pages', 10080, None, 10080, 9),  # 10080 min = 1 week

    # Global settings row
    ('_GLOBAL_', 60, 360, 720, 0)
]