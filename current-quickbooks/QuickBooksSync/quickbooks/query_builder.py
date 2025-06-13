"""
QuickBooks query builder and filter application - FIXED for Active/Inactive Items
"""
import logging
import datetime
import pywintypes
from typing import Optional, Dict, Any

from utils import format_datetime_for_qb, create_pywin_time


class QueryBuilder:
    """Builds and configures QuickBooks queries"""

    def __init__(self):
        self.filter_paths = self._initialize_filter_paths()

    def _initialize_filter_paths(self) -> Dict[str, Dict[str, Any]]:
        """Initialize known filter paths for different query types"""
        return {
            # Transaction queries with standard filter structure
            "invoices": {
                "or_query": "ORInvoiceQuery",
                "filter": "InvoiceFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "sales_orders": {
                "or_query": "ORTxnNoAccountQuery",
                "filter": "TxnFilterNoAccount",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "sales_receipts": {
                "or_query": "ORSalesReceiptQuery",
                "filter": "SalesReceiptFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "estimates": {
                "or_query": "OREstimateQuery",
                "filter": "EstimateFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "credit_memos": {
                "or_query": "ORCreditMemoQuery",
                "filter": "CreditMemoFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "bills": {
                "or_query": "ORBillQuery",
                "filter": "BillFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "purchase_orders": {
                "or_query": "ORPurchaseOrderQuery",
                "filter": "PurchaseOrderFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "item_receipts": {
                "or_query": "ORItemReceiptQuery",
                "filter": "ItemReceiptFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "receive_payments": {
                "or_query": "ORReceivePaymentQuery",
                "filter": "ReceivePaymentFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "bill_payment_checks": {
                "or_query": "ORBillPaymentCheckQuery",
                "filter": "BillPaymentCheckFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "bill_payment_credit_cards": {
                "or_query": "ORBillPaymentCreditCardQuery",
                "filter": "BillPaymentCreditCardFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "checks": {
                "or_query": "ORCheckQuery",
                "filter": "CheckFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "deposits": {
                "or_query": "ORDepositQuery",
                "filter": "DepositFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "transfers": {
                "or_query": "ORTransferTxnQuery",
                "filter": "TransferTxnFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "credit_card_charges": {
                "or_query": "ORCreditCardChargeQuery",
                "filter": "CreditCardChargeFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "credit_card_credits": {
                "or_query": "ORCreditCardCreditQuery",
                "filter": "CreditCardCreditFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "journal_entries": {
                "or_query": "ORJournalEntryQuery",
                "filter": "JournalEntryFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "inventory_adjustments": {
                "or_query": "ORInventoryAdjustmentQuery",
                "filter": "TxnFilterWithItemFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "build_assemblies": {
                "or_query": "ORBuildAssemblyQuery",
                "filter": "BuildAssemblyFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },
            "time_trackings": {
                "or_query": "ORTimeTrackingTxnQuery",
                "filter": "TimeTrackingTxnFilter",
                "date_path": "ORDateRangeFilter.ModifiedDateRangeFilter"
            },

            # List queries with direct date fields
            "customers": {
                "or_query": "ORCustomerListQuery",
                "filter": "CustomerListFilter",
                "date_field": "FromModifiedDate"
            },
            "vendors": {
                "or_query": "ORVendorListQuery",
                "filter": "VendorListFilter",
                "date_field": "FromModifiedDate"
            },
            "employees": {
                "or_query": "OREmployeeListQuery",
                "filter": "EmployeeListFilter",
                "date_field": "FromModifiedDate"
            },
            "other_names": {
                "or_query": "OROtherNameListQuery",
                "filter": "OtherNameListFilter",
                "date_field": "FromModifiedDate"
            },
            "accounts": {
                "or_query": "ORAccountListQuery",
                "filter": "AccountListFilter",
                "date_field": "FromModifiedDate"
            },
            "classes": {
                "or_query": "ORClassListQuery",
                "filter": "ClassListFilter",
                "date_field": "FromModifiedDate"
            },
            "customer_types": {
                "or_query": "ORCustomerTypeListQuery",
                "filter": "CustomerTypeListFilter",
                "date_field": "FromModifiedDate"
            },
            "vendor_types": {
                "or_query": "ORVendorTypeListQuery",
                "filter": "VendorTypeListFilter",
                "date_field": "FromModifiedDate"
            },
            "terms": {
                "or_query": "ORTermsListQuery",
                "filter": "TermsListFilter",
                "date_field": "FromModifiedDate"
            },
            "shipping_methods": {
                "or_query": "ORShipMethodListQuery",
                "filter": "ShipMethodListFilter",
                "date_field": "FromModifiedDate"
            },
            "sales_tax_codes": {
                "or_query": "ORSalesTaxCodeListQuery",
                "filter": "SalesTaxCodeListFilter",
                "date_field": "FromModifiedDate"
            },

            # Special queries
            "qb_txn_deleted_data": {
                "special": "transaction_deletions"
            },
            "qb_list_deleted_data": {
                "special": "list_deletions"
            },
            "company_info": {
                "special": "company_info"
            }
        }

    def configure_query(self, query_obj: Any, table_config: Dict[str, Any],
                        last_sync_time: Optional[str] = None) -> bool:
        """
        Configure query object with filters and options
        """
        table_name = table_config["name"]
        has_line_items = table_config.get("has_line_items", False)

        # Configure basic options
        self._configure_basic_options(query_obj, has_line_items, table_name)

        # Apply date filter if needed
        filter_applied = True
        if last_sync_time:
            filter_applied = self.apply_date_filter(query_obj, last_sync_time, table_name)
            if not filter_applied:
                logging.warning(f"Date filter not applied for {table_name}")

        # Configure special queries
        self._configure_special_queries(query_obj, table_name)

        return filter_applied

    def _configure_basic_options(self, query_obj: Any, has_line_items: bool,
                                 table_name: str) -> None:
        """Configure basic query options"""

        # Include line items
        if has_line_items and hasattr(query_obj, 'IncludeLineItems'):
            query_obj.IncludeLineItems.SetValue(True)
            logging.debug(f"Set IncludeLineItems=True for {table_name}")

        # Configure ActiveStatus for item queries to include inactive items
        item_tables = [
            'items_inventory',
            'items_service',
            'items_noninventory',
            'items_fixed_asset',
            'items_other_charge',
            'items_discount',
            'items_group',
            'items_inventory_assembly',
            'items_sales_tax',
            'items_sales_tax_group',
            'items_payment',
            'items_all_types'  # Don't forget the general item query
        ]

        if table_name in item_tables:
            # For item queries, ActiveStatus is nested in the ORListQueryWithOwnerIDAndClass structure
            applied = False

            # Method 1: Try the ORListQueryWithOwnerIDAndClass path (most common for item queries)
            if hasattr(query_obj, 'ORListQueryWithOwnerIDAndClass'):
                try:
                    or_list = query_obj.ORListQueryWithOwnerIDAndClass
                    if hasattr(or_list, 'ListWithClassFilter'):
                        filter_obj = or_list.ListWithClassFilter
                        if hasattr(filter_obj, 'ActiveStatus'):
                            filter_obj.ActiveStatus.SetValue(2)  # 2 = All (Active and Inactive)
                            logging.info(f"Set ActiveStatus=All for {table_name} via ORListQueryWithOwnerIDAndClass.ListWithClassFilter.ActiveStatus")
                            applied = True
                        else:
                            logging.debug(f"ActiveStatus not found on ListWithClassFilter for {table_name}")
                    else:
                        logging.debug(f"ListWithClassFilter not found for {table_name}")
                except Exception as e:
                    logging.error(f"Error setting ActiveStatus via ORListQueryWithOwnerIDAndClass: {e}")

            # Method 2: If not applied yet, try direct ActiveStatus on query object
            if not applied and hasattr(query_obj, 'ActiveStatus'):
                try:
                    query_obj.ActiveStatus.SetValue(2)
                    logging.info(f"Set ActiveStatus=All for {table_name} (direct property)")
                    applied = True
                except Exception as e:
                    logging.error(f"Could not set direct ActiveStatus for {table_name}: {e}")

            # If still not applied, log warning
            if not applied:
                logging.warning(f"Could not set ActiveStatus for {table_name} - will only get active items")

        # Include linked transactions for queries that support it
        linkedtxn_supported_tables = [
            'invoices',  # Shows linked payments, credit memos
            'bills',  # Shows linked bill payments, vendor credits
            'sales_receipts',  # Shows linked deposits
            'credit_memos',  # Shows what it was applied to
            'checks',  # Shows linked bills (if paying bills)
            'credit_card_charges',  # Shows linked bills
            'credit_card_credits',  # Shows linked bills
            'journal_entries',  # May show linked transactions
            'deposits'  # Shows linked payments/receipts
        ]

        if table_name in linkedtxn_supported_tables and hasattr(query_obj, 'IncludeLinkedTxns'):
            try:
                query_obj.IncludeLinkedTxns.SetValue(True)
                logging.info(f"Set IncludeLinkedTxns=True for {table_name}")
            except AttributeError as e:
                # Log but don't fail - some versions might not support it
                logging.debug(f"Could not set IncludeLinkedTxns for {table_name}: {e}")

        # Owner ID list (for certain queries)
        if hasattr(query_obj, 'OwnerIDList'):
            try:
                query_obj.OwnerIDList.Add("0")
                logging.debug(f"Added OwnerID=0 for {table_name}")
            except Exception as e:
                logging.debug(f"Could not add OwnerID for {table_name}: {e}")

    def _configure_special_queries(self, query_obj: Any, table_name: str) -> None:
        """Configure special query types"""
        if table_name == "qb_txn_deleted_data":
            if hasattr(query_obj, "TxnDelTypeList"):
                for txn_type in range(1, 25):
                    query_obj.TxnDelTypeList.Add(txn_type)
                logging.debug("Added transaction deletion types 1-24")

        elif table_name == "qb_list_deleted_data":
            if hasattr(query_obj, "ListDelTypeList"):
                for list_type in range(1, 25):
                    query_obj.ListDelTypeList.Add(list_type)
                logging.debug("Added list deletion types 1-24")

    def apply_date_filter(self, query_obj: Any, from_date: str, table_name: str) -> bool:
        """
        Apply date filter to query

        Args:
            query_obj: QuickBooks query object
            from_date: ISO format date string
            table_name: Table name for filter path lookup

        Returns:
            bool: True if filter was successfully applied
        """
        logging.debug(f"Attempting to apply date filter for {table_name} from {from_date}")

        try:
            # Special handling for deletion queries
            if table_name == "qb_txn_deleted_data":
                return self._apply_deletion_date_filter(query_obj, from_date, "DeletedDateRangeFilter")
            elif table_name == "qb_list_deleted_data":
                return self._apply_deletion_date_filter(query_obj, from_date, "DeletedDateRangeFilter")

            # Try direct FromModifiedDate first
            if self._try_direct_date_filter(query_obj, from_date):
                return True

            # Use filter path configuration
            filter_config = self.filter_paths.get(table_name, {})
            if filter_config and "special" not in filter_config:
                result = self._apply_configured_filter(query_obj, from_date, filter_config)
                if result:
                    return True
                # If configured filter didn't work, still try generic paths
                logging.debug(f"Configured filter failed for {table_name}, trying generic paths")

            # Try generic paths
            result = self._try_generic_filter_paths(query_obj, from_date)

            # If nothing worked, log what's available for debugging
            if not result:
                available_attrs = [attr for attr in dir(query_obj) if not attr.startswith('_') and not attr.isupper()]
                logging.debug(f"No date filter applied for {table_name}. Available query attributes: {available_attrs}")

            return result

        except Exception as e:
            logging.error(f"Error applying date filter for {table_name}: {e}", exc_info=True)
            return False

    def _apply_deletion_date_filter(self, query_obj: Any, from_date: str,
                                   filter_name: str) -> bool:
        """Apply date filter for deletion queries"""
        try:
            if hasattr(query_obj, filter_name):
                filter_obj = getattr(query_obj, filter_name)
                if hasattr(filter_obj, "FromDeletedDate"):
                    dt = datetime.datetime.fromisoformat(from_date.replace('Z', '+00:00'))
                    pywin_time = pywintypes.Time(dt)
                    filter_obj.FromDeletedDate.SetValue(pywin_time, True)
                    logging.debug(f"Applied deletion date filter: {from_date}")
                    return True
        except Exception as e:
            logging.error(f"Error setting deletion date filter: {e}")

        return True  # Return True for deletions even if filter fails

    def _try_direct_date_filter(self, query_obj: Any, from_date: str) -> bool:
        """Try to apply date filter directly on query object"""
        if hasattr(query_obj, "FromModifiedDate"):
            date_prop = query_obj.FromModifiedDate
            if hasattr(date_prop, "SetValue") or hasattr(date_prop, "setvalue"):
                return self._set_date_value(date_prop, from_date, "Direct")
        return False

    def _apply_configured_filter(self, query_obj: Any, from_date: str,
                                filter_config: Dict[str, Any]) -> bool:
        """Apply filter using configuration"""
        try:
            # Navigate to filter object
            current_obj = query_obj

            # Handle OR query structure (if specified)
            if "or_query" in filter_config:
                or_query_name = filter_config["or_query"]
                if hasattr(current_obj, or_query_name):
                    current_obj = getattr(current_obj, or_query_name)
                else:
                    # Log but don't return false - try without OR wrapper
                    logging.debug(f"{or_query_name} not found, trying direct filter approach")

            # Navigate to filter
            if "filter" in filter_config:
                filter_name = filter_config["filter"]
                if hasattr(current_obj, filter_name):
                    current_obj = getattr(current_obj, filter_name)
                else:
                    # Show available attributes for debugging
                    available_attrs = [attr for attr in dir(current_obj) if not attr.startswith('_') and not attr.isupper()]
                    logging.debug(f"{filter_name} not found on {type(current_obj).__name__}. Available: {available_attrs[:10]}")
                    return False

            # Apply date filter
            if "date_path" in filter_config:
                # Navigate nested path
                path_parts = filter_config["date_path"].split('.')
                for part in path_parts:
                    if hasattr(current_obj, part):
                        current_obj = getattr(current_obj, part)
                    else:
                        logging.debug(f"{part} not found in path {filter_config['date_path']}")
                        return False

                # Now current_obj should be ModifiedDateRangeFilter
                # Look for FromModifiedDate on this object
                if hasattr(current_obj, "FromModifiedDate"):
                    date_prop = current_obj.FromModifiedDate
                    return self._set_date_value(date_prop, from_date, filter_config["date_path"] + ".FromModifiedDate")

            elif "date_field" in filter_config:
                # Direct date field
                date_field_name = filter_config["date_field"]
                if hasattr(current_obj, date_field_name):
                    date_prop = getattr(current_obj, date_field_name)
                    return self._set_date_value(date_prop, from_date, date_field_name)

        except Exception as e:
            logging.error(f"Error applying configured filter: {e}", exc_info=True)

        return False

    def _try_generic_filter_paths(self, query_obj: Any, from_date: str) -> bool:
        """Try common generic filter paths"""
        # Direct transaction filter (no OR wrapper)
        if hasattr(query_obj, "TxnFilter"):
            if self._try_transaction_filter_path(query_obj, from_date, "Direct"):
                return True

        # Transaction paths with OR wrapper
        if hasattr(query_obj, "ORTxnQuery"):
            or_txn = query_obj.ORTxnQuery
            if self._try_transaction_filter_path(or_txn, from_date, "ORTxnQuery"):
                return True

        # Direct list filter (no OR wrapper)
        if hasattr(query_obj, "ListFilter"):
            if self._try_list_filter_path(query_obj, from_date, "Direct"):
                return True

        # List paths with OR wrapper
        if hasattr(query_obj, "ORListQuery"):
            or_list = query_obj.ORListQuery
            if self._try_list_filter_path(or_list, from_date, "ORListQuery"):
                return True

        # Item queries with ORListQueryWithOwnerIDAndClass
        if hasattr(query_obj, "ORListQueryWithOwnerIDAndClass"):
            or_list = query_obj.ORListQueryWithOwnerIDAndClass
            if self._try_item_filter_path(or_list, from_date):
                return True

        # Try direct date range filter (some queries might have this at top level)
        if hasattr(query_obj, "ORDateRangeFilter"):
            or_date = query_obj.ORDateRangeFilter
            if hasattr(or_date, "ModifiedDateRangeFilter"):
                mod_filter = or_date.ModifiedDateRangeFilter
                if hasattr(mod_filter, "FromModifiedDate"):
                    date_prop = mod_filter.FromModifiedDate
                    return self._set_date_value(
                        date_prop, from_date,
                        "ORDateRangeFilter.ModifiedDateRangeFilter"
                    )

        return False

    def _try_transaction_filter_path(self, or_query: Any, from_date: str,
                                    path_prefix: str) -> bool:
        """Try transaction filter path"""
        if hasattr(or_query, "TxnFilter"):
            txn_filter = or_query.TxnFilter
            if hasattr(txn_filter, "ORDateRangeFilter"):
                or_date = txn_filter.ORDateRangeFilter
                if hasattr(or_date, "ModifiedDateRangeFilter"):
                    mod_filter = or_date.ModifiedDateRangeFilter
                    if hasattr(mod_filter, "FromModifiedDate"):
                        date_prop = mod_filter.FromModifiedDate
                        return self._set_date_value(
                            date_prop, from_date,
                            f"{path_prefix}.TxnFilter.ORDateRangeFilter.ModifiedDateRangeFilter"
                        )
        return False

    def _try_list_filter_path(self, or_query: Any, from_date: str,
                             path_prefix: str) -> bool:
        """Try list filter path"""
        if hasattr(or_query, "ListFilter"):
            list_filter = or_query.ListFilter

            # Try direct FromModifiedDate
            if hasattr(list_filter, "FromModifiedDate"):
                date_prop = list_filter.FromModifiedDate
                if self._set_date_value(date_prop, from_date, f"{path_prefix}.ListFilter"):
                    return True

            # Try ModifiedDateRangeFilter
            if hasattr(list_filter, "ModifiedDateRangeFilter"):
                mod_filter = list_filter.ModifiedDateRangeFilter
                if hasattr(mod_filter, "FromModifiedDate"):
                    date_prop = mod_filter.FromModifiedDate
                    return self._set_date_value(
                        date_prop, from_date,
                        f"{path_prefix}.ListFilter.ModifiedDateRangeFilter"
                    )

        return False

    def _try_item_filter_path(self, or_query: Any, from_date: str) -> bool:
        """Try item-specific filter paths"""
        # Try ListWithClassFilter (found in testing)
        if hasattr(or_query, "ListWithClassFilter"):
            filter_obj = or_query.ListWithClassFilter
            if hasattr(filter_obj, "FromModifiedDate"):
                date_prop = filter_obj.FromModifiedDate
                if hasattr(date_prop, "SetValue") or hasattr(date_prop, "setvalue"):
                    return self._set_date_value(
                        date_prop, from_date,
                        "ORListQueryWithOwnerIDAndClass.ListWithClassFilter"
                    )

        # Try ItemFilter
        if hasattr(or_query, "ItemFilter"):
            return self._try_list_filter_path(or_query, from_date, "ItemFilter")

        return False

    def _set_date_value(self, date_prop: Any, date_str: str, path: str) -> bool:
        """Set date value on property"""
        if not hasattr(date_prop, "setvalue") and not hasattr(date_prop, "SetValue"):
            logging.debug(f"Date property at {path} has no set method")
            return False

        try:
            # Format date for QuickBooks
            formatted_date = format_datetime_for_qb(date_str, as_date_only=False)

            # Try lowercase setvalue first (more common)
            if hasattr(date_prop, "setvalue"):
                date_prop.setvalue(formatted_date, False)
            else:
                date_prop.SetValue(formatted_date, False)

            logging.info(f"Successfully set date filter at {path}: {formatted_date}")
            return True

        except TypeError:
            # Fallback to pywintypes.Time
            try:
                pywin_time = create_pywin_time(date_str, as_date_only=False)
                if hasattr(date_prop, "setvalue"):
                    date_prop.setvalue(pywin_time, False)
                else:
                    date_prop.SetValue(pywin_time, False)
                logging.info(f"Successfully set date filter at {path} (using pywintypes.Time)")
                return True
            except Exception as e:
                logging.error(f"Failed to set date filter at {path}: {e}")

        except Exception as e:
            logging.error(f"Error setting date filter at {path}: {e}")

        return False