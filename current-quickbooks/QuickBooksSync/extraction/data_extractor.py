"""
Data extraction from QuickBooks COM objects
"""
import logging
from typing import Dict, Any, Set, List, Optional, Tuple
from collections import defaultdict

from utils import (
    get_com_value,
    convert_com_datetime,
    clean_field_name,
    get_transaction_type_description,
    get_list_type_description
)


class DataExtractor:
    """Handles extraction of data from QuickBooks COM objects"""

    def extract_header_data(self, com_record: Any, table_name: str,
                            known_fields: Set[str]) -> Tuple[Dict[str, Any], Set[str]]:
        """
        Extract header data from COM record

        Args:
            com_record: COM record object
            table_name: Name of the table
            known_fields: Set of known field names

        Returns:
            Tuple of (data_dict, updated_fields_set)
        """
        # Special handling for different record types
        if table_name == "qb_txn_deleted_data":
            return self._extract_transaction_deletion_data(com_record, known_fields)
        elif table_name == "qb_list_deleted_data":
            return self._extract_list_deletion_data(com_record, known_fields)
        elif table_name == "terms":
            return self._extract_terms_data(com_record, known_fields)
        elif table_name == "items_all_types":  # ADD THIS LINE
            return self._extract_item_all_types_data(com_record, known_fields)  # ADD THIS LINE
        else:
            # Standard extraction
            return self.extract_com_record_data(com_record, known_fields, is_line_item=False)



    def _extract_item_all_types_data(self, com_record: Any,
                                     known_fields: Set[str]) -> Tuple[Dict[str, Any], Set[str]]:
        """Extract data from items_all_types which has OR structure"""
        # ItemQuery returns one of several item types wrapped in OR structure
        # Check which type this record is

        if hasattr(com_record, "ItemServiceRet") and com_record.ItemServiceRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemServiceRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "Service"
            fields.add("ItemType")
            return data, fields

        elif hasattr(com_record, "ItemInventoryRet") and com_record.ItemInventoryRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemInventoryRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "Inventory"
            fields.add("ItemType")
            return data, fields

        elif hasattr(com_record, "ItemNonInventoryRet") and com_record.ItemNonInventoryRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemNonInventoryRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "NonInventory"
            fields.add("ItemType")
            return data, fields

        elif hasattr(com_record, "ItemOtherChargeRet") and com_record.ItemOtherChargeRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemOtherChargeRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "OtherCharge"
            fields.add("ItemType")
            return data, fields

        elif hasattr(com_record, "ItemFixedAssetRet") and com_record.ItemFixedAssetRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemFixedAssetRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "FixedAsset"
            fields.add("ItemType")
            return data, fields

        elif hasattr(com_record, "ItemSubtotalRet") and com_record.ItemSubtotalRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemSubtotalRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "Subtotal"
            fields.add("ItemType")
            return data, fields

        elif hasattr(com_record, "ItemDiscountRet") and com_record.ItemDiscountRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemDiscountRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "Discount"
            fields.add("ItemType")
            return data, fields

        elif hasattr(com_record, "ItemPaymentRet") and com_record.ItemPaymentRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemPaymentRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "Payment"
            fields.add("ItemType")
            return data, fields

        elif hasattr(com_record, "ItemSalesTaxRet") and com_record.ItemSalesTaxRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemSalesTaxRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "SalesTax"
            fields.add("ItemType")
            return data, fields

        elif hasattr(com_record, "ItemSalesTaxGroupRet") and com_record.ItemSalesTaxGroupRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemSalesTaxGroupRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "SalesTaxGroup"
            fields.add("ItemType")
            return data, fields

        elif hasattr(com_record, "ItemGroupRet") and com_record.ItemGroupRet:
            data, fields = self.extract_com_record_data(
                com_record.ItemGroupRet, known_fields, is_line_item=False
            )
            data["ItemType"] = "Group"
            fields.add("ItemType")
            return data, fields

        # If we get here, we couldn't identify the item type
        logging.warning(f"Could not identify item type for items_all_types record")
        return {}, known_fields


    def extract_line_items(self, header_record: Any, table_config: Dict[str, Any],
                           parent_id: str, known_fields: Set[str]) -> List[Dict[str, Any]]:
        """
        Extract line items from header record

        Args:
            header_record: Parent COM record
            table_config: Table configuration
            parent_id: Parent record ID
            known_fields: Set of known field names

        Returns:
            List of line item dictionaries
        """
        table_name = table_config["name"]
        key_field = table_config["key_field"]

        # Special handling for Bills
        if table_name == "bills":
            return self._extract_bill_line_items(header_record, parent_id, key_field, known_fields)
        else:
            # Generic line item extraction
            return self._extract_generic_line_items(
                header_record, table_config, parent_id, key_field, known_fields
            )

    def extract_com_record_data(self, com_record: Any, known_fields: Set[str],
                                is_line_item: bool = False,
                                parent_key: str = "") -> Tuple[Dict[str, Any], Set[str]]:
        """
        Extract data from QBFC COM record object

        Args:
            com_record: COM record object
            known_fields: Set of known field names
            is_line_item: Whether this is a line item
            parent_key: Parent key for flattening

        Returns:
            Tuple of (data_dict, updated_fields_set)
        """
        data = {}
        updated_fields = known_fields.copy()
        field_prefix = "CustomField_Line_" if is_line_item else "CustomField_"

        # Extract standard fields
        for prop_name in dir(com_record):
            if self._should_skip_property(prop_name):
                continue

            value = None
            try:
                prop_obj = getattr(com_record, prop_name)

                # Special handling for ORRate structure (important for sales order line items)
                if prop_name == 'ORRate' and hasattr(prop_obj, 'Rate'):
                    # Extract the rate value from ORRate.Rate
                    try:
                        if hasattr(prop_obj.Rate, 'GetValue'):
                            rate_value = prop_obj.Rate.GetValue()
                            if rate_value is not None:
                                data['ORRate_Rate'] = rate_value
                                updated_fields.add('ORRate_Rate')
                                # Removed debug logging here
                    except Exception as e:
                        pass  # Silently continue

                    # Also check for RatePercent if it exists
                    try:
                        if hasattr(prop_obj, 'RatePercent') and hasattr(prop_obj.RatePercent, 'GetValue'):
                            rate_percent = prop_obj.RatePercent.GetValue()
                            if rate_percent is not None:
                                data['ORRate_RatePercent'] = rate_percent
                                updated_fields.add('ORRate_RatePercent')
                                # Removed debug logging here
                    except Exception as e:
                        pass  # Silently continue

                    continue  # Don't process ORRate as a regular property

                if hasattr(prop_obj, 'GetValue'):
                    value = prop_obj.GetValue()
                elif hasattr(prop_obj, 'ListID') and hasattr(prop_obj, 'FullName'):
                    # Flatten reference objects
                    self._extract_reference_object(
                        prop_obj, prop_name, parent_key, data, updated_fields
                    )
                    continue
                elif not callable(prop_obj) and type(prop_obj).__name__ not in ['CDispatch', 'PyIDispatch']:
                    value = prop_obj

            except Exception:
                continue

            if value is not None:
                # Convert datetime objects
                if type(value).__name__ == 'datetime':
                    value = convert_com_datetime(value)

                db_key = f"{parent_key}{prop_name}"
                data[db_key] = value
                updated_fields.add(db_key)

        # Extract custom fields
        self._extract_custom_fields(com_record, data, updated_fields, field_prefix)

        return data, updated_fields

    def _should_skip_property(self, prop_name: str) -> bool:
        """Check if property should be skipped"""
        skip_list = [
            "Count", "GetAt", "Parent", "ElementName", "GetXML", "Type",
            "Invoke", "DataExtRetList", "ORSalesOrderLineRetList",
            "ORInvoiceLineRetList", "ORBillLineList", "ORPurchaseOrderLineRetList",
            "Clear", "Append", "QueryInterface", "AddRef", "Release",
            "GetTypeInfoCount", "GetTypeInfo", "GetIDsOfNames", "iterator",
            "iteratorID", "metaData", "CopyTo"
        ]

        return (prop_name.startswith('_') or
                prop_name.isupper() or
                prop_name in skip_list)

    def _extract_reference_object(self, ref_obj: Any, prop_name: str, parent_key: str,
                                  data: Dict[str, Any], fields: Set[str]) -> None:
        """Extract ListID and FullName from reference object"""
        list_id = get_com_value(ref_obj, 'ListID')
        full_name = get_com_value(ref_obj, 'FullName')

        if list_id is not None:
            key = f"{parent_key}{prop_name}_ListID"
            data[key] = list_id
            fields.add(key)

        if full_name is not None:
            key = f"{parent_key}{prop_name}_FullName"
            data[key] = full_name
            fields.add(key)

    def _extract_custom_fields(self, com_record: Any, data: Dict[str, Any],
                               fields: Set[str], prefix: str) -> None:
        """Extract custom fields from DataExtRetList"""
        if not hasattr(com_record, 'DataExtRetList') or com_record.DataExtRetList is None:
            return

        data_ext_list = com_record.DataExtRetList
        if not hasattr(data_ext_list, "Count"):
            return

        for i in range(data_ext_list.Count):
            data_ext = data_ext_list.GetAt(i)
            if not data_ext:
                continue

            name = get_com_value(data_ext, 'DataExtName')
            value = get_com_value(data_ext, 'DataExtValue')

            if name:
                clean_name = clean_field_name(name, prefix)
                data[clean_name] = value

                if clean_name not in fields:
                    logging.info(f"Discovered new custom field: '{name}' as '{clean_name}'")

                fields.add(clean_name)

    def _extract_transaction_deletion_data(self, com_record: Any,
                                           known_fields: Set[str]) -> Tuple[Dict[str, Any], Set[str]]:
        """Extract transaction deletion data"""
        data = {}
        fields = known_fields.copy()

        # TxnID
        txn_id = get_com_value(com_record, 'TxnID')
        if txn_id:
            data['TxnID'] = txn_id
            fields.add('TxnID')

        # Deletion type
        del_type_num = get_com_value(com_record, 'TxnDelType')
        if del_type_num is not None:
            data['DelTypeNbr'] = del_type_num
            data['DelType'] = get_transaction_type_description(del_type_num)
            fields.add('DelTypeNbr')
            fields.add('DelType')

        # Timestamps
        time_created = get_com_value(com_record, 'TimeCreated')
        if time_created:
            data['TimeCreated'] = convert_com_datetime(time_created)
            fields.add('TimeCreated')

        time_deleted = get_com_value(com_record, 'TimeDeleted')
        if time_deleted:
            data['TimeDeleted'] = convert_com_datetime(time_deleted)
            fields.add('TimeDeleted')

        # RefNumber (optional)
        ref_number = get_com_value(com_record, 'RefNumber')
        if ref_number:
            data['RefNumber'] = ref_number
            fields.add('RefNumber')

        return data, fields

    def _extract_list_deletion_data(self, com_record: Any,
                                    known_fields: Set[str]) -> Tuple[Dict[str, Any], Set[str]]:
        """Extract list deletion data"""
        data = {}
        fields = known_fields.copy()

        # ListID
        list_id = get_com_value(com_record, 'ListID')
        if list_id:
            data['ListID'] = list_id
            fields.add('ListID')

        # FullName
        full_name = get_com_value(com_record, 'FullName')
        if full_name:
            data['FullName'] = full_name
            fields.add('FullName')

        # Deletion type
        del_type_num = get_com_value(com_record, 'ListDelType')
        if del_type_num is not None:
            data['DelTypeNbr'] = del_type_num
            data['DelType'] = get_list_type_description(del_type_num)
            fields.add('DelTypeNbr')
            fields.add('DelType')

        # Timestamps
        time_created = get_com_value(com_record, 'TimeCreated')
        if time_created:
            data['TimeCreated'] = convert_com_datetime(time_created)
            fields.add('TimeCreated')

        time_deleted = get_com_value(com_record, 'TimeDeleted')
        if time_deleted:
            data['TimeDeleted'] = convert_com_datetime(time_deleted)
            fields.add('TimeDeleted')

        return data, fields

    def _extract_terms_data(self, com_record: Any,
                            known_fields: Set[str]) -> Tuple[Dict[str, Any], Set[str]]:
        """Extract terms data (has OR structure)"""
        if hasattr(com_record, "StandardTermsRet") and com_record.StandardTermsRet:
            data, fields = self.extract_com_record_data(
                com_record.StandardTermsRet, known_fields, is_line_item=False
            )
            data["TermsType"] = "Standard"
            fields.add("TermsType")
            return data, fields

        elif hasattr(com_record, "DateDrivenTermsRet") and com_record.DateDrivenTermsRet:
            data, fields = self.extract_com_record_data(
                com_record.DateDrivenTermsRet, known_fields, is_line_item=False
            )
            data["TermsType"] = "DateDriven"
            fields.add("TermsType")
            return data, fields

        logging.warning("Terms record has no StandardTermsRet or DateDrivenTermsRet")
        return {}, known_fields

    def _extract_bill_line_items(self, header_record: Any, parent_id: str,
                                 key_field: str, known_fields: Set[str]) -> List[Dict[str, Any]]:
        """Extract bill line items (multiple types)"""
        line_items = []

        bill_line_configs = [
            {"list_prop": "ORItemLineRetList", "ret_prop": "ItemLineRet", "line_type": "Item"},
            {"list_prop": "ExpenseLineRetList", "ret_prop": "ExpenseLineRet", "line_type": "Expense"}
        ]

        for config in bill_line_configs:
            if not hasattr(header_record, config["list_prop"]):
                continue

            line_list = getattr(header_record, config["list_prop"])
            if not line_list or not hasattr(line_list, "Count"):
                continue

            num_lines = line_list.Count
            if num_lines > 0:
                logging.debug(f"Bill {parent_id}: Found {num_lines} {config['line_type']} lines")

            for i in range(num_lines):
                wrapper = line_list.GetAt(i)
                if not wrapper or not hasattr(wrapper, config["ret_prop"]):
                    continue

                line_record = getattr(wrapper, config["ret_prop"])
                if line_record:
                    line_data, updated_fields = self.extract_com_record_data(
                        line_record, known_fields, is_line_item=True
                    )

                    if line_data:
                        line_data[key_field] = parent_id
                        line_data['BillLineType'] = config['line_type']
                        known_fields.add('BillLineType')
                        line_items.append(line_data)

        return line_items

    def extract_linked_transactions(self, header_record: Any, parent_txn_id: str,
                                    parent_txn_type: str) -> List[Dict[str, Any]]:
        """
        Extract LinkedTxn elements from a transaction record

        Args:
            header_record: The main transaction COM record
            parent_txn_id: TxnID of the parent transaction
            parent_txn_type: Type of parent (Invoice, Bill, etc)

        Returns:
            List of linked transaction dictionaries
        """
        linked_txns = []

        # Debug logging
        logging.debug(f"Checking for LinkedTxn in {parent_txn_type} {parent_txn_id}")

        # Check if record has LinkedTxn property
        if not hasattr(header_record, 'LinkedTxn'):
            logging.debug(f"No LinkedTxn property found on {parent_txn_type} COM object")

            # List properties that contain 'Link' for debugging
            link_props = [p for p in dir(header_record) if 'Link' in p and not p.startswith('_')]
            if link_props:
                logging.debug(f"Properties with 'Link' in name: {link_props}")

            return linked_txns

        logging.info(f"LinkedTxn property exists on {parent_txn_type} {parent_txn_id}")

        # QuickBooks can return either a single LinkedTxn or a list
        # First check if it's a list
        if hasattr(header_record, 'LinkedTxnList'):
            linked_list = header_record.LinkedTxnList
            if linked_list and hasattr(linked_list, 'Count'):
                logging.info(f"Found LinkedTxnList with {linked_list.Count} items")
                for i in range(linked_list.Count):
                    linked_txn = linked_list.GetAt(i)
                    if linked_txn:
                        txn_data = self._extract_single_linked_txn(
                            linked_txn, parent_txn_id, parent_txn_type
                        )
                        if txn_data:
                            linked_txns.append(txn_data)

        # Also check for single LinkedTxn
        elif hasattr(header_record, 'LinkedTxn') and header_record.LinkedTxn:
            # Could be single or multiple - need to check
            linked_obj = header_record.LinkedTxn

            # If it has Count, it's a collection
            if hasattr(linked_obj, 'Count'):
                logging.info(f"Found LinkedTxn collection with {linked_obj.Count} items")
                for i in range(linked_obj.Count):
                    linked_txn = linked_obj.GetAt(i)
                    if linked_txn:
                        txn_data = self._extract_single_linked_txn(
                            linked_txn, parent_txn_id, parent_txn_type
                        )
                        if txn_data:
                            linked_txns.append(txn_data)
            else:
                # Single LinkedTxn object
                logging.info(f"Found single LinkedTxn object")
                txn_data = self._extract_single_linked_txn(
                    linked_obj, parent_txn_id, parent_txn_type
                )
                if txn_data:
                    linked_txns.append(txn_data)

        if linked_txns:
            logging.info(f"Extracted {len(linked_txns)} LinkedTxns via COM for {parent_txn_type} {parent_txn_id}")

        return linked_txns

    def _extract_single_linked_txn(self, linked_txn: Any, parent_txn_id: str,
                                   parent_txn_type: str) -> Optional[Dict[str, Any]]:
        """Extract data from a single LinkedTxn object"""
        try:
            data = {
                'ParentTxnID': parent_txn_id,
                'ParentTxnType': parent_txn_type
            }

            # Extract standard fields
            data['LinkedTxnID'] = get_com_value(linked_txn, 'TxnID')
            data['LinkedTxnType'] = get_com_value(linked_txn, 'TxnType')
            data['LinkedTxnDate'] = convert_com_datetime(get_com_value(linked_txn, 'TxnDate'))
            data['LinkedRefNumber'] = get_com_value(linked_txn, 'RefNumber')
            data['LinkType'] = get_com_value(linked_txn, 'LinkType')
            data['Amount'] = get_com_value(linked_txn, 'Amount')

            # Extract optional discount fields
            data['DiscountAmount'] = get_com_value(linked_txn, 'DiscountAmount')

            # Extract discount account reference
            if hasattr(linked_txn, 'DiscountAccountRef') and linked_txn.DiscountAccountRef:
                disc_ref = linked_txn.DiscountAccountRef
                data['DiscountAccountRef_ListID'] = get_com_value(disc_ref, 'ListID')
                data['DiscountAccountRef_FullName'] = get_com_value(disc_ref, 'FullName')

            # Extract discount class reference
            if hasattr(linked_txn, 'DiscountClassRef') and linked_txn.DiscountClassRef:
                class_ref = linked_txn.DiscountClassRef
                data['DiscountClassRef_ListID'] = get_com_value(class_ref, 'ListID')
                data['DiscountClassRef_FullName'] = get_com_value(class_ref, 'FullName')

            # Only return if we have the essential fields
            if data.get('LinkedTxnID') and data.get('LinkedTxnType'):
                return data
            else:
                return None

        except Exception as e:
            logging.error(f"Error extracting LinkedTxn: {e}")
            return None


    def _extract_generic_line_items(self, header_record: Any, table_config: Dict[str, Any],
                                    parent_id: str, key_field: str,
                                    known_fields: Set[str]) -> List[Dict[str, Any]]:
        """Extract generic line items"""
        line_items = []

        list_prop = table_config.get("or_line_list_prop_name")
        ret_prop = table_config.get("specific_line_ret_prop_name")

        if not list_prop:
            return line_items

        if not hasattr(header_record, list_prop):
            return line_items

        line_list = getattr(header_record, list_prop)
        if not line_list or not hasattr(line_list, "Count"):
            return line_items

        num_lines = line_list.Count
        #if num_lines > 0:
         #   logging.debug(f"Found {num_lines} lines for {table_config['name']} {parent_id}")

        for i in range(num_lines):
            line_wrapper = line_list.GetAt(i)

            # Handle direct vs wrapped line items
            if ret_prop is None:
                # Direct line items (like deposits)
                line_record = line_wrapper
            else:
                # Wrapped line items
                if not line_wrapper or not hasattr(line_wrapper, ret_prop):
                    continue
                line_record = getattr(line_wrapper, ret_prop)

            if line_record:
                line_data, updated_fields = self.extract_com_record_data(
                    line_record, known_fields, is_line_item=True
                )

                if line_data:
                    line_data[key_field] = parent_id
                    line_items.append(line_data)

        return line_items