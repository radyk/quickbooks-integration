"""
Price analysis for open orders and historical customer pricing
"""
import logging
import time
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timedelta
import pywintypes

from database.base import DatabaseInterface, SyncStatus
from quickbooks.connection import QuickBooksConnection
from utils import get_com_value, convert_com_datetime, log_com_error


class PriceAnalyzer:
    """Analyzes customer pricing patterns and variations"""

    def __init__(self, qb_connection: QuickBooksConnection, database: DatabaseInterface):
        self.qb = qb_connection
        self.db = database
        self.batch_size = 50  # Items per sales order for price checking

    def analyze_open_orders(self, max_orders: Optional[int] = None) -> None:
        """
        Analyze pricing on open sales orders vs QuickBooks calculated prices
        Also updates customer_price_pages with current pricing data

        Args:
            max_orders: Maximum number of orders to analyze (None = all)
        """
        start_time = time.time()
        logging.info("Starting open sales order price analysis...")

        try:
            # Get previously analyzed orders to skip
            previously_analyzed = self._get_previously_analyzed_orders()
            logging.info(f"Found {len(previously_analyzed)} previously analyzed orders")

            # Get all open sales orders
            open_orders = self._get_open_sales_orders(max_orders=max_orders)

            if not open_orders:
                logging.info("No open sales orders found")
                self.db.update_sync_timestamp(
                    "open_order_price_analysis",
                    duration=time.time() - start_time,
                    status=SyncStatus.SUCCESS
                )
                return

            # Filter out previously analyzed orders (unless price changed)
            orders_to_process = []
            for order in open_orders:
                should_process = True

                if order['TxnID'] in previously_analyzed:
                    # Check if any line items have changed prices
                    prev_data = previously_analyzed[order['TxnID']]
                    prices_changed = False

                    for line in order['LineItems']:
                        line_key = f"{order['TxnID']}-{line.get('TxnLineID', line.get('LineSeq', 0))}"
                        if line_key in prev_data:
                            prev_price = prev_data[line_key]
                            current_price = line.get('Rate', 0) or 0.0
                            if abs(prev_price - current_price) > 0.01:  # Price changed
                                prices_changed = True
                                break

                    should_process = prices_changed
                    if not should_process:
                        logging.debug(f"Skipping order {order['RefNumber']} - already analyzed with same prices")

                if should_process:
                    orders_to_process.append(order)

            if not orders_to_process:
                logging.info("No orders need analysis (all previously analyzed with same prices)")
                self.db.update_sync_timestamp(
                    "open_order_price_analysis",
                    duration=time.time() - start_time,
                    status=SyncStatus.SUCCESS
                )
                return

            logging.info(
                f"Processing {len(orders_to_process)} orders (skipped {len(open_orders) - len(orders_to_process)} already analyzed)")

            # Create tables if needed
            self._create_price_analysis_table()
            self._create_customer_price_pages_table()

            # Clear only records for orders being processed
            order_ids = [f"'{order['TxnID']}'" for order in orders_to_process]
            if order_ids:
                self.db.execute_query(f"DELETE FROM open_order_price_analysis WHERE TxnID IN ({','.join(order_ids)})")

            # Get existing customer prices for comparison
            existing_customer_prices = self._get_existing_customer_prices_dict()

            # Process each order
            total_lines = 0
            analysis_records = []
            customer_prices_to_update = []
            prices_added = 0
            prices_updated = 0

            for order_idx, order in enumerate(orders_to_process):
                logging.info(f"Processing order {order_idx + 1}/{len(orders_to_process)}: {order['RefNumber']}")

                # Get QuickBooks prices for all items in this order
                qb_prices = self._get_quickbooks_prices_for_order(order)

                # Analyze each line item
                for line in order['LineItems']:
                    item_id = line.get('ItemListID')
                    if not item_id:
                        continue

                    customer_id = order['CustomerListID']
                    qb_price = qb_prices.get(item_id, 0) or 0.0

                    analysis = {
                        'TxnID': order['TxnID'],
                        'TxnLineID': line.get('TxnLineID', f"{order['TxnID']}-{line.get('LineSeq', 0)}"),
                        'RefNumber': order['RefNumber'],
                        'TxnDate': order['TxnDate'],
                        'CustomerListID': customer_id,
                        'CustomerName': order['CustomerName'],
                        'ItemListID': item_id,
                        'ItemName': line.get('ItemName', ''),
                        'ItemDescription': line.get('Desc', ''),
                        'TxnLineSeqNo': line.get('TxnLineSeqNo', line.get('LineSeq', 0)),
                        'Quantity': line.get('Quantity', 0),
                        'OrderedPrice': line.get('Rate', 0) or 0.0,
                        'QuickBooksPrice': qb_price,
                        'PriceLevelName': order.get('PriceLevelName', '')
                    }

                    # Calculate variance
                    if analysis['QuickBooksPrice'] > 0:
                        analysis['Variance'] = analysis['OrderedPrice'] - analysis['QuickBooksPrice']
                        analysis['VariancePercent'] = (analysis['Variance'] / analysis['QuickBooksPrice']) * 100
                    else:
                        analysis['Variance'] = 0
                        analysis['VariancePercent'] = 0

                    analysis_records.append(analysis)
                    total_lines += 1

                    # Check if we need to update customer_price_pages
                    if qb_price > 0:  # Only update if we got a valid price
                        price_key = (customer_id, item_id)
                        existing_price = existing_customer_prices.get(price_key)

                        if existing_price is None:
                            # New customer-item combination
                            customer_prices_to_update.append({
                                'CustomerListID': customer_id,
                                'CustomerName': order['CustomerName'],
                                'ItemListID': item_id,
                                'ItemName': line.get('ItemName', ''),
                                'ItemFullName': line.get('ItemName', ''),  # Use ItemName if FullName not available
                                'Price': qb_price
                            })
                            prices_added += 1
                        elif abs(existing_price - qb_price) > 0.01:
                            # Price changed
                            customer_prices_to_update.append({
                                'CustomerListID': customer_id,
                                'CustomerName': order['CustomerName'],
                                'ItemListID': item_id,
                                'ItemName': line.get('ItemName', ''),
                                'ItemFullName': line.get('ItemName', ''),
                                'Price': qb_price
                            })
                            prices_updated += 1

                    # Save in batches
                    if len(analysis_records) >= 100:
                        self._save_analysis_records(analysis_records)
                        analysis_records = []

                    if len(customer_prices_to_update) >= 100:
                        self._save_customer_prices(customer_prices_to_update)
                        customer_prices_to_update = []

            # Save remaining records
            if analysis_records:
                self._save_analysis_records(analysis_records)

            if customer_prices_to_update:
                self._save_customer_prices(customer_prices_to_update)

            # Update sync timestamp
            duration = time.time() - start_time
            self.db.update_sync_timestamp(
                "open_order_price_analysis",
                duration=duration,
                status=SyncStatus.SUCCESS
            )

            logging.info(f"Analysis complete:")
            logging.info(f"  - Analyzed {total_lines} line items from {len(orders_to_process)} orders")
            logging.info(f"  - Customer prices added: {prices_added}")
            logging.info(f"  - Customer prices updated: {prices_updated}")
            logging.info(f"  - Duration: {duration:.2f} seconds")

        except Exception as e:
            logging.error(f"Error during open order analysis: {str(e)}", exc_info=True)
            self.db.update_sync_timestamp(
                "open_order_price_analysis",
                duration=time.time() - start_time,
                status=SyncStatus.ERROR,
                error_message=str(e)
            )

    def _get_existing_customer_prices_dict(self) -> Dict[Tuple[str, str], float]:
        """Get existing customer prices as a dictionary for easy lookup"""
        prices = {}
        try:
            results = self.db.execute_query(
                "SELECT CustomerListID, ItemListID, Price FROM customer_price_pages"
            )
            for row in results:
                key = (row[0], row[1])  # (CustomerListID, ItemListID)
                prices[key] = float(row[2]) if row[2] is not None else 0.0
        except Exception as e:
            logging.debug(f"Error getting existing customer prices: {e}")
        return prices


    def extract_historical_prices(self, months: int = 6) -> None:
        """Extract customer-item prices for combinations that existed in sales orders within N months"""
        start_time = time.time()
        logging.info(f"Starting customer price extraction for combinations from last {months} months...")

        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=months * 30)

            logging.info(f"Finding customer-item combinations from {start_date.date()} to {end_date.date()}")

            # Get unique customer-item combinations from sales orders in date range
            combinations = self._get_customer_item_combinations_from_orders(start_date, end_date)

            if not combinations:
                logging.info("No customer-item combinations found in date range")
                self.db.update_sync_timestamp(
                    "customer_price_pages",
                    duration=time.time() - start_time,
                    status=SyncStatus.SUCCESS
                )
                return

            logging.info(f"Found {len(combinations)} unique customer-item combinations to process")

            # Create price pages table if needed
            self._create_customer_price_pages_table()

            # Get previously extracted combinations
            existing_prices = self._get_existing_customer_prices()
            logging.info(f"Found {len(existing_prices)} existing price records")

            # Group combinations by customer for efficient processing
            customer_items = {}
            for customer_id, customer_name, item_id, item_name in combinations:
                if customer_id not in customer_items:
                    customer_items[customer_id] = {
                        'name': customer_name,
                        'items': []
                    }
                customer_items[customer_id]['items'].append({
                    'ListID': item_id,
                    'FullName': item_name
                })

            logging.info(f"Processing {len(customer_items)} customers")

            # Process each customer
            all_prices = []
            combinations_processed = 0
            combinations_skipped = 0
            combinations_new = 0

            for cust_idx, (customer_id, customer_data) in enumerate(customer_items.items()):
                customer_name = customer_data['name']
                items = customer_data['items']

                logging.info(
                    f"Processing customer {cust_idx + 1}/{len(customer_items)}: {customer_name} ({len(items)} items)")

                # Process items in batches
                for batch_start in range(0, len(items), self.batch_size):
                    batch_end = min(batch_start + self.batch_size, len(items))
                    item_batch = items[batch_start:batch_end]

                    # Check which items we already have for this customer
                    items_to_process = []
                    for item in item_batch:
                        key = (customer_id, item['ListID'])
                        if key not in existing_prices:
                            items_to_process.append(item)
                            combinations_new += 1
                        else:
                            combinations_skipped += 1

                    if not items_to_process:
                        continue

                    logging.info(f"  Getting prices for {len(items_to_process)} new items")

                    # Create test sales order and get prices
                    prices = self._create_test_sales_order_for_prices(customer_id, customer_name, items_to_process)

                    if prices:
                        all_prices.extend(prices)
                        combinations_processed += len(prices)

                    # Save periodically
                    if len(all_prices) >= 500:
                        self._save_customer_prices(all_prices)
                        all_prices = []

                    # Small delay between batches
                    time.sleep(0.1)

            # Save any remaining prices
            if all_prices:
                self._save_customer_prices(all_prices)

            # Update sync timestamp
            duration = time.time() - start_time
            self.db.update_sync_timestamp(
                "customer_price_pages",
                duration=duration,
                status=SyncStatus.SUCCESS
            )

            logging.info(f"Customer price extraction complete:")
            logging.info(f"  - Total combinations found: {len(combinations)}")
            logging.info(f"  - New combinations: {combinations_new}")
            logging.info(f"  - Skipped (already exists): {combinations_skipped}")
            logging.info(f"  - Successfully processed: {combinations_processed}")
            logging.info(f"  - Duration: {duration:.2f} seconds ({duration / 60:.2f} minutes)")

        except Exception as e:
            logging.error(f"Error during customer price extraction: {str(e)}", exc_info=True)
            self.db.update_sync_timestamp(
                "customer_price_pages",
                duration=time.time() - start_time,
                status=SyncStatus.ERROR,
                error_message=str(e)
            )

    def _get_customer_item_combinations_from_orders(self, start_date: datetime, end_date: datetime) -> List[
        Tuple[str, str, str, str]]:
        """Get unique customer-item combinations from sales orders in date range"""
        combinations = set()

        try:
            # Query to get unique customer-item combinations from sales orders
            query = """
                SELECT DISTINCT
                    so.CustomerRef_ListID,
                    so.CustomerRef_FullName,
                    sol.ItemRef_ListID,
                    sol.ItemRef_FullName
                FROM sales_orders so
                INNER JOIN sales_orders_line_items sol ON so.TxnID = sol.TxnID
                INNER JOIN items_inventory inv ON sol.ItemRef_ListID = inv.ListID
                WHERE so.TxnDate >= ?
                  AND so.TxnDate <= ?
                  AND sol.ItemRef_ListID IS NOT NULL
                  AND sol.Quantity > 0
                  AND sol.ORRate_Rate > 0
                ORDER BY so.CustomerRef_FullName, sol.ItemRef_FullName
            """

            results = self.db.execute_query(
                query,
                (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            )

            for row in results:
                if all(row):  # Ensure no None values
                    combinations.add((row[0], row[1], row[2], row[3]))

            return list(combinations)

        except Exception as e:
            logging.error(f"Error getting customer-item combinations: {str(e)}")
            return []

    def _get_existing_customer_prices(self) -> Set[Tuple[str, str]]:
        """Get existing customer-item combinations from customer_price_pages"""
        existing = set()
        try:
            results = self.db.execute_query(
                "SELECT CustomerListID, ItemListID FROM customer_price_pages"
            )
            for row in results:
                existing.add((row[0], row[1]))
        except Exception as e:
            logging.debug(f"Error getting existing prices: {e}")
        return existing

    def _save_customer_prices(self, prices: List[Dict[str, Any]]) -> None:
        """Save customer prices to customer_price_pages table"""
        current_time = datetime.now().isoformat()

        for price in prices:
            self.db.execute_query(
                """
                INSERT OR REPLACE INTO customer_price_pages 
                (CustomerListID, CustomerName, ItemListID, ItemName, ItemFullName, Price, LastUpdated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    price['CustomerListID'],
                    price['CustomerName'],
                    price['ItemListID'],
                    price.get('ItemName', ''),
                    price['ItemFullName'],
                    price['Price'],
                    current_time
                )
            )

        logging.info(f"Saved {len(prices)} customer prices to database")

    def _create_customer_price_pages_table(self) -> None:
        """Create the customer_price_pages table"""
        self.db.execute_query("""
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
        """)

        # Create indexes
        self.db.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_customer_prices_customer ON customer_price_pages (CustomerListID)"
        )
        self.db.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_customer_prices_item ON customer_price_pages (ItemListID)"
        )

    def _create_test_sales_order_for_prices(self, customer_id: str, customer_name: str,
                                            items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create a temporary sales order to get pricing information"""
        try:
            request_msg_set = self.qb.create_request()
            sales_order_add = request_msg_set.AppendSalesOrderAddRq()

            # Set customer
            sales_order_add.CustomerRef.ListID.SetValue(customer_id)

            # Set custom reference number
            current_time = int(time.time())
            ref_number = f"CPP-{current_time % 1000000:06d}"
            sales_order_add.RefNumber.SetValue(ref_number)

            # Set memo
            sales_order_add.Memo.SetValue("CUSTOMER PRICE EXTRACTION - DO NOT PROCESS - WILL BE DELETED")

            # Add line items
            for item in items:
                line_add = sales_order_add.ORSalesOrderLineAddList.Append()
                line_add.SalesOrderLineAdd.ItemRef.ListID.SetValue(item['ListID'])
                line_add.SalesOrderLineAdd.Quantity.SetValue(1.0)

            # Execute the request
            response_msg_set = self.qb.do_requests(request_msg_set)
            response = response_msg_set.ResponseList.GetAt(0)

            if response.StatusCode != 0:
                logging.error(f"Error creating test sales order: {response.StatusMessage}")
                return []

            # Extract the sales order details
            sales_order_ret = response.Detail
            if sales_order_ret is None:
                return []

            # Get the TxnID for deletion
            txn_id = sales_order_ret.TxnID.GetValue()

            # Extract line item prices
            prices = []
            line_ret_list = sales_order_ret.ORSalesOrderLineRetList

            if line_ret_list and line_ret_list.Count > 0:
                for i in range(line_ret_list.Count):
                    line_wrapper = line_ret_list.GetAt(i)
                    if hasattr(line_wrapper, 'SalesOrderLineRet'):
                        line_ret = line_wrapper.SalesOrderLineRet

                        item_ref = line_ret.ItemRef
                        item_list_id = get_com_value(item_ref, 'ListID') if item_ref else None

                        # Get the rate
                        rate = None
                        if hasattr(line_ret, 'ORRate') and line_ret.ORRate:
                            or_rate = line_ret.ORRate
                            if hasattr(or_rate, 'Rate') and or_rate.Rate:
                                rate = or_rate.Rate.GetValue()

                        if rate is None and hasattr(line_ret, 'Rate') and line_ret.Rate:
                            rate = line_ret.Rate.GetValue()

                        if item_list_id and rate is not None:
                            # Find the item name from our batch
                            item_name = ''
                            for item in items:
                                if item['ListID'] == item_list_id:
                                    item_name = item['FullName']
                                    break

                            prices.append({
                                'CustomerListID': customer_id,
                                'CustomerName': customer_name,
                                'ItemListID': item_list_id,
                                'ItemFullName': item_name,
                                'Price': float(rate)
                            })

            # Delete the sales order
            self._delete_sales_order(txn_id)

            return prices

        except Exception as e:
            logging.error(f"Error in create_test_sales_order_for_prices: {str(e)}", exc_info=True)
            return []
    def _get_historical_sales_orders_from_db(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get sales orders from database within date range (inventory items only)"""
        orders = []

        try:
            # Query sales orders in date range
            query = """
                SELECT 
                    so.TxnID,
                    so.RefNumber,
                    so.TxnDate,
                    so.CustomerRef_ListID,
                    so.CustomerRef_FullName
                FROM sales_orders so
                WHERE so.TxnDate >= ?
                  AND so.TxnDate <= ?
                ORDER BY so.TxnDate DESC
            """

            header_results = self.db.execute_query(
                query,
                (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            )

            # Get line items for each order
            for row in header_results:
                txn_id = row[0]

                order_data = {
                    'TxnID': txn_id,
                    'RefNumber': row[1],
                    'TxnDate': row[2],
                    'CustomerListID': row[3],
                    'CustomerName': row[4],
                    'LineItems': []
                }

                # Get inventory line items only
                line_query = """
                    SELECT 
                        sol.ItemRef_ListID,
                        sol.ItemRef_FullName,
                        sol.ORRate_Rate
                    FROM sales_orders_line_items sol
                    INNER JOIN items_inventory inv ON sol.ItemRef_ListID = inv.ListID
                    WHERE sol.TxnID = ?
                      AND sol.ItemRef_ListID IS NOT NULL
                      AND sol.Quantity > 0
                      AND sol.ORRate_Rate > 0
                      AND sol.ORRate_Rate IS NOT NULL
                """

                line_results = self.db.execute_query(line_query, (txn_id,))

                for line_row in line_results:
                    line_data = {
                        'ItemListID': line_row[0],
                        'ItemName': line_row[1] or '',
                        'Rate': float(line_row[2]) if line_row[2] else 0.0
                    }
                    order_data['LineItems'].append(line_data)

                # Only include orders with inventory items
                if order_data['LineItems']:
                    orders.append(order_data)

            return orders

        except Exception as e:
            logging.error(f"Error retrieving historical sales orders: {str(e)}")
            return []

    def _get_existing_price_history(self) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """Get existing customer-item price history from database"""
        existing = {}

        try:
            query = """
                SELECT 
                    CustomerListID,
                    ItemListID,
                    LatestPrice,
                    LatestTxnDate,
                    TransactionCount,
                    FirstTxnDate
                FROM customer_item_price_history
            """

            results = self.db.execute_query(query)

            for row in results:
                key = (row[0], row[1])  # (CustomerListID, ItemListID)
                existing[key] = {
                    'LatestPrice': row[2],
                    'LatestTxnDate': row[3],
                    'TransactionCount': row[4],
                    'FirstTxnDate': row[5]
                }

            logging.info(f"Found {len(existing)} existing customer-item combinations in history")

        except Exception as e:
            logging.debug(f"Error getting existing price history: {e}")

        return existing
    def _get_open_sales_orders(self, max_orders: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get open sales orders from the database"""
        orders = []

        try:
            logging.info(f"Querying open sales orders from database (max: {max_orders or 'all'})...")
            query_start = time.time()

            # Query from the synced sales_orders table
            query = """
                SELECT 
                    so.TxnID,
                    so.RefNumber,
                    so.TxnDate,
                    so.CustomerRef_ListID,
                    so.CustomerRef_FullName,
                    so.IsFullyInvoiced,
                    so.IsManuallyClosed
                FROM sales_orders so
                WHERE (so.IsFullyInvoiced = 0 OR so.IsFullyInvoiced IS NULL)
                  AND (so.IsManuallyClosed = 0 OR so.IsManuallyClosed IS NULL)
                ORDER BY so.TxnDate DESC
            """

            if max_orders:
                query += f" LIMIT {max_orders}"

            header_results = self.db.execute_query(query)

            query_duration = time.time() - query_start
            logging.info(
                f"Database query completed in {query_duration:.2f} seconds, found {len(header_results)} open orders")

            # Now get line items for each order
            for row in header_results:
                txn_id = row[0]
                ref_number = row[1]

                order_data = {
                    'TxnID': txn_id,
                    'RefNumber': ref_number,
                    'TxnDate': row[2],
                    'CustomerListID': row[3],
                    'CustomerName': row[4],
                    'LineItems': [],
                    'PriceLevelName': ''
                }

                # Get line items from database using actual column names
                # Join with items_inventory table to ensure we only get inventory items
                line_query = """
                    SELECT 
                        sol.TxnLineID,
                        sol.ItemRef_ListID,
                        sol.ItemRef_FullName,
                        sol.Desc,
                        sol.Quantity,
                        sol.Amount,
                        sol.ORRate_Rate
                    FROM sales_orders_line_items sol
                    INNER JOIN items_inventory inv ON sol.ItemRef_ListID = inv.ListID
                    WHERE sol.TxnID = ?
                    AND sol.ItemRef_ListID IS NOT NULL
                    AND sol.Quantity > 0
                    AND sol.ORRate_Rate > 0
                    AND sol.ORRate_Rate IS NOT NULL
                    ORDER BY sol.TxnLineID
                """

                line_results = self.db.execute_query(line_query, (txn_id,))

                for line_idx, line_row in enumerate(line_results):
                    # Get rate from ORRate_Rate field
                    rate = float(line_row[6]) if line_row[6] is not None else 0.0

                    # Skip if rate is 0 (shouldn't happen with our query, but double-check)
                    if rate <= 0:
                        continue

                    # Use line index as sequence number for now
                    line_seq_no = line_idx + 1

                    line_data = {
                        'TxnLineID': line_row[0] or f"{txn_id}-{line_seq_no}",
                        'ItemListID': line_row[1],
                        'ItemName': line_row[2] or '',
                        'Desc': line_row[3] or '',
                        'Quantity': float(line_row[4]) if line_row[4] else 0.0,
                        'Rate': rate,
                        'Amount': float(line_row[5]) if line_row[5] else 0.0,
                        'LineSeq': line_seq_no,
                        'TxnLineSeqNo': line_seq_no
                    }
                    order_data['LineItems'].append(line_data)

                # Only include orders that have inventory line items
                if order_data['LineItems']:
                    orders.append(order_data)

            logging.info(f"Retrieved {len(orders)} open sales orders with inventory line items")

            # Log filtering statistics
            total_lines = sum(len(order['LineItems']) for order in orders)
            logging.info(f"Total inventory line items to analyze: {total_lines}")

            return orders

        except Exception as e:
            logging.error(f"Error retrieving open sales orders from database: {str(e)}", exc_info=True)
            # Fall back to QuickBooks query if database fails
            logging.info("Falling back to QuickBooks query...")
            return self._get_open_sales_orders_from_qb(max_orders)


    def _get_open_sales_orders_from_qb(self, max_orders: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fallback method to get orders directly from QuickBooks"""
        # This is the original QB query code - keeping as fallback
        orders = []
        logging.warning("Using fallback QuickBooks query - this may be slow")
        # ... (original implementation)
        return orders

    def _get_sales_orders_in_range(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get sales orders within a date range"""
        orders = []

        try:
            request_msg_set = self.qb.create_request()
            query = request_msg_set.AppendSalesOrderQueryRq()

            # Include line items
            query.IncludeLineItems.SetValue(True)

            # Set date range filter
            if hasattr(query, 'ORTxnQuery'):
                or_query = query.ORTxnQuery
                if hasattr(or_query, 'TxnFilter'):
                    txn_filter = or_query.TxnFilter
                    if hasattr(txn_filter, 'ORDateRangeFilter'):
                        date_filter = txn_filter.ORDateRangeFilter.TxnDateRangeFilter

                        # Set date range
                        from_date = pywintypes.Time(start_date)
                        to_date = pywintypes.Time(end_date)

                        if hasattr(date_filter.FromTxnDate, 'SetValue'):
                            date_filter.FromTxnDate.SetValue(from_date, True)
                        else:
                            date_filter.FromTxnDate.setvalue(from_date, True)

                        if hasattr(date_filter.ToTxnDate, 'SetValue'):
                            date_filter.ToTxnDate.SetValue(to_date, True)
                        else:
                            date_filter.ToTxnDate.setvalue(to_date, True)

            response_msg_set = self.qb.do_requests(request_msg_set)

            if response_msg_set.ResponseList.Count == 0:
                return orders

            response = response_msg_set.ResponseList.GetAt(0)
            if response.StatusCode != 0:
                logging.error(f"Error querying sales orders: {response.StatusMessage}")
                return orders

            order_list = response.Detail
            if order_list is None:
                return orders

            for i in range(order_list.Count):
                order = order_list.GetAt(i)

                order_data = {
                    'TxnID': get_com_value(order, 'TxnID'),
                    'RefNumber': get_com_value(order, 'RefNumber'),
                    'TxnDate': convert_com_datetime(get_com_value(order, 'TxnDate')),
                    'CustomerListID': get_com_value(order.CustomerRef, 'ListID') if order.CustomerRef else None,
                    'CustomerName': get_com_value(order.CustomerRef, 'FullName') if order.CustomerRef else '',
                    'LineItems': []
                }

                # Extract line items
                line_list = order.ORSalesOrderLineRetList
                if line_list and hasattr(line_list, 'Count'):
                    for j in range(line_list.Count):
                        line_wrapper = line_list.GetAt(j)
                        if hasattr(line_wrapper, 'SalesOrderLineRet'):
                            line = line_wrapper.SalesOrderLineRet

                            line_data = {
                                'ItemListID': get_com_value(line.ItemRef, 'ListID') if line.ItemRef else None,
                                'ItemName': get_com_value(line.ItemRef, 'FullName') if line.ItemRef else '',
                                'Rate': get_com_value(line, 'Rate')
                            }

                            if line_data['ItemListID']:
                                order_data['LineItems'].append(line_data)

                if order_data['LineItems']:
                    orders.append(order_data)

            return orders

        except Exception as e:
            logging.error(f"Error retrieving sales orders in range: {str(e)}", exc_info=True)
            return orders

    def _get_previously_analyzed_orders(self) -> Dict[str, Dict[str, float]]:
        """Get previously analyzed orders and their prices"""
        analyzed = {}

        try:
            results = self.db.execute_query("""
                SELECT TxnID, TxnLineID, OrderedPrice 
                FROM open_order_price_analysis
            """)

            for row in results:
                txn_id = row[0]
                line_id = row[1]
                price = row[2]

                if txn_id not in analyzed:
                    analyzed[txn_id] = {}
                analyzed[txn_id][line_id] = price

        except Exception as e:
            logging.debug(f"Error getting previously analyzed orders: {e}")

        return analyzed

    def _get_quickbooks_prices_for_order(self, order: Dict[str, Any]) -> Dict[str, float]:
        """
        Get QuickBooks calculated prices for all items in an order using sales order method
        Returns dict: {item_id: price}
        """
        prices = {}
        customer_id = order['CustomerListID']
        items = [line for line in order['LineItems'] if line.get('ItemListID')]

        if not items:
            return prices

        # Process in batches
        for batch_start in range(0, len(items), self.batch_size):
            batch_end = min(batch_start + self.batch_size, len(items))
            batch_items = items[batch_start:batch_end]

            # Create test sales order
            sales_order_prices = self._create_test_sales_order(customer_id, batch_items, order['RefNumber'])
            if sales_order_prices:
                prices.update(sales_order_prices)

            # Small delay between batches
            if batch_end < len(items):
                time.sleep(0.1)

        return prices

    def _create_test_sales_order(self, customer_id: str, items: List[Dict[str, Any]],
                                order_ref: str) -> Dict[str, float]:
        """
        Create a temporary sales order to get pricing information
        Returns dict: {item_id: price}
        """
        try:
            request_msg_set = self.qb.create_request()
            sales_order_add = request_msg_set.AppendSalesOrderAddRq()

            # Set customer
            sales_order_add.CustomerRef.ListID.SetValue(customer_id)

            # Set custom reference number (PA = Price Analysis)
            current_time = int(time.time())
            ref_number = f"PA-{current_time % 1000000:06d}"
            sales_order_add.RefNumber.SetValue(ref_number)

            # Set clear memo
            sales_order_add.Memo.SetValue(f"PRICE ANALYSIS FOR SO#{order_ref} - DO NOT PROCESS - WILL BE DELETED")

            # Add line items
            for item in items:
                line_add = sales_order_add.ORSalesOrderLineAddList.Append()
                line_add.SalesOrderLineAdd.ItemRef.ListID.SetValue(item['ItemListID'])

                # Ensure quantity is a valid float
                quantity = item.get('Quantity', 1.0)
                if quantity is None or quantity == 0:
                    quantity = 1.0
                line_add.SalesOrderLineAdd.Quantity.SetValue(float(quantity))
                # Don't set rate - let QB calculate it

            # Execute the request
            response_msg_set = self.qb.do_requests(request_msg_set)
            response = response_msg_set.ResponseList.GetAt(0)

            if response.StatusCode != 0:
                logging.error(f"Error creating test sales order: {response.StatusMessage}")
                return {}

            # Extract the sales order details
            sales_order_ret = response.Detail
            if sales_order_ret is None:
                return {}

            # Get the TxnID for deletion
            txn_id = sales_order_ret.TxnID.GetValue()

            # Extract line item prices
            prices = {}
            line_ret_list = sales_order_ret.ORSalesOrderLineRetList

            if line_ret_list and line_ret_list.Count > 0:
                for i in range(line_ret_list.Count):
                    line_wrapper = line_ret_list.GetAt(i)
                    if hasattr(line_wrapper, 'SalesOrderLineRet'):
                        line_ret = line_wrapper.SalesOrderLineRet

                        item_ref = line_ret.ItemRef
                        item_list_id = get_com_value(item_ref, 'ListID') if item_ref else None

                        # Get the rate from ORRate structure (sales orders use this)
                        rate = None

                        if hasattr(line_ret, 'ORRate') and line_ret.ORRate:
                            or_rate = line_ret.ORRate
                            if hasattr(or_rate, 'Rate') and or_rate.Rate:
                                try:
                                    rate = or_rate.Rate.GetValue()
                                except:
                                    rate = None

                        # Fallback to direct Rate field if needed
                        if rate is None and hasattr(line_ret, 'Rate') and line_ret.Rate:
                            try:
                                rate = line_ret.Rate.GetValue()
                            except:
                                rate = None

                        if item_list_id and rate is not None:
                            prices[item_list_id] = float(rate)

            # Delete the sales order
            self._delete_sales_order(txn_id)

            return prices

        except Exception as e:
            logging.error(f"Error in create_test_sales_order: {str(e)}", exc_info=True)
            return {}

    def _delete_sales_order(self, txn_id: str) -> None:
        """Delete a sales order by TxnID"""
        try:
            request_msg_set = self.qb.create_request()
            txn_del = request_msg_set.AppendTxnDelRq()
            txn_del.TxnDelType.SetValue(18)  # 18 = SalesOrder
            txn_del.TxnID.SetValue(txn_id)

            response_msg_set = self.qb.do_requests(request_msg_set)
            response = response_msg_set.ResponseList.GetAt(0)

            if response.StatusCode != 0:
                logging.warning(f"Error deleting sales order {txn_id}: {response.StatusMessage}")

        except Exception as e:
            logging.error(f"Error deleting sales order {txn_id}: {str(e)}", exc_info=True)

    def _create_price_analysis_table(self) -> None:
        """Create the open order price analysis table"""
        fields = {
            'TxnID': 'TEXT',
            'TxnLineID': 'TEXT',
            'RefNumber': 'TEXT',
            'TxnDate': 'TEXT',
            'CustomerListID': 'TEXT',
            'CustomerName': 'TEXT',
            'ItemListID': 'TEXT',
            'ItemName': 'TEXT',
            'ItemDescription': 'TEXT',
            'TxnLineSeqNo': 'INTEGER',  # Add line number field
            'Quantity': 'REAL',
            'OrderedPrice': 'REAL',
            'QuickBooksPrice': 'REAL',
            'Variance': 'REAL',
            'VariancePercent': 'REAL',
            'PriceLevelName': 'TEXT',
            'LastUpdated': 'TEXT'
        }

        # Check if table exists and add new column if needed
        try:
            # First create table if it doesn't exist
            self.db.execute_query(f"""
                CREATE TABLE IF NOT EXISTS open_order_price_analysis (
                    {', '.join([f'{k} {v}' for k, v in fields.items()])},
                    PRIMARY KEY (TxnID, TxnLineID)
                )
            """)

            # Check if TxnLineSeqNo column exists, add if missing
            result = self.db.execute_query("""
                SELECT COUNT(*) 
                FROM pragma_table_info('open_order_price_analysis') 
                WHERE name='TxnLineSeqNo'
            """)

            if result and result[0][0] == 0:
                # Column doesn't exist, add it
                self.db.execute_query("""
                    ALTER TABLE open_order_price_analysis 
                    ADD COLUMN TxnLineSeqNo INTEGER
                """)
                logging.info("Added TxnLineSeqNo column to open_order_price_analysis table")

        except Exception as e:
            logging.debug(f"Note: {e}")
            # If alter fails, table might not exist, so create it
            self.db.execute_query(f"""
                CREATE TABLE IF NOT EXISTS open_order_price_analysis (
                    {', '.join([f'{k} {v}' for k, v in fields.items()])},
                    PRIMARY KEY (TxnID, TxnLineID)
                )
            """)

        # Create indexes for performance
        self.db.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_open_order_customer ON open_order_price_analysis (CustomerListID)"
        )
        self.db.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_open_order_item ON open_order_price_analysis (ItemListID)"
        )
        self.db.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_open_order_variance ON open_order_price_analysis (VariancePercent)"
        )

    def _create_price_history_table(self) -> None:
        """Create the customer item price history table"""
        fields = {
            'CustomerListID': 'TEXT',
            'CustomerName': 'TEXT',
            'ItemListID': 'TEXT',
            'ItemName': 'TEXT',
            'LatestPrice': 'REAL',
            'LatestTxnDate': 'TEXT',
            'LatestRefNumber': 'TEXT',
            'TransactionCount': 'INTEGER',
            'FirstTxnDate': 'TEXT',
            'LastUpdated': 'TEXT'
        }

        # Use composite primary key
        self.db.execute_query(f"""
            CREATE TABLE IF NOT EXISTS customer_item_price_history (
                {', '.join([f'{k} {v}' for k, v in fields.items()])},
                PRIMARY KEY (CustomerListID, ItemListID)
            )
        """)

        # Create indexes
        self.db.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_price_history_customer ON customer_item_price_history (CustomerListID)"
        )
        self.db.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_price_history_item ON customer_item_price_history (ItemListID)"
        )

    def _save_analysis_records(self, records: List[Dict[str, Any]]) -> None:
        """Save price analysis records to database"""
        current_time = datetime.now().isoformat()

        for record in records:
            record['LastUpdated'] = current_time

            columns = list(record.keys())
            values = [record[col] for col in columns]
            placeholders = ', '.join(['?' for _ in columns])
            columns_str = ', '.join(columns)

            self.db.execute_query(
                f"INSERT OR REPLACE INTO open_order_price_analysis ({columns_str}) VALUES ({placeholders})",
                tuple(values)
            )

        logging.debug(f"Saved {len(records)} analysis records")

    def _save_history_records(self, records: List[Dict[str, Any]]) -> None:
        """Save price history records to database"""
        current_time = datetime.now().isoformat()

        for record in records:
            record['LastUpdated'] = current_time

            columns = list(record.keys())
            values = [record[col] for col in columns]
            placeholders = ', '.join(['?' for _ in columns])
            columns_str = ', '.join(columns)

            self.db.execute_query(
                f"INSERT OR REPLACE INTO customer_item_price_history ({columns_str}) VALUES ({placeholders})",
                tuple(values)
            )

        logging.debug(f"Saved {len(records)} history records")