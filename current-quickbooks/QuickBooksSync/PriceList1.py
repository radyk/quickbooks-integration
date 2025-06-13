import win32com.client as win32
import time
import logging
import os
import sys
import datetime
import pywintypes
import sqlite3
from collections import defaultdict
import traceback

# Configure logging
logging.basicConfig(
    filename='customer_price_pages.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Add console handler
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# Global database path (same as main script)
DB_PATH = r'C:\Users\radke\quickbooks_data.db'

# Configuration
BATCH_SIZE = 100  # Number of items to add per sales order
PROGRESS_SAVE_INTERVAL = 50  # Save progress every N customers
TEST_MODE = True  # Set to False for full extraction
TEST_CUSTOMER_LIMIT = 1  # Number of customers to test
TEST_ITEM_LIMIT = 500  # Number of items to test


def quickbooks_login():
    """Login to QuickBooks - simplified version from main script"""
    try:
        logging.info("Starting QuickBooks automation for price pages.")
        logging.info("Creating QBFC Session Manager...")

        # Try different QBFC versions
        try:
            qb = win32.Dispatch("QBFC16.QBSessionManager")
        except Exception as dispatch_error:
            logging.error(f"Failed to dispatch QBFC16.QBSessionManager: {dispatch_error}")
            logging.info("Attempting with common fallbacks...")
            try:
                qb = win32.Dispatch("QBFC15.QBSessionManager")
            except:
                qb = win32.Dispatch("QBFC13.QBSessionManager")

        logging.info("QBFC Session Manager created successfully.")

        # Verify the QuickBooks file path
        qb_file_path = r"C:\Users\Public\Documents\Intuit\QuickBooks\Company Files\Fromm Packaging Systems Canada Inc..QBW"
        logging.info(f"Checking if QuickBooks file exists at: {qb_file_path}")
        if not os.path.exists(qb_file_path):
            logging.error(f"QuickBooks file not found at path: {qb_file_path}")
            return None

        logging.info("Attempting to open connection to QuickBooks...")
        qb.OpenConnection2("", "Fromm Packaging QuickBooks Integration", 1)
        logging.info("Connection to QuickBooks opened successfully.")

        logging.info(f"Attempting to begin session with QuickBooks file: {qb_file_path}")
        qb.BeginSession(qb_file_path, 2)  # 2: Open in Multi-user mode
        logging.info("Logged into QuickBooks successfully.")

        return qb
    except Exception as e:
        logging.error(f"Failed to open QuickBooks session: {str(e)}", exc_info=True)
        return None


def quickbooks_logout(qb):
    """Logout from QuickBooks"""
    if not qb:
        return
    try:
        logging.info("Attempting to end QuickBooks session...")
        qb.EndSession()
        qb.CloseConnection()
        logging.info("Logged out of QuickBooks successfully.")
    except Exception as e:
        logging.error(f"Failed to close QuickBooks session: {str(e)}", exc_info=True)


def get_all_customers(qb):
    """Get all active customers from QuickBooks"""
    customers = []
    try:
        request_msg_set = qb.CreateMsgSetRequest("US", 16, 0)
        customer_query = request_msg_set.AppendCustomerQueryRq()

        # Note: ActiveStatus filter may not be available on all QB versions
        # We'll get all customers and filter inactive ones if needed

        response_msg_set = qb.DoRequests(request_msg_set)
        response = response_msg_set.ResponseList.GetAt(0)

        if response.StatusCode != 0:
            logging.error(f"Error getting customers: {response.StatusMessage}")
            return customers

        customer_ret_list = response.Detail
        if customer_ret_list is None:
            return customers

        for i in range(customer_ret_list.Count):
            customer = customer_ret_list.GetAt(i)

            # Check if customer is active (if property exists)
            is_active = True
            if hasattr(customer, 'IsActive') and customer.IsActive:
                is_active = customer.IsActive.GetValue()

            if is_active:
                customers.append({
                    'ListID': customer.ListID.GetValue(),
                    'FullName': customer.FullName.GetValue() if hasattr(customer,
                                                                        'FullName') and customer.FullName else '',
                    'Name': customer.Name.GetValue() if hasattr(customer, 'Name') and customer.Name else ''
                })

        logging.info(f"Retrieved {len(customers)} active customers")
        return customers

    except Exception as e:
        logging.error(f"Error retrieving customers: {str(e)}", exc_info=True)
        return customers


def get_all_items(qb):
    """Get all active items that can be sold"""
    items = []
    try:
        # Get inventory items
        items.extend(get_items_by_type(qb, "ItemInventoryQueryRq", "Inventory"))

        # Get service items
        items.extend(get_items_by_type(qb, "ItemServiceQueryRq", "Service"))

        # Get non-inventory items
        items.extend(get_items_by_type(qb, "ItemNonInventoryQueryRq", "NonInventory"))

        # Get other charge items
        items.extend(get_items_by_type(qb, "ItemOtherChargeQueryRq", "OtherCharge"))

        logging.info(f"Retrieved {len(items)} total items")
        return items

    except Exception as e:
        logging.error(f"Error retrieving items: {str(e)}", exc_info=True)
        return items


def get_items_by_type(qb, query_method, item_type):
    """Get items of a specific type"""
    items = []
    try:
        request_msg_set = qb.CreateMsgSetRequest("US", 16, 0)
        item_query = getattr(request_msg_set, f"Append{query_method}")()

        # Note: ActiveStatus filter may not be available on all QB versions
        # We'll get all items and filter inactive ones if needed

        response_msg_set = qb.DoRequests(request_msg_set)
        response = response_msg_set.ResponseList.GetAt(0)

        if response.StatusCode != 0:
            logging.warning(f"Error getting {item_type} items: {response.StatusMessage}")
            return items

        item_ret_list = response.Detail
        if item_ret_list is None:
            return items

        for i in range(item_ret_list.Count):
            item = item_ret_list.GetAt(i)

            # Check if item is active (if property exists)
            is_active = True
            if hasattr(item, 'IsActive') and item.IsActive:
                is_active = item.IsActive.GetValue()

            if is_active:
                items.append({
                    'ListID': item.ListID.GetValue(),
                    'FullName': item.FullName.GetValue() if hasattr(item, 'FullName') and item.FullName else '',
                    'Name': item.Name.GetValue() if hasattr(item, 'Name') and item.Name else '',
                    'Type': item_type
                })

        logging.info(f"Retrieved {len(items)} active {item_type} items")
        return items

    except Exception as e:
        logging.error(f"Error retrieving {item_type} items: {str(e)}", exc_info=True)
        return items


def create_test_estimate(qb, customer_id, customer_name, item_batch, customer_index, batch_number):
    """
    Create a temporary estimate to get pricing information
    Returns a list of item prices or None if failed
    """
    try:
        request_msg_set = qb.CreateMsgSetRequest("US", 16, 0)
        estimate_add = request_msg_set.AppendEstimateAddRq()

        # Set customer
        estimate_add.CustomerRef.ListID.SetValue(customer_id)

        # Set custom reference number to avoid incrementing QB's counter
        # QuickBooks RefNumber limited to 11 characters
        # Need to ensure uniqueness to avoid duplicate errors
        # Format: PX-NNNNNN where N is a sequential number
        # Using combination of time and counter for uniqueness
        current_time = int(time.time())
        # Use last 5 digits of timestamp + batch number
        unique_num = (current_time % 100000) * 10 + (batch_number % 10)
        ref_number = f"PX-{unique_num:06d}"
        estimate_add.RefNumber.SetValue(ref_number)

        # Set clear memo indicating this is temporary
        estimate_add.Memo.SetValue("AUTOMATED PRICE EXTRACTION - DO NOT PROCESS - WILL BE DELETED")

        # Set a template if needed (you might need to adjust this)
        # estimate_add.TemplateRef.ListID.SetValue("YOUR_TEMPLATE_ID")

        # Add line items
        items_added = 0
        for item in item_batch:
            line_add = estimate_add.OREstimateLineAddList.Append()
            line_add.EstimateLineAdd.ItemRef.ListID.SetValue(item['ListID'])
            line_add.EstimateLineAdd.Quantity.SetValue(1.0)  # Quantity of 1 to get unit price
            # Don't set rate - let QB calculate it
            items_added += 1

        logging.debug(f"Added {items_added} items to estimate request with RefNumber: {ref_number}")

        # Execute the request
        response_msg_set = qb.DoRequests(request_msg_set)
        response = response_msg_set.ResponseList.GetAt(0)

        if response.StatusCode != 0:
            logging.error(f"Error creating estimate: {response.StatusMessage}")
            return None

        # Extract the estimate details
        estimate_ret = response.Detail
        if estimate_ret is None:
            return None

        # Get the TxnID for deletion
        txn_id = estimate_ret.TxnID.GetValue()
        logging.debug(f"Created estimate {txn_id} with RefNumber: {ref_number}")

        # Extract line item prices
        prices = []
        line_ret_list = estimate_ret.OREstimateLineRetList

        if line_ret_list and line_ret_list.Count > 0:
            logging.debug(f"Estimate has {line_ret_list.Count} line items")
            for i in range(line_ret_list.Count):
                line_wrapper = line_ret_list.GetAt(i)
                if hasattr(line_wrapper, 'EstimateLineRet'):
                    line_ret = line_wrapper.EstimateLineRet

                    item_ref = line_ret.ItemRef
                    item_list_id = item_ref.ListID.GetValue() if item_ref and hasattr(item_ref, 'ListID') else None

                    # Get the rate (price)
                    rate = None
                    if hasattr(line_ret, 'Rate') and line_ret.Rate:
                        rate = line_ret.Rate.GetValue()
                    elif hasattr(line_ret, 'ORRate') and line_ret.ORRate:
                        # Handle OR rate structure
                        or_rate = line_ret.ORRate
                        if hasattr(or_rate, 'Rate') and or_rate.Rate:
                            rate = or_rate.Rate.GetValue()

                    if item_list_id and rate is not None:
                        # Find the item name from our batch
                        item_name = ''
                        item_fullname = ''
                        for item in item_batch:
                            if item['ListID'] == item_list_id:
                                item_name = item['Name']
                                item_fullname = item['FullName']
                                break

                        prices.append({
                            'CustomerListID': customer_id,
                            'CustomerName': customer_name,
                            'ItemListID': item_list_id,
                            'ItemName': item_name,
                            'ItemFullName': item_fullname,
                            'Rate': rate
                        })
                    else:
                        logging.debug(f"Line item missing data: ItemID={item_list_id}, Rate={rate}")
        else:
            logging.warning(f"No line items returned in estimate")

        logging.debug(f"Extracted {len(prices)} prices from estimate")

        # Delete the estimate
        delete_estimate(qb, txn_id)

        return prices

    except Exception as e:
        logging.error(f"Error in create_test_estimate: {str(e)}", exc_info=True)
        return None


def delete_estimate(qb, txn_id):
    """Delete an estimate by TxnID"""
    try:
        request_msg_set = qb.CreateMsgSetRequest("US", 16, 0)
        txn_del = request_msg_set.AppendTxnDelRq()
        txn_del.TxnDelType.SetValue(11)  # 11 = Estimate
        txn_del.TxnID.SetValue(txn_id)

        response_msg_set = qb.DoRequests(request_msg_set)
        response = response_msg_set.ResponseList.GetAt(0)

        if response.StatusCode != 0:
            logging.warning(f"Error deleting estimate {txn_id}: {response.StatusMessage}")
        else:
            logging.debug(f"Successfully deleted estimate {txn_id}")

    except Exception as e:
        logging.error(f"Error deleting estimate {txn_id}: {str(e)}", exc_info=True)


def save_customer_prices(customer_prices):
    """Save customer prices to database"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='customer_price_pages'")
        table_exists = cursor.fetchone() is not None

        if table_exists:
            # Check if we need to add the new columns
            cursor.execute("PRAGMA table_info(customer_price_pages)")
            columns = [col[1] for col in cursor.fetchall()]

            # Add missing columns if needed
            if 'CustomerName' not in columns:
                logging.info("Adding CustomerName column to existing table...")
                cursor.execute("ALTER TABLE customer_price_pages ADD COLUMN CustomerName TEXT")

            if 'ItemName' not in columns:
                logging.info("Adding ItemName column to existing table...")
                cursor.execute("ALTER TABLE customer_price_pages ADD COLUMN ItemName TEXT")

            if 'ItemFullName' not in columns:
                logging.info("Adding ItemFullName column to existing table...")
                cursor.execute("ALTER TABLE customer_price_pages ADD COLUMN ItemFullName TEXT")

            conn.commit()
        else:
            # Create table with all columns
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
            logging.info("Created customer_price_pages table with all columns including names")

        # Create indexes
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_customer_prices_customer 
        ON customer_price_pages (CustomerListID)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_customer_prices_item 
        ON customer_price_pages (ItemListID)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_customer_prices_customer_name 
        ON customer_price_pages (CustomerName)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_customer_prices_item_name 
        ON customer_price_pages (ItemName)
        ''')

        # Insert or update prices
        current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()

        inserted = 0
        updated = 0

        for price_info in customer_prices:
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

            if cursor.rowcount == 1:
                inserted += 1
            else:
                updated += 1

        conn.commit()
        logging.info(f"Saved {len(customer_prices)} customer prices to database ({inserted} new, {updated} updated)")

    except sqlite3.Error as e:
        logging.error(f"Database error saving customer prices: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


def load_progress():
    """Load progress from database"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Check if progress table exists
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_extraction_progress (
            id INTEGER PRIMARY KEY,
            last_customer_id TEXT,
            last_customer_index INTEGER,
            total_customers INTEGER,
            start_time TEXT,
            last_update TEXT
        )
        ''')

        cursor.execute('SELECT * FROM price_extraction_progress ORDER BY id DESC LIMIT 1')
        row = cursor.fetchone()

        if row:
            return {
                'last_customer_id': row[1],
                'last_customer_index': row[2],
                'total_customers': row[3],
                'start_time': row[4],
                'last_update': row[5]
            }
        return None

    except sqlite3.Error as e:
        logging.error(f"Database error loading progress: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def save_progress(customer_id, customer_index, total_customers, start_time):
    """Save progress to database"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()

        cursor.execute('''
        INSERT INTO price_extraction_progress 
        (last_customer_id, last_customer_index, total_customers, start_time, last_update)
        VALUES (?, ?, ?, ?, ?)
        ''', (customer_id, customer_index, total_customers, start_time, current_time))

        conn.commit()

    except sqlite3.Error as e:
        logging.error(f"Database error saving progress: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


def extract_all_customer_prices(qb, resume=True):
    """Main function to extract all customer prices"""
    try:
        start_time = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Get all customers and items
        logging.info("Retrieving all customers...")
        customers = get_all_customers(qb)

        logging.info("Retrieving all items...")
        items = get_all_items(qb)

        if not customers or not items:
            logging.error("No customers or items found. Aborting.")
            return

        # Apply test mode limits
        if TEST_MODE:
            logging.info(f"TEST MODE: Limiting to {TEST_CUSTOMER_LIMIT} customers and {TEST_ITEM_LIMIT} items")
            customers = customers[:TEST_CUSTOMER_LIMIT]
            items = items[:TEST_ITEM_LIMIT]
            logging.info(f"TEST MODE: Selected customer: {customers[0]['FullName'] or customers[0]['Name']}")

        # Check for resume
        start_index = 0
        if resume and not TEST_MODE:  # Don't resume in test mode
            progress = load_progress()
            if progress:
                logging.info(f"Resuming from previous run. Last customer index: {progress['last_customer_index']}")
                start_index = progress['last_customer_index'] + 1
                start_time = progress['start_time']  # Keep original start time

        total_combinations = len(customers) * len(items)
        logging.info(f"Total price combinations to extract: {total_combinations:,}")
        logging.info(f"Processing {len(customers)} customers with {len(items)} items")

        customer_prices = []
        prices_extracted = 0

        # Process each customer
        for customer_index in range(start_index, len(customers)):
            customer = customers[customer_index]
            customer_id = customer['ListID']
            customer_name = customer['FullName'] or customer['Name']

            logging.info(f"Processing customer {customer_index + 1}/{len(customers)}: {customer_name}")

            # Process items in batches
            batch_number = 0
            for batch_start in range(0, len(items), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(items))
                item_batch = items[batch_start:batch_end]
                batch_number += 1

                logging.info(f"  Processing items {batch_start + 1}-{batch_end} of {len(items)}")

                # Create test estimate and get prices
                prices = create_test_estimate(qb, customer_id, customer_name, item_batch, customer_index, batch_number)

                if prices:
                    customer_prices.extend(prices)
                    prices_extracted += len(prices)
                    logging.info(f"    Extracted {len(prices)} prices from this batch")
                else:
                    logging.warning(f"    No prices extracted from this batch")

                # Small delay to avoid overwhelming QuickBooks
                time.sleep(0.1)

            # Save progress periodically (or always in test mode)
            if TEST_MODE or (customer_index + 1) % PROGRESS_SAVE_INTERVAL == 0:
                save_customer_prices(customer_prices)
                if not TEST_MODE:
                    save_progress(customer_id, customer_index, len(customers), start_time)
                logging.info(f"Progress saved. Extracted {prices_extracted:,} prices so far.")
                if not TEST_MODE:
                    customer_prices.clear()  # Clear to save memory

        # Save any remaining prices
        if customer_prices:
            save_customer_prices(customer_prices)
            logging.info(f"Final save: {len(customer_prices)} prices")

        # Clear progress on completion (not in test mode)
        if not TEST_MODE:
            clear_progress()

        end_time = datetime.datetime.now(datetime.timezone.utc)
        duration = (datetime.datetime.fromisoformat(end_time.isoformat()) -
                    datetime.datetime.fromisoformat(start_time)).total_seconds()

        logging.info(f"Price extraction completed!")
        logging.info(f"Total prices extracted: {prices_extracted:,}")
        logging.info(f"Duration: {duration:.2f} seconds ({duration / 60:.2f} minutes)")

        if TEST_MODE:
            # Show sample of extracted prices
            logging.info("\nSample of extracted prices:")
            sample_count = 0
            for price_info in customer_prices[:10]:
                logging.info(f"  {price_info['CustomerName']} - {price_info['ItemName']}: ${price_info['Rate']:.2f}")
                sample_count += 1

    except Exception as e:
        logging.error(f"Error in extract_all_customer_prices: {str(e)}", exc_info=True)
        raise


def clear_progress():
    """Clear the progress table"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM price_extraction_progress')
        conn.commit()
        logging.info("Progress cleared")
    except sqlite3.Error as e:
        logging.error(f"Database error clearing progress: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


def verify_price_data():
    """Verify the extracted price data"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='customer_price_pages'")
        if not cursor.fetchone():
            logging.info("customer_price_pages table does not exist yet.")
            return

        # Check which columns exist
        cursor.execute("PRAGMA table_info(customer_price_pages)")
        columns = [col[1] for col in cursor.fetchall()]
        has_names = 'CustomerName' in columns

        # Get statistics
        cursor.execute('SELECT COUNT(*) FROM customer_price_pages')
        total_prices = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(DISTINCT CustomerListID) FROM customer_price_pages')
        unique_customers = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(DISTINCT ItemListID) FROM customer_price_pages')
        unique_items = cursor.fetchone()[0]

        cursor.execute('SELECT MIN(Price), MAX(Price), AVG(Price) FROM customer_price_pages WHERE Price > 0')
        price_stats = cursor.fetchone()

        logging.info("Customer Price Pages Statistics:")
        logging.info(f"  Total price records: {total_prices:,}")
        logging.info(f"  Unique customers: {unique_customers:,}")
        logging.info(f"  Unique items: {unique_items:,}")
        if price_stats[0] is not None:
            logging.info(f"  Price range: ${price_stats[0]:.2f} - ${price_stats[1]:.2f}")
            logging.info(f"  Average price: ${price_stats[2]:.2f}")

        # Show sample data
        if has_names:
            # Use names if available
            cursor.execute('''
            SELECT CustomerName, ItemName, ItemFullName, Price
            FROM customer_price_pages
            WHERE Price > 0
            ORDER BY Price DESC
            LIMIT 10
            ''')

            sample_data = cursor.fetchall()
            if sample_data:
                logging.info("\nTop 10 highest prices:")
                for row in sample_data:
                    item_display = row[2] if row[2] else row[1]  # Use FullName if available, else Name
                    logging.info(f"  {row[0]} - {item_display}: ${row[3]:.2f}")
        else:
            # Fallback to IDs only
            cursor.execute('''
            SELECT CustomerListID, ItemListID, Price
            FROM customer_price_pages
            WHERE Price > 0
            ORDER BY Price DESC
            LIMIT 10
            ''')

            sample_data = cursor.fetchall()
            if sample_data:
                logging.info("\nTop 10 highest prices:")
                for row in sample_data:
                    logging.info(f"  Customer ID: {row[0][:12]}... Item ID: {row[1][:12]}... Price: ${row[2]:.2f}")

        # Show some zero price items
        cursor.execute('''
        SELECT COUNT(*) FROM customer_price_pages WHERE Price = 0 OR Price IS NULL
        ''')
        zero_price_count = cursor.fetchone()[0]
        if zero_price_count > 0:
            logging.info(f"\nItems with zero or null price: {zero_price_count}")

            if has_names:
                # Show examples with names
                cursor.execute('''
                SELECT CustomerName, ItemName, Price
                FROM customer_price_pages
                WHERE Price = 0 OR Price IS NULL
                LIMIT 5
                ''')
                zero_examples = cursor.fetchall()
                if zero_examples:
                    logging.info("Examples of zero price items:")
                    for row in zero_examples:
                        logging.info(f"  {row[0]} - {row[1]}: ${row[2] if row[2] is not None else 0:.2f}")

    except sqlite3.Error as e:
        logging.error(f"Database error verifying price data: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


def main():
    """Main function"""
    logging.info("==== STARTING CUSTOMER PRICE PAGES EXTRACTION ====")

    if TEST_MODE:
        logging.info("*** RUNNING IN TEST MODE ***")
        logging.info(f"Will process {TEST_CUSTOMER_LIMIT} customer(s) and {TEST_ITEM_LIMIT} items")

    # Parse command line arguments
    resume = True  # Default to resume
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == '--fresh':
            resume = False
            logging.info("Starting fresh extraction (not resuming)")

    # Connect to QuickBooks
    qb = quickbooks_login()
    if not qb:
        logging.error("Failed to connect to QuickBooks. Aborting.")
        return

    try:
        # Run the extraction
        extract_all_customer_prices(qb, resume=resume)

        # Verify the results
        logging.info("\n==== VERIFYING EXTRACTED DATA ====")
        verify_price_data()

    except Exception as e:
        logging.error(f"Critical error during extraction: {str(e)}", exc_info=True)
    finally:
        # Logout from QuickBooks
        quickbooks_logout(qb)

    logging.info("\n==== CUSTOMER PRICE PAGES EXTRACTION COMPLETED ====")

    if TEST_MODE:
        logging.info("*** TEST MODE COMPLETE - Set TEST_MODE = False for full extraction ***")


if __name__ == "__main__":
    overall_start_time = time.time()
    logging.info(f"SCRIPT EXECUTION STARTED AT {datetime.datetime.now().isoformat()}")

    try:
        main()
    except Exception as e:
        logging.critical(f"Unhandled exception in __main__: {str(e)}", exc_info=True)
    finally:
        overall_duration = time.time() - overall_start_time
        logging.info(f"SCRIPT EXECUTION FINISHED. TOTAL DURATION: {overall_duration:.2f} seconds.")