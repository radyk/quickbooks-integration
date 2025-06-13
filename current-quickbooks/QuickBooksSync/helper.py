"""
Check specific sales order in QuickBooks and database
This script helps troubleshoot sync issues by comparing QB vs DB data
"""
import win32com.client as win32
import sqlite3
import datetime
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration - adjust these paths as needed
QB_FILE = r"C:\Users\Public\Documents\Intuit\QuickBooks\Company Files\Fromm Packaging Systems Canada Inc..QBW"
DB_PATH = r'C:\quickbookssync\data\quickbooks_data.db'  # Updated path
ORDER_NUMBER = "22800"  # The order to check

# You can also override these via command line arguments
import sys
if len(sys.argv) > 1:
    ORDER_NUMBER = sys.argv[1]
    print(f"Checking order number: {ORDER_NUMBER}")
if len(sys.argv) > 2:
    DB_PATH = sys.argv[2]
    print(f"Using database: {DB_PATH}")

def quickbooks_login():
    """Login to QuickBooks - using EXACT same connection as main app"""
    try:
        logging.info("Connecting to QuickBooks...")

        # Try different QBFC versions
        try:
            qb = win32.Dispatch("QBFC16.QBSessionManager")
        except:
            try:
                qb = win32.Dispatch("QBFC15.QBSessionManager")
            except:
                qb = win32.Dispatch("QBFC13.QBSessionManager")

        # MUST use exact same app name as main.py to avoid reauthorization
        qb.OpenConnection2("", "Fromm Packaging QuickBooks Integration", 1)
        qb.BeginSession(QB_FILE, 2)  # 2 = Multi-user mode

        logging.info("Connected to QuickBooks successfully")
        return qb

    except Exception as e:
        logging.error(f"Failed to connect to QuickBooks: {e}")
        return None

def quickbooks_logout(qb):
    """Logout from QuickBooks"""
    if qb:
        try:
            qb.EndSession()
            qb.CloseConnection()
            logging.info("Disconnected from QuickBooks")
        except:
            pass

def check_order_in_quickbooks(qb, order_number):
    """Check sales order details in QuickBooks"""
    try:
        # Create request
        request_msg_set = qb.CreateMsgSetRequest("US", 16, 0)
        query = request_msg_set.AppendSalesOrderQueryRq()

        # Include all details
        query.IncludeLineItems.SetValue(True)

        # Filter by RefNumber using OR structure
        or_query = query.ORTxnNoAccountQuery
        filter_obj = or_query.TxnFilterNoAccount

        # Use ORRefNumberFilter instead of RefNumberFilter
        ref_filter = filter_obj.ORRefNumberFilter.RefNumberFilter
        ref_filter.MatchCriterion.SetValue(2)  # 2 = Contains
        ref_filter.RefNumber.SetValue(order_number)

        # Execute query
        response_msg_set = qb.DoRequests(request_msg_set)
        response = response_msg_set.ResponseList.GetAt(0)

        if response.StatusCode != 0:
            logging.error(f"QB Error: {response.StatusMessage}")
            return None

        if response.Detail is None or response.Detail.Count == 0:
            logging.warning(f"Order {order_number} not found in QuickBooks")
            return None

        # Get the order
        order = response.Detail.GetAt(0)

        # Extract key fields
        order_data = {
            'TxnID': order.TxnID.GetValue() if order.TxnID else None,
            'RefNumber': order.RefNumber.GetValue() if order.RefNumber else None,
            'TxnDate': str(order.TxnDate.GetValue()) if order.TxnDate else None,
            'TimeCreated': str(order.TimeCreated.GetValue()) if order.TimeCreated else None,
            'TimeModified': str(order.TimeModified.GetValue()) if order.TimeModified else None,
            'EditSequence': order.EditSequence.GetValue() if order.EditSequence else None,
            'CustomerName': order.CustomerRef.FullName.GetValue() if order.CustomerRef and order.CustomerRef.FullName else None,
            'IsFullyInvoiced': order.IsFullyInvoiced.GetValue() if order.IsFullyInvoiced else False,
            'IsManuallyClosed': order.IsManuallyClosed.GetValue() if order.IsManuallyClosed else False,
            'IsToBeEmailed': order.IsToBeEmailed.GetValue() if order.IsToBeEmailed else False,
            'IsToBePrinted': order.IsToBePrinted.GetValue() if order.IsToBePrinted else False
        }

        # Check for linked transactions
        linked_txns = []
        if hasattr(order, 'LinkedTxn') and order.LinkedTxn:
            logging.info("Found LinkedTxn data!")
            # Handle single or multiple linked transactions
            if hasattr(order.LinkedTxn, 'Count'):
                for i in range(order.LinkedTxn.Count):
                    linked = order.LinkedTxn.GetAt(i)
                    linked_txns.append({
                        'TxnID': linked.TxnID.GetValue() if linked.TxnID else None,
                        'TxnType': linked.TxnType.GetValue() if linked.TxnType else None,
                        'TxnDate': str(linked.TxnDate.GetValue()) if linked.TxnDate else None,
                        'RefNumber': linked.RefNumber.GetValue() if linked.RefNumber else None,
                        'Amount': linked.Amount.GetValue() if linked.Amount else None
                    })
            else:
                # Single linked transaction
                linked = order.LinkedTxn
                linked_txns.append({
                    'TxnID': linked.TxnID.GetValue() if linked.TxnID else None,
                    'TxnType': linked.TxnType.GetValue() if linked.TxnType else None,
                    'TxnDate': str(linked.TxnDate.GetValue()) if linked.TxnDate else None,
                    'RefNumber': linked.RefNumber.GetValue() if linked.RefNumber else None,
                    'Amount': linked.Amount.GetValue() if linked.Amount else None
                })

        order_data['LinkedTransactions'] = linked_txns

        # Get line items count
        line_count = 0
        if order.ORSalesOrderLineRetList and order.ORSalesOrderLineRetList.Count:
            line_count = order.ORSalesOrderLineRetList.Count
        order_data['LineItemCount'] = line_count

        return order_data

    except Exception as e:
        logging.error(f"Error checking order in QuickBooks: {e}", exc_info=True)
        return None

def check_order_in_database(db_path, order_number):
    """Check sales order details in database"""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get order from database
        cursor.execute("""
            SELECT TxnID, RefNumber, TxnDate, TimeCreated, TimeModified,
                   EditSequence, CustomerRef_FullName, IsFullyInvoiced,
                   IsManuallyClosed, IsToBeEmailed, IsToBePrinted
            FROM sales_orders
            WHERE RefNumber = ?
        """, (order_number,))

        row = cursor.fetchone()
        if not row:
            logging.warning(f"Order {order_number} not found in database")
            conn.close()
            return None

        order_data = dict(row)

        # Get line items count
        cursor.execute("""
            SELECT COUNT(*) as line_count
            FROM sales_orders_line_items
            WHERE TxnID = ?
        """, (order_data['TxnID'],))

        line_count = cursor.fetchone()['line_count']
        order_data['LineItemCount'] = line_count

        # Check for linked transactions
        cursor.execute("""
            SELECT LinkedTxnID, LinkedTxnType, LinkedTxnDate, 
                   LinkedRefNumber, Amount
            FROM linked_transactions
            WHERE ParentTxnID = ?
        """, (order_data['TxnID'],))

        linked_txns = []
        for row in cursor.fetchall():
            linked_txns.append(dict(row))

        order_data['LinkedTransactions'] = linked_txns

        # Get last sync info
        cursor.execute("""
            SELECT last_sync_time, last_status, last_error_message
            FROM sync_log
            WHERE table_name = 'sales_orders'
        """)

        sync_info = cursor.fetchone()
        if sync_info:
            order_data['LastSyncTime'] = sync_info['last_sync_time']
            order_data['LastSyncStatus'] = sync_info['last_status']
            order_data['LastSyncError'] = sync_info['last_error_message']

        conn.close()
        return order_data

    except Exception as e:
        logging.error(f"Error checking order in database: {e}", exc_info=True)
        return None

def compare_timestamps(qb_time, db_time):
    """Compare QuickBooks and database timestamps"""
    if not qb_time or not db_time:
        return "Cannot compare - missing timestamp"

    # Parse timestamps
    try:
        # Handle different timestamp formats
        qb_time_str = str(qb_time).split('+')[0].strip()  # Remove timezone
        db_time_str = str(db_time).split('+')[0].strip()

        # Parse dates
        if 'T' in qb_time_str:
            qb_dt = datetime.datetime.fromisoformat(qb_time_str)
        else:
            qb_dt = datetime.datetime.strptime(qb_time_str, '%Y-%m-%d %H:%M:%S')

        if 'T' in db_time_str:
            db_dt = datetime.datetime.fromisoformat(db_time_str)
        else:
            db_dt = datetime.datetime.strptime(db_time_str, '%Y-%m-%d %H:%M:%S')

        if qb_dt > db_dt:
            diff = qb_dt - db_dt
            return f"QB is NEWER by {diff}"
        elif db_dt > qb_dt:
            diff = db_dt - qb_dt
            return f"DB is NEWER by {diff} (PROBLEM!)"
        else:
            return "Timestamps match"

    except Exception as e:
        return f"Error comparing: {e}"

def main():
    """Main function"""
    print("=" * 80)
    print(f"CHECKING SALES ORDER {ORDER_NUMBER}")
    print("=" * 80)

    # Check database
    print("\n1. DATABASE CHECK:")
    print("-" * 40)
    db_data = check_order_in_database(DB_PATH, ORDER_NUMBER)

    if db_data:
        print(f"TxnID: {db_data['TxnID']}")
        print(f"RefNumber: {db_data['RefNumber']}")
        print(f"Customer: {db_data.get('CustomerRef_FullName', 'N/A')}")
        print(f"TxnDate: {db_data['TxnDate']}")
        print(f"TimeCreated: {db_data['TimeCreated']}")
        print(f"TimeModified: {db_data['TimeModified']}")
        print(f"EditSequence: {db_data['EditSequence']}")
        print(f"IsFullyInvoiced: {db_data['IsFullyInvoiced']}")
        print(f"IsManuallyClosed: {db_data['IsManuallyClosed']}")
        print(f"Line Items: {db_data['LineItemCount']}")
        print(f"Linked Transactions: {len(db_data['LinkedTransactions'])}")

        if 'LastSyncTime' in db_data:
            print(f"\nLast Sync: {db_data['LastSyncTime']}")
            print(f"Sync Status: {db_data['LastSyncStatus']}")
            if db_data.get('LastSyncError'):
                print(f"Sync Error: {db_data['LastSyncError']}")
    else:
        print("Order not found in database")

    # Check QuickBooks
    print("\n2. QUICKBOOKS CHECK:")
    print("-" * 40)

    qb = quickbooks_login()
    if not qb:
        print("Failed to connect to QuickBooks")
        return

    try:
        qb_data = check_order_in_quickbooks(qb, ORDER_NUMBER)

        if qb_data:
            print(f"TxnID: {qb_data['TxnID']}")
            print(f"RefNumber: {qb_data['RefNumber']}")
            print(f"Customer: {qb_data['CustomerName']}")
            print(f"TxnDate: {qb_data['TxnDate']}")
            print(f"TimeCreated: {qb_data['TimeCreated']}")
            print(f"TimeModified: {qb_data['TimeModified']}")
            print(f"EditSequence: {qb_data['EditSequence']}")
            print(f"IsFullyInvoiced: {qb_data['IsFullyInvoiced']}")
            print(f"IsManuallyClosed: {qb_data['IsManuallyClosed']}")
            print(f"Line Items: {qb_data['LineItemCount']}")
            print(f"Linked Transactions: {len(qb_data['LinkedTransactions'])}")

            if qb_data['LinkedTransactions']:
                print("\nLinked Transactions:")
                for lt in qb_data['LinkedTransactions']:
                    print(f"  - {lt['TxnType']} {lt['RefNumber']} ({lt['TxnDate']}) Amount: ${lt['Amount']}")
        else:
            print("Order not found in QuickBooks")

        # Compare if both exist
        if db_data and qb_data:
            print("\n3. COMPARISON:")
            print("-" * 40)

            # Compare TimeModified
            print(f"TimeModified comparison: {compare_timestamps(qb_data['TimeModified'], db_data['TimeModified'])}")

            # Compare key fields (handle bool vs int)
            qb_fully_invoiced = qb_data['IsFullyInvoiced']
            db_fully_invoiced = bool(db_data['IsFullyInvoiced'])
            if qb_fully_invoiced != db_fully_invoiced:
                print(f"IsFullyInvoiced MISMATCH: QB={qb_fully_invoiced}, DB={db_fully_invoiced}")

            qb_manually_closed = qb_data['IsManuallyClosed']
            db_manually_closed = bool(db_data['IsManuallyClosed'])
            if qb_manually_closed != db_manually_closed:
                print(f"IsManuallyClosed MISMATCH: QB={qb_manually_closed}, DB={db_manually_closed}")

            if str(qb_data['EditSequence']) != str(db_data['EditSequence']):
                print(f"EditSequence MISMATCH: QB={qb_data['EditSequence']}, DB={db_data['EditSequence']}")

            # Check last sync time vs QB modified time
            if 'LastSyncTime' in db_data:
                print(f"\nLast sync vs QB modified: {compare_timestamps(qb_data['TimeModified'], db_data['LastSyncTime'])}")

                # Parse dates for detailed analysis
                qb_mod_str = str(qb_data['TimeModified']).split('+')[0].strip()
                last_sync_str = str(db_data['LastSyncTime']).split('+')[0].strip()

                if 'T' in qb_mod_str:
                    qb_mod_dt = datetime.datetime.fromisoformat(qb_mod_str)
                else:
                    qb_mod_dt = datetime.datetime.strptime(qb_mod_str, '%Y-%m-%d %H:%M:%S')

                if 'T' in last_sync_str:
                    last_sync_dt = datetime.datetime.fromisoformat(last_sync_str)
                else:
                    last_sync_dt = datetime.datetime.strptime(last_sync_str, '%Y-%m-%d %H:%M:%S')

                if qb_mod_dt < last_sync_dt:
                    print(f"\n⚠️  WARNING: Order was NOT modified in QB since last sync!")
                    print(f"   QB TimeModified: {qb_mod_dt}")
                    print(f"   Last Sync Time:  {last_sync_dt}")
                    print(f"   This explains why the change wasn't synced.")
                    print(f"\n   SOLUTION: Run a FULL SYNC to pick up status changes:")
                    print(f"   python main.py --table sales_orders --full")

    finally:
        quickbooks_logout(qb)

    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()