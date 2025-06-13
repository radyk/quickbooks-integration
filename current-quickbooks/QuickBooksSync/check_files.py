import os
import sys

print("=== Checking QuickBooks Sync Files ===")
print(f"Current directory: {os.getcwd()}")

# Check for sync_manager.py
files_to_check = [
    "sync_manager.py",
    "main.py",
    "config.py",
    "report_manager.py",
    "email_sender.py",
    "goal_tracker3.py",
    "quickbooks_data.db"
]

print("\nChecking for required files:")
for file in files_to_check:
    path = os.path.join(os.getcwd(), file)
    exists = os.path.exists(path)
    size = os.path.getsize(path) if exists else 0
    print(f"{'✓' if exists else '✗'} {file:<25} {'(' + str(size) + ' bytes)' if exists else '(NOT FOUND)'}")

# Try to read first few lines of sync_manager.py
sync_manager_path = "sync_manager.py"
if os.path.exists(sync_manager_path):
    print(f"\nFirst 10 lines of {sync_manager_path}:")
    print("-" * 50)
    try:
        with open(sync_manager_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= 10:
                    break
                print(f"{i + 1:3}: {line.rstrip()}")
    except Exception as e:
        print(f"Error reading file: {e}")

    # Check if it's empty or very small
    size = os.path.getsize(sync_manager_path)
    if size < 100:
        print(f"\n⚠️  WARNING: sync_manager.py is very small ({size} bytes) - might be corrupted or empty!")

# Try importing sync_manager
print("\n\nTrying to import sync_manager...")
try:
    import sync_manager

    print("✓ Successfully imported sync_manager")

    # Check if main exists
    if hasattr(sync_manager, 'main'):
        print("✓ main() function found")
    else:
        print("✗ main() function NOT found!")

    # Check for QuickBooksSyncManager class
    if hasattr(sync_manager, 'QuickBooksSyncManager'):
        print("✓ QuickBooksSyncManager class found")
    else:
        print("✗ QuickBooksSyncManager class NOT found!")

except Exception as e:
    print(f"✗ Failed to import: {e}")
    import traceback

    traceback.print_exc()

input("\nPress Enter to exit...")