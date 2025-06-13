# Add this at the top of sync_manager.py after the imports:

# Fix for running from OneDrive location
import os
import sys

# Determine where we're running from
current_dir = os.path.dirname(os.path.abspath(__file__))
print(f"Running from: {current_dir}")

# Check if we're in OneDrive location
if "OneDrive" in current_dir:
    print("Detected OneDrive location, adjusting paths...")

    # Override DATABASE_CONFIG to point to the correct location
    DATABASE_CONFIG = {
        'sqlite': {
            'path': r'C:\Users\radke\quickbooks_data.db'  # Or wherever your database actually is
        }
    }

    # Add QuickBooksSync directory to path so we can find other modules
    quickbooks_sync_dir = r'C:\QuickBooksSync'
    if os.path.exists(quickbooks_sync_dir):
        sys.path.insert(0, quickbooks_sync_dir)
        print(f"Added {quickbooks_sync_dir} to path")


    # Override main_script path
    class QuickBooksSyncManager:
        def find_main_script(self):
            """Find the main.py script in various possible locations"""
            possible_paths = [
                r"C:\QuickBooksSync\main.py",  # Absolute path
                os.path.join(quickbooks_sync_dir, "main.py"),
            ]

            for path in possible_paths:
                if os.path.exists(path):
                    print(f"Found main.py at: {path}")
                    return path

            print("WARNING: Could not find main.py")
            return "main.py"