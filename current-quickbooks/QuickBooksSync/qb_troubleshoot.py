#!/usr/bin/env python3
"""
QuickBooks Connection Troubleshooting Script
Diagnoses QBFC connection issues
"""

import os
import sys
import platform
import subprocess
import winreg
import win32com.client
import pythoncom
from datetime import datetime
import ctypes


def print_header(text):
    """Print a formatted header"""
    print(f"\n{'=' * 60}")
    print(f"{text}")
    print(f"{'=' * 60}")


def print_info(label, value):
    """Print formatted info"""
    print(f"{label:<30}: {value}")


def check_system_info():
    """Check system information"""
    print_header("SYSTEM INFORMATION")
    print_info("Python Version", sys.version)
    print_info("Python Executable", sys.executable)
    print_info("Platform", platform.platform())
    print_info("Machine", platform.machine())
    print_info("Processor", platform.processor())

    # Check if running as admin
    try:
        is_admin = ctypes.windll.shell32.IsUserAnAdmin()
        print_info("Running as Administrator", "Yes" if is_admin else "No")
    except:
        print_info("Running as Administrator", "Unable to determine")


def check_quickbooks_installation():
    """Check QuickBooks installation"""
    print_header("QUICKBOOKS INSTALLATION")

    # Common QuickBooks installation paths
    qb_paths = [
        r"C:\Program Files\Intuit\QuickBooks 2024",
        r"C:\Program Files\Intuit\QuickBooks 2023",
        r"C:\Program Files\Intuit\QuickBooks 2022",
        r"C:\Program Files\Intuit\QuickBooks 2021",
        r"C:\Program Files\Intuit\QuickBooks 2020",
        r"C:\Program Files (x86)\Intuit\QuickBooks Enterprise Solutions 24.0",
        r"C:\Program Files (x86)\Intuit\QuickBooks Enterprise Solutions 23.0",
        r"C:\Program Files (x86)\Intuit\QuickBooks Enterprise Solutions 22.0",
        r"C:\Program Files (x86)\Intuit\QuickBooks Enterprise Solutions 21.0",
        r"C:\Program Files (x86)\Intuit\QuickBooks Enterprise Solutions 20.0",
    ]

    found_qb = False
    for path in qb_paths:
        if os.path.exists(path):
            print(f"✓ Found QuickBooks at: {path}")
            found_qb = True

            # Check for QBW32.exe
            qbw32_path = os.path.join(path, "QBW32.exe")
            if os.path.exists(qbw32_path):
                print(f"  - QBW32.exe found")

                # Get file version
                try:
                    import win32api
                    info = win32api.GetFileVersionInfo(qbw32_path, '\\')
                    ms = info['FileVersionMS']
                    ls = info['FileVersionLS']
                    version = f"{win32api.HIWORD(ms)}.{win32api.LOWORD(ms)}.{win32api.HIWORD(ls)}.{win32api.LOWORD(ls)}"
                    print(f"  - Version: {version}")
                except:
                    pass

    if not found_qb:
        print("✗ No QuickBooks installation found in standard locations")

    # Check if QuickBooks is running
    print("\nChecking if QuickBooks is running...")
    try:
        result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq QBW32.exe'],
                                capture_output=True, text=True)
        if 'QBW32.exe' in result.stdout:
            print("✓ QuickBooks is currently running")
        else:
            print("✗ QuickBooks is NOT running")
    except:
        print("Unable to check if QuickBooks is running")


def check_qbfc_registration():
    """Check QBFC COM registration"""
    print_header("QBFC COM REGISTRATION")

    # QBFC versions to check
    qbfc_versions = [
        ('QBFC16.QBSessionManager', 'QBFC16'),
        ('QBFC15.QBSessionManager', 'QBFC15'),
        ('QBFC14.QBSessionManager', 'QBFC14'),
        ('QBFC13.QBSessionManager', 'QBFC13'),
        ('QBFC12.QBSessionManager', 'QBFC12'),
        ('QBFC11.QBSessionManager', 'QBFC11'),
        ('QBFC10.QBSessionManager', 'QBFC10'),
        ('QBFC9.QBSessionManager', 'QBFC9'),
        ('QBFC8.QBSessionManager', 'QBFC8'),
        ('QBFC7.QBSessionManager', 'QBFC7'),
        ('QBFC.QBSessionManager', 'QBFC (Legacy)'),
    ]

    available_versions = []

    for prog_id, name in qbfc_versions:
        try:
            # Try to create the COM object
            pythoncom.CoInitialize()
            obj = win32com.client.Dispatch(prog_id)
            print(f"✓ {name} - Registered and available")
            available_versions.append(name)

            # Try to get version info
            try:
                version = obj.QBFCVersion
                print(f"  - Version: {version}")
            except:
                pass

            del obj
            pythoncom.CoUninitialize()

        except Exception as e:
            error_code = getattr(e, 'hresult', None)
            if error_code == -2147221005:
                print(f"✗ {name} - Invalid class string (not installed)")
            elif error_code == -2147221164:
                print(f"✗ {name} - Class not registered")
            else:
                print(f"✗ {name} - Error: {str(e)}")

    if not available_versions:
        print("\n⚠️  NO QBFC VERSIONS ARE REGISTERED!")
        print("This is why your sync is failing.")
    else:
        print(f"\nAvailable QBFC versions: {', '.join(available_versions)}")


def check_registry_entries():
    """Check registry for QBFC entries"""
    print_header("REGISTRY ENTRIES")

    # Check HKEY_CLASSES_ROOT for QBFC entries
    try:
        with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, "") as root_key:
            index = 0
            qbfc_entries = []

            while True:
                try:
                    key_name = winreg.EnumKey(root_key, index)
                    if 'QBFC' in key_name and 'QBSessionManager' in key_name:
                        qbfc_entries.append(key_name)
                    index += 1
                except WindowsError:
                    break

            if qbfc_entries:
                print("Found QBFC entries in registry:")
                for entry in sorted(qbfc_entries):
                    print(f"  - {entry}")
            else:
                print("No QBFC entries found in HKEY_CLASSES_ROOT")

    except Exception as e:
        print(f"Error checking registry: {e}")


def check_qbfc_dlls():
    """Check for QBFC DLL files"""
    print_header("QBFC DLL FILES")

    # Common locations for QBFC DLLs
    dll_paths = [
        r"C:\Program Files\Common Files\Intuit\QuickBooks",
        r"C:\Program Files (x86)\Common Files\Intuit\QuickBooks",
        r"C:\Windows\System32",
        r"C:\Windows\SysWOW64",
    ]

    dll_names = [
        "QBFC16.dll",
        "QBFC15.dll",
        "QBFC14.dll",
        "QBFC13.dll",
        "QBFC12.dll",
        "QBFC11.dll",
        "QBFC10.dll",
        "QBFC9.dll",
        "QBFC8.dll",
        "QBFC7.dll",
        "QBFC.dll",
        "QBFCServer.dll",
        "Interop.QBFC16.dll",
        "Interop.QBFC15.dll",
        "Interop.QBFC13.dll",
    ]

    found_dlls = []

    for path in dll_paths:
        if os.path.exists(path):
            for dll in dll_names:
                dll_path = os.path.join(path, dll)
                if os.path.exists(dll_path):
                    file_size = os.path.getsize(dll_path) / 1024  # KB
                    mod_time = datetime.fromtimestamp(os.path.getmtime(dll_path))
                    print(f"✓ Found: {dll_path}")
                    print(f"  - Size: {file_size:.1f} KB")
                    print(f"  - Modified: {mod_time}")
                    found_dlls.append(dll)

    if not found_dlls:
        print("✗ No QBFC DLL files found!")


def test_simple_connection():
    """Test a simple QBFC connection"""
    print_header("TESTING QBFC CONNECTION")

    # Try different methods
    methods = [
        ("win32com.client.Dispatch", lambda: win32com.client.Dispatch("QBFC13.QBSessionManager")),
        ("win32com.client.DispatchEx", lambda: win32com.client.DispatchEx("QBFC13.QBSessionManager")),
        ("CreateObject", lambda: win32com.client.gencache.EnsureDispatch("QBFC13.QBSessionManager")),
    ]

    for method_name, method_func in methods:
        print(f"\nTrying {method_name}...")
        try:
            pythoncom.CoInitialize()
            session_manager = method_func()
            print(f"✓ Successfully created session manager using {method_name}")

            # Try to open connection
            print("  - Attempting to open connection...")
            session_manager.OpenConnection("", "Python Test Connection")
            print("  ✓ Connection opened successfully")

            # Try to begin session
            print("  - Attempting to begin session...")
            session_manager.BeginSession("", 0)  # 0 = open file mode
            print("  ✓ Session started successfully")

            # Close properly
            session_manager.EndSession()
            session_manager.CloseConnection()
            print("  ✓ Connection closed successfully")

            pythoncom.CoUninitialize()
            return True

        except Exception as e:
            print(f"  ✗ Failed: {str(e)}")
            pythoncom.CoUninitialize()

    return False


def suggest_fixes():
    """Suggest fixes based on findings"""
    print_header("SUGGESTED FIXES")

    print("Based on the diagnostics, try these fixes in order:\n")

    print("1. REINSTALL QBFC SDK:")
    print("   - Download QBFC SDK from Intuit Developer site")
    print("   - Current version: https://http-download.intuit.com/http.intuit/Downloads/2022/03/QBFC15_0Installer.exe")
    print("   - Install with Administrator privileges")
    print("   - Restart your computer after installation")

    print("\n2. REGISTER QBFC MANUALLY:")
    print("   Run these commands as Administrator:")
    print("   - regsvr32 \"C:\\Program Files (x86)\\Common Files\\Intuit\\QuickBooks\\QBFC13.dll\"")
    print("   - regsvr32 \"C:\\Program Files (x86)\\Common Files\\Intuit\\QuickBooks\\QBFC15.dll\"")

    print("\n3. CHECK QUICKBOOKS PERMISSIONS:")
    print("   - Open QuickBooks")
    print("   - Go to Edit > Preferences > Integrated Applications")
    print("   - Look for your application and ensure it has access")
    print("   - If not listed, it will be added on first successful connection")

    print("\n4. RUN AS ADMINISTRATOR:")
    print("   - Right-click your Python script or IDE")
    print("   - Select 'Run as administrator'")

    print("\n5. CHECK PYTHON ARCHITECTURE:")
    print("   - Ensure you're using 32-bit Python if QuickBooks is 32-bit")
    print("   - Or use 64-bit Python with 64-bit QuickBooks")
    print(f"   - Your Python is: {platform.machine()}")

    print("\n6. REPAIR QUICKBOOKS INSTALLATION:")
    print("   - Control Panel > Programs > QuickBooks > Repair")
    print("   - This can fix missing or corrupted COM components")


def main():
    """Run all diagnostics"""
    print(f"QuickBooks Connection Troubleshooting - {datetime.now()}")
    print("This script will diagnose why QBFC connection is failing\n")

    # Run all checks
    check_system_info()
    check_quickbooks_installation()
    check_qbfc_registration()
    check_registry_entries()
    check_qbfc_dlls()
    test_simple_connection()
    suggest_fixes()

    print("\n" + "=" * 60)
    print("Diagnostics complete. Share this output for further assistance.")
    print("=" * 60)

    input("\nPress Enter to exit...")


if __name__ == "__main__":
    main()