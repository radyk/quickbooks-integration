#!/usr/bin/env python3
"""
QuickBooks Sync Manager - Interactive Interface
A user-friendly interface for managing QuickBooks synchronization
Includes Goal Tracker report automation
"""
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import threading
import subprocess
import sys
import os
import time
import datetime
import json
import queue
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
import sqlite3
import signal
import platform

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Try importing from different potential paths
try:
    from config import DATABASE_CONFIG, QB_CONFIG, TABLE_CONFIGS
    from database.sqlite_db import SQLiteDatabase
except ImportError:
    try:
        # Try without database subfolder
        from sqlite_db import SQLiteDatabase
        from config import DATABASE_CONFIG, QB_CONFIG, TABLE_CONFIGS
    except ImportError:
        # If all else fails, define minimal configs
        print("Warning: Could not import config files. Using defaults.")
        DATABASE_CONFIG = {
            'sqlite': {
                'path': r'C:\Users\radke\quickbooks_data.db'
            }
        }
        QB_CONFIG = {}
        TABLE_CONFIGS = []


        # Create a minimal SQLiteDatabase class
        class SQLiteDatabase:
            def __init__(self, config):
                self.db_path = config['path']
                self.connection = None

            def connect(self):
                import sqlite3
                self.connection = sqlite3.connect(self.db_path)
                self.connection.row_factory = sqlite3.Row

            def disconnect(self):
                if self.connection:
                    self.connection.close()
                    self.connection = None

            def verify_database(self):
                import os
                return {
                    'exists': os.path.exists(self.db_path),
                    'size_mb': os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0,
                    'tables': [],
                    'custom_fields_count': 0
                }


class QuickBooksSyncManager:
    def __init__(self, root, auto_start_minutes=None, minimize_on_start=False):
        self.root = root
        self.root.title("QuickBooks Sync Manager")

        # Get screen dimensions for better sizing
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()

        # Set window size based on screen resolution
        if screen_height <= 768:
            # Low resolution screen
            window_width = min(1000, int(screen_width * 0.95))
            window_height = min(700, int(screen_height * 0.95))
        else:
            # Normal resolution
            window_width = min(1200, int(screen_width * 0.8))
            window_height = min(800, int(screen_height * 0.8))

        self.root.geometry(f"{window_width}x{window_height}")
        self.root.minsize(800, 600)  # Minimum size

        # Store auto-start settings
        self.auto_start_minutes = auto_start_minutes
        self.minimize_on_start = minimize_on_start

        # Set icon if available
        try:
            self.root.iconbitmap('qb_icon.ico')
        except:
            pass

        # Variables
        self.sync_process = None
        self.output_queue = queue.Queue()
        self.timer_active = False
        self.timer_thread = None
        self.last_sync_time = None
        self.next_sync_time = None
        self.countdown_seconds = 0

        # Process tracking improvements
        self.sync_start_time = None
        self.sync_timeout = 3600  # 1 hour default timeout
        self.last_output_time = None
        self.output_stall_timeout = 300  # 5 minutes without output = stalled

        # Find main.py script
        self.main_script = self.find_main_script()

        # Create main container with scrolling
        self.create_main_container()

        # Create UI
        self.create_output_console()  # Create console FIRST
        self.create_widgets()  # Then create other widgets
        self.load_sync_status()  # Then load data

        # Start output monitoring
        self.monitor_output()

        # Start process health monitoring
        self.check_process_health()

        # Auto-start timer if requested
        if self.auto_start_minutes:
            # Set the interval
            self.interval_var.set(str(self.auto_start_minutes))
            # Start the timer after GUI is ready
            self.root.after(1000, self.auto_start_timer)

            # Minimize window if requested
            if self.minimize_on_start:
                self.root.after(1500, self.minimize_window)

        # Center window on screen
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')

    def check_process_health(self):
        """Periodically check if sync process is healthy"""
        if self.sync_process:
            poll_result = self.sync_process.poll()

            if poll_result is None:
                # Process is still running
                if self.sync_start_time:
                    elapsed = time.time() - self.sync_start_time

                    # Check for timeout
                    if elapsed > self.sync_timeout:
                        self.log_output(f"\n!!! SYNC TIMEOUT: Process running for {elapsed / 60:.1f} minutes !!!")
                        self.force_stop_sync()

                    # Check for output stall
                    elif self.last_output_time:
                        output_elapsed = time.time() - self.last_output_time
                        if output_elapsed > self.output_stall_timeout:
                            self.log_output(f"\n!!! SYNC STALLED: No output for {output_elapsed / 60:.1f} minutes !!!")
                            self.force_stop_sync()
            else:
                # Process terminated
                if poll_result != 0:
                    self.log_output(f"\n=== SYNC PROCESS TERMINATED WITH ERROR CODE: {poll_result} ===")
                self.sync_process = None
                self.sync_start_time = None
                self.last_output_time = None

        # Schedule next health check
        self.root.after(5000, self.check_process_health)  # Check every 5 seconds

    def is_sync_running(self):
        """Check if sync is actually running (not just stalled)"""
        if not self.sync_process:
            return False

        # Check if process is alive
        poll_result = self.sync_process.poll()
        if poll_result is not None:
            # Process has terminated
            self.sync_process = None
            self.sync_start_time = None
            self.last_output_time = None
            return False

        # Check for timeout
        if self.sync_start_time:
            elapsed = time.time() - self.sync_start_time
            if elapsed > self.sync_timeout:
                self.log_output(f"\nSync process appears stalled (running for {elapsed / 60:.1f} minutes)")
                return False

        return True

    def kill_orphaned_processes(self):
        """Kill any orphaned Python processes running main.py"""
        self.log_output("\nChecking for orphaned sync processes...")
        killed_count = 0

        try:
            if sys.platform == 'win32':
                # Windows: Use WMI to find and kill processes
                import subprocess

                # Get current process ID to avoid killing ourselves
                current_pid = os.getpid()

                # Find Python processes with main.py in command line
                cmd = 'wmic process where "name=\'python.exe\'" get ProcessId,CommandLine /FORMAT:CSV'
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

                if result.returncode == 0:
                    lines = result.stdout.strip().split('\n')
                    for line in lines:
                        if 'main.py' in line and 'sync_manager.py' not in line:
                            # Extract PID from the line
                            parts = line.split(',')
                            if len(parts) >= 3:
                                try:
                                    pid = int(parts[-1])
                                    if pid != current_pid:
                                        self.log_output(f"Killing orphaned process PID: {pid}")
                                        subprocess.call(['taskkill', '/F', '/PID', str(pid)])
                                        killed_count += 1
                                except (ValueError, subprocess.CalledProcessError):
                                    continue

                # Alternative method using PowerShell
                if killed_count == 0:
                    ps_cmd = 'Get-Process python | Where-Object {$_.CommandLine -like "*main.py*"} | Stop-Process -Force'
                    subprocess.run(['powershell', '-Command', ps_cmd], capture_output=True)

            else:
                # Unix/Linux: Use ps and grep
                result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
                for line in result.stdout.split('\n'):
                    if 'python' in line and 'main.py' in line and 'sync_manager.py' not in line:
                        parts = line.split()
                        if len(parts) > 1:
                            try:
                                pid = int(parts[1])
                                os.kill(pid, signal.SIGTERM)
                                killed_count += 1
                                self.log_output(f"Killed orphaned process PID: {pid}")
                            except (ValueError, ProcessLookupError):
                                continue

        except Exception as e:
            self.log_output(f"Error checking for orphaned processes: {e}")

        if killed_count > 0:
            self.log_output(f"Cleaned up {killed_count} orphaned processes")
        else:
            self.log_output("No orphaned processes found")

    def run_command(self, cmd, message):
        """Run a command and capture output - IMPROVED VERSION"""
        # Clean up any dead process reference
        if self.sync_process and self.sync_process.poll() is not None:
            self.sync_process = None
            self.sync_start_time = None
            self.last_output_time = None

        # Check if already running
        if self.is_sync_running():
            messagebox.showwarning("Sync Running", "A sync is already in progress!")
            return

        # Kill any orphaned processes before starting
        self.kill_orphaned_processes()

        self.log_output(f"\n{message}")
        self.log_output(f"Command: {' '.join(cmd)}")
        self.log_output("-" * 80)

        # Record start time
        self.sync_start_time = time.time()
        self.last_output_time = time.time()

        try:
            # Platform-specific process creation
            if sys.platform == 'win32':
                # Windows: use CREATE_NEW_PROCESS_GROUP for better control
                self.sync_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                    env={**os.environ, 'PYTHONUNBUFFERED': '1'}
                )
            else:
                # Unix/Linux: use process group
                self.sync_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                    preexec_fn=os.setsid,
                    env={**os.environ, 'PYTHONUNBUFFERED': '1'}
                )

            self.log_output(f"Started process with PID: {self.sync_process.pid}")

            # Start thread to read output
            thread = threading.Thread(target=self.read_process_output, daemon=True)
            thread.start()

        except Exception as e:
            self.log_output(f"Error starting process: {e}")
            self.sync_process = None
            self.sync_start_time = None
            self.last_output_time = None

    def read_process_output(self):
        """Read process output in a thread"""
        try:
            for line in iter(self.sync_process.stdout.readline, ''):
                if line:
                    # Update last output time
                    self.last_output_time = time.time()

                    line = line.rstrip()

                    # Handle progress lines differently (they use \r)
                    if "Processing records" in line or "Processed" in line:
                        # Progress update - update the last line instead of adding new
                        self.output_queue.put(('progress', line))
                    else:
                        # Regular output
                        self.output_queue.put(('normal', line))

            self.sync_process.wait()

            # Process finished
            if self.sync_process.returncode == 0:
                self.output_queue.put(('normal', "\n=== PROCESS COMPLETED SUCCESSFULLY ==="))
            else:
                self.output_queue.put(
                    ('normal', f"\n=== PROCESS FAILED (Exit code: {self.sync_process.returncode}) ==="))

            # Clear process references
            self.sync_process = None
            self.sync_start_time = None
            self.last_output_time = None

            # Refresh status after sync
            self.root.after(1000, self.load_sync_status)

            # Check if Goal Tracker should run after successful sync
            if self.sync_process and self.sync_process.returncode == 0:
                self.root.after(2000, self.check_goal_tracker_schedule)

        except Exception as e:
            self.output_queue.put(('normal', f"Error reading output: {e}"))
            self.sync_process = None
            self.sync_start_time = None
            self.last_output_time = None

    def stop_sync(self):
        """Stop running sync process - IMPROVED VERSION"""
        if self.sync_process:
            pid = self.sync_process.pid
            poll_result = self.sync_process.poll()

            if poll_result is None:
                # Process is still running
                self.log_output(f"\n=== STOPPING SYNC PROCESS (PID: {pid}) ===")

                try:
                    if sys.platform == 'win32':
                        # Windows: Try graceful shutdown first
                        try:
                            self.sync_process.terminate()
                            # Give it 5 seconds to terminate
                            self.sync_process.wait(timeout=5)
                            self.log_output("Process terminated gracefully")
                        except subprocess.TimeoutExpired:
                            # Force kill using taskkill
                            subprocess.call(['taskkill', '/F', '/T', '/PID', str(pid)])
                            self.log_output("Process force killed")
                    else:
                        # Unix/Linux: Kill process group
                        try:
                            os.killpg(os.getpgid(pid), signal.SIGTERM)
                            self.sync_process.wait(timeout=5)
                            self.log_output("Process terminated gracefully")
                        except subprocess.TimeoutExpired:
                            os.killpg(os.getpgid(pid), signal.SIGKILL)
                            self.log_output("Process force killed")

                except Exception as e:
                    self.log_output(f"Error stopping process: {e}")
                    self.force_stop_sync()
            else:
                self.log_output("Process was already terminated")

            # Clean up
            self.sync_process = None
            self.sync_start_time = None
            self.last_output_time = None

        else:
            # No process reference, check for orphans
            self.kill_orphaned_processes()
            messagebox.showinfo("No Process", "No sync process is currently running.")

    def force_stop_sync(self):
        """Force kill the sync process and any children"""
        if self.sync_process:
            try:
                pid = self.sync_process.pid
                self.log_output(f"Force killing sync process {pid}...")

                if sys.platform == 'win32':
                    # Windows: Kill process tree
                    subprocess.call(['taskkill', '/F', '/T', '/PID', str(pid)])
                    # Also try WMI method
                    subprocess.run(
                        f'wmic process where "ParentProcessId={pid}" delete',
                        shell=True, capture_output=True
                    )
                else:
                    # Unix: Kill process group
                    try:
                        os.killpg(os.getpgid(pid), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

                self.log_output("Process force killed")

            except Exception as e:
                self.log_output(f"Error force killing process: {e}")

            finally:
                self.sync_process = None
                self.sync_start_time = None
                self.last_output_time = None

        # Always check for orphans
        self.kill_orphaned_processes()

    def auto_start_timer(self):
        """Auto-start the timer with command-line specified interval"""
        self.log_output(f"\n=== AUTO-STARTING TIMER WITH {self.auto_start_minutes} MINUTE INTERVAL ===")
        self.start_timer()

    def minimize_window(self):
        """Minimize the window to system tray/taskbar"""
        self.root.iconify()
        self.log_output("Window minimized to taskbar")

    def find_main_script(self):
        """Find the main.py script in various possible locations"""
        possible_paths = [
            Path(__file__).parent / "main.py",
            Path(__file__).parent.parent / "main.py",
            Path(__file__).parent / "sync" / "main.py",
            Path.cwd() / "main.py"
        ]

        for path in possible_paths:
            if path.exists():
                return str(path)

        # If not found, return "main.py" and hope it's in PATH
        return "main.py"

    def create_main_container(self):
        """Create main container with scrolling for low resolution screens"""
        # Create main container frame
        self.main_container = ttk.Frame(self.root)
        self.main_container.pack(fill='both', expand=True)

        # Create canvas and scrollbar for scrolling
        self.canvas = tk.Canvas(self.main_container)
        self.v_scrollbar = ttk.Scrollbar(self.main_container, orient='vertical', command=self.canvas.yview)
        self.h_scrollbar = ttk.Scrollbar(self.main_container, orient='horizontal', command=self.canvas.xview)

        # Create frame inside canvas for content
        self.scrollable_frame = ttk.Frame(self.canvas)

        # Configure canvas
        self.canvas.configure(
            yscrollcommand=self.v_scrollbar.set,
            xscrollcommand=self.h_scrollbar.set
        )

        # Create window in canvas
        self.canvas_window = self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor='nw')

        # Configure scrolling
        self.scrollable_frame.bind('<Configure>', self._on_frame_configure)
        self.canvas.bind('<Configure>', self._on_canvas_configure)

        # Pack scrollbars and canvas
        self.v_scrollbar.pack(side='right', fill='y')
        self.h_scrollbar.pack(side='bottom', fill='x')
        self.canvas.pack(side='left', fill='both', expand=True)

        # Bind mouse wheel for scrolling
        self.canvas.bind_all('<MouseWheel>', self._on_mousewheel)
        self.canvas.bind_all('<Button-4>', self._on_mousewheel)
        self.canvas.bind_all('<Button-5>', self._on_mousewheel)

    def _on_frame_configure(self, event=None):
        """Update scroll region when frame size changes"""
        self.canvas.configure(scrollregion=self.canvas.bbox('all'))

    def _on_canvas_configure(self, event=None):
        """Update frame width when canvas size changes"""
        canvas_width = event.width if event else self.canvas.winfo_width()
        self.canvas.itemconfig(self.canvas_window, width=canvas_width)

    def _on_mousewheel(self, event):
        """Handle mouse wheel scrolling"""
        if event.delta:
            # Windows
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        else:
            # Linux
            if event.num == 4:
                self.canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                self.canvas.yview_scroll(1, "units")

    def create_widgets(self):
        """Create all UI widgets"""
        # Create notebook for tabs INSIDE the scrollable frame
        self.notebook = ttk.Notebook(self.scrollable_frame)
        self.notebook.pack(fill='both', expand=True, padx=5, pady=5)

        # Tab 1: Main Sync Controls
        self.sync_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.sync_frame, text="Sync Controls")
        self.create_sync_controls()

        # Tab 2: Table Selection
        self.table_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.table_frame, text="Table Selection")
        self.create_table_selection()

        # Tab 3: Price Analysis
        self.price_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.price_frame, text="Price Analysis")
        self.create_price_controls()

        # Tab 4: Timer Settings
        self.timer_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.timer_frame, text="Auto Sync Timer")
        self.create_timer_controls()

        # Tab 5: Database Info
        self.db_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.db_frame, text="Database Info")
        self.create_db_info()

        # Tab 6: Goal Tracker Reports
        self.reports_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.reports_frame, text="Goal Tracker")
        self.create_goal_tracker_controls()

    def create_sync_controls(self):
        """Create main sync control widgets"""
        # Control buttons frame
        control_frame = ttk.LabelFrame(self.sync_frame, text="Sync Controls", padding=10)
        control_frame.pack(fill='x', padx=10, pady=10)

        # Full sync checkbox
        self.full_sync_var = tk.BooleanVar()
        ttk.Checkbutton(control_frame, text="Full Sync (ignore last sync time)",
                        variable=self.full_sync_var).grid(row=0, column=0, sticky='w', padx=5)

        # Skip auto-analysis checkbox
        self.skip_analysis_var = tk.BooleanVar()
        ttk.Checkbutton(control_frame, text="Skip auto open order analysis",
                        variable=self.skip_analysis_var).grid(row=0, column=1, sticky='w', padx=5)

        # Sync buttons
        button_frame = ttk.Frame(control_frame)
        button_frame.grid(row=1, column=0, columnspan=3, pady=10)

        ttk.Button(button_frame, text="Sync All Tables",
                   command=self.sync_all_tables, width=20).pack(side='left', padx=5)

        ttk.Button(button_frame, text="Sync Selected Tables",
                   command=self.sync_selected_tables, width=20).pack(side='left', padx=5)

        ttk.Button(button_frame, text="Test Connection",
                   command=self.test_connection, width=20).pack(side='left', padx=5)

        ttk.Button(button_frame, text="Stop Sync",
                   command=self.stop_sync, width=20).pack(side='left', padx=5)

        # Status frame
        status_frame = ttk.LabelFrame(self.sync_frame, text="Sync Status", padding=10)
        status_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Create treeview for sync status
        columns = ('Table', 'Last Sync', 'Records', 'Duration', 'Status')
        self.status_tree = ttk.Treeview(status_frame, columns=columns, show='tree headings', height=15)

        # Define column headings
        self.status_tree.heading('#0', text='')
        self.status_tree.heading('Table', text='Table')
        self.status_tree.heading('Last Sync', text='Last Sync')
        self.status_tree.heading('Records', text='Records')
        self.status_tree.heading('Duration', text='Duration')
        self.status_tree.heading('Status', text='Status')

        # Column widths
        self.status_tree.column('#0', width=0, stretch=False)
        self.status_tree.column('Table', width=200)
        self.status_tree.column('Last Sync', width=180)
        self.status_tree.column('Records', width=100)
        self.status_tree.column('Duration', width=100)
        self.status_tree.column('Status', width=150)

        # Scrollbar
        scrollbar = ttk.Scrollbar(status_frame, orient='vertical', command=self.status_tree.yview)
        self.status_tree.configure(yscroll=scrollbar.set)

        self.status_tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        # Refresh button
        ttk.Button(status_frame, text="Refresh Status",
                   command=self.load_sync_status).pack(pady=5)

    def create_table_selection(self):
        """Create table selection widgets"""
        # Instructions
        ttk.Label(self.table_frame, text="Select specific tables to sync:").pack(pady=5)

        # Create frame with scrollbar
        list_frame = ttk.Frame(self.table_frame)
        list_frame.pack(fill='both', expand=True, padx=10, pady=5)

        # Scrollbar
        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side='right', fill='y')

        # Listbox for tables
        self.table_listbox = tk.Listbox(list_frame, selectmode='multiple',
                                        yscrollcommand=scrollbar.set, height=20)
        self.table_listbox.pack(side='left', fill='both', expand=True)
        scrollbar.config(command=self.table_listbox.yview)

        # Populate with tables
        for config in TABLE_CONFIGS:
            self.table_listbox.insert('end', config['name'])

        # Selection buttons
        button_frame = ttk.Frame(self.table_frame)
        button_frame.pack(pady=5)

        ttk.Button(button_frame, text="Select All",
                   command=lambda: self.table_listbox.select_set(0, 'end')).pack(side='left', padx=5)

        ttk.Button(button_frame, text="Clear All",
                   command=lambda: self.table_listbox.select_clear(0, 'end')).pack(side='left', padx=5)

        ttk.Button(button_frame, text="Select High Priority",
                   command=self.select_high_priority).pack(side='left', padx=5)

    def create_price_controls(self):
        """Create price analysis controls"""
        # Open order analysis
        order_frame = ttk.LabelFrame(self.price_frame, text="Open Order Price Analysis", padding=10)
        order_frame.pack(fill='x', padx=10, pady=10)

        ttk.Label(order_frame, text="Analyze pricing on open sales orders vs QuickBooks calculated prices").pack()

        max_frame = ttk.Frame(order_frame)
        max_frame.pack(pady=5)

        ttk.Label(max_frame, text="Max orders to analyze (blank = all):").pack(side='left', padx=5)
        self.max_orders_var = tk.StringVar()
        ttk.Entry(max_frame, textvariable=self.max_orders_var, width=10).pack(side='left')

        ttk.Button(order_frame, text="Analyze Open Orders",
                   command=self.analyze_open_orders, width=30).pack(pady=5)

        # Historical price extraction
        hist_frame = ttk.LabelFrame(self.price_frame, text="Historical Price Extraction", padding=10)
        hist_frame.pack(fill='x', padx=10, pady=10)

        ttk.Label(hist_frame, text="Extract customer-item prices from historical sales orders").pack()

        months_frame = ttk.Frame(hist_frame)
        months_frame.pack(pady=5)

        ttk.Label(months_frame, text="Months of history:").pack(side='left', padx=5)
        self.months_var = tk.StringVar(value="6")
        ttk.Spinbox(months_frame, from_=1, to=24, textvariable=self.months_var, width=5).pack(side='left')

        # Rebuild option
        self.rebuild_var = tk.BooleanVar()
        ttk.Checkbutton(hist_frame, text="Clear and rebuild entire price table",
                        variable=self.rebuild_var).pack(pady=5)

        ttk.Button(hist_frame, text="Extract Historical Prices",
                   command=self.extract_historical_prices, width=30).pack(pady=5)

        # Offline QuickBooks file option
        offline_frame = ttk.LabelFrame(self.price_frame, text="Offline QuickBooks File (Price Rebuild)", padding=10)
        offline_frame.pack(fill='x', padx=10, pady=10)

        ttk.Label(offline_frame, text="Use an offline QuickBooks backup file for price extraction:").pack()

        file_frame = ttk.Frame(offline_frame)
        file_frame.pack(pady=5, fill='x')

        self.offline_qb_var = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.offline_qb_var, width=60).pack(side='left', padx=5)
        ttk.Button(file_frame, text="Browse...",
                   command=self.browse_qb_file).pack(side='left')
        ttk.Button(file_frame, text="Clear",
                   command=lambda: self.offline_qb_var.set("")).pack(side='left', padx=5)

        ttk.Label(offline_frame, text="Note: This allows price rebuild without affecting production QB file",
                  font=('Arial', 9, 'italic')).pack(pady=2)

        # Combined analysis
        ttk.Separator(self.price_frame, orient='horizontal').pack(fill='x', padx=10, pady=20)

        ttk.Button(self.price_frame, text="Run Both Price Analyses",
                   command=self.run_both_analyses, width=30).pack()

    def create_timer_controls(self):
        """Create auto-sync timer controls"""
        # Timer settings
        settings_frame = ttk.LabelFrame(self.timer_frame, text="Auto-Sync Settings", padding=10)
        settings_frame.pack(fill='x', padx=10, pady=10)

        # Interval setting
        interval_frame = ttk.Frame(settings_frame)
        interval_frame.pack(pady=5)

        ttk.Label(interval_frame, text="Sync interval (minutes):").pack(side='left', padx=5)
        self.interval_var = tk.StringVar(value="60")
        ttk.Spinbox(interval_frame, from_=5, to=1440, textvariable=self.interval_var, width=10).pack(side='left')

        # Timer controls
        button_frame = ttk.Frame(settings_frame)
        button_frame.pack(pady=10)

        self.timer_button = ttk.Button(button_frame, text="Start Timer",
                                       command=self.toggle_timer, width=20)
        self.timer_button.pack(side='left', padx=5)

        # Timer status
        self.timer_status = ttk.Label(settings_frame, text="Timer: Inactive", font=('Arial', 12, 'bold'))
        self.timer_status.pack(pady=5)

        self.next_sync_label = ttk.Label(settings_frame, text="")
        self.next_sync_label.pack()

        self.last_sync_label = ttk.Label(settings_frame, text="")
        self.last_sync_label.pack()

        # Progress bar
        self.timer_progress = ttk.Progressbar(settings_frame, mode='determinate', length=400)
        self.timer_progress.pack(pady=10)

        # Schedule display
        schedule_frame = ttk.LabelFrame(self.timer_frame, text="Sync Schedule", padding=10)
        schedule_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Create text widget for schedule
        self.schedule_text = scrolledtext.ScrolledText(schedule_frame, height=15, width=80)
        self.schedule_text.pack(fill='both', expand=True)

        self.load_sync_schedule()

    def create_db_info(self):
        """Create database info display"""
        # Database path
        info_frame = ttk.LabelFrame(self.db_frame, text="Database Information", padding=10)
        info_frame.pack(fill='x', padx=10, pady=10)

        db_path = DATABASE_CONFIG['sqlite']['path']
        ttk.Label(info_frame, text=f"Database: {db_path}").pack(anchor='w', pady=2)

        # Database stats will be loaded here
        self.db_stats_text = scrolledtext.ScrolledText(info_frame, height=10, width=80)
        self.db_stats_text.pack(fill='both', expand=True, pady=5)

        # Buttons
        button_frame = ttk.Frame(info_frame)
        button_frame.pack(pady=5)

        ttk.Button(button_frame, text="Verify Database",
                   command=self.verify_database).pack(side='left', padx=5)

        ttk.Button(button_frame, text="Open DB Location",
                   command=self.open_db_location).pack(side='left', padx=5)

        # Additional tools
        tools_frame = ttk.LabelFrame(self.db_frame, text="Database Tools", padding=10)
        tools_frame.pack(fill='x', padx=10, pady=10)

        ttk.Button(tools_frame, text="List All Tables",
                   command=self.list_all_tables, width=20).pack(pady=2)

        ttk.Button(tools_frame, text="Show LinkedTxn Stats",
                   command=self.show_linkedtxn_stats, width=20).pack(pady=2)

        ttk.Button(tools_frame, text="Export Sync Log",
                   command=self.export_sync_log, width=20).pack(pady=2)

    def create_goal_tracker_controls(self):
        """Create Goal Tracker report controls"""
        try:
            from report_manager import ReportManager
            has_report_manager = True
        except ImportError:
            has_report_manager = False
            ttk.Label(self.reports_frame, text="Report Manager not found. Please install report_manager.py",
                      foreground='red').pack(pady=20)
            return

        # Configuration frame
        config_frame = ttk.LabelFrame(self.reports_frame, text="Goal Tracker Configuration", padding=10)
        config_frame.pack(fill='x', padx=10, pady=10)

        # Enable/disable
        self.goal_tracker_enabled = tk.BooleanVar(value=True)
        ttk.Checkbutton(config_frame, text="Enable automatic Goal Tracker generation",
                        variable=self.goal_tracker_enabled,
                        command=self.save_goal_tracker_config).grid(row=0, column=0, columnspan=2, sticky='w', pady=5)

        # Preferred time
        time_frame = ttk.Frame(config_frame)
        time_frame.grid(row=1, column=0, columnspan=2, sticky='w', pady=5)

        ttk.Label(time_frame, text="Preferred time:").pack(side='left', padx=(0, 5))
        self.goal_tracker_time = tk.StringVar(value="08:00")
        time_spinbox = ttk.Spinbox(time_frame, textvariable=self.goal_tracker_time,
                                   values=[f"{h:02d}:00" for h in range(24)],
                                   width=8, state='readonly')
        time_spinbox.pack(side='left')

        ttk.Label(time_frame, text="Window (minutes):").pack(side='left', padx=(20, 5))
        self.goal_tracker_window = tk.IntVar(value=120)
        ttk.Spinbox(time_frame, from_=30, to=240, increment=30,
                    textvariable=self.goal_tracker_window, width=8).pack(side='left')

        # Days of week
        days_frame = ttk.LabelFrame(config_frame, text="Run on these days", padding=5)
        days_frame.grid(row=2, column=0, columnspan=2, sticky='ew', pady=10)

        self.day_vars = {}
        days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        for i, day in enumerate(days):
            var = tk.BooleanVar(value=(i < 5))  # Default M-F
            self.day_vars[day] = var
            ttk.Checkbutton(days_frame, text=day, variable=var,
                            command=self.save_goal_tracker_config).grid(row=0, column=i, padx=5)

        # Business days only
        self.business_days_only = tk.BooleanVar(value=True)
        ttk.Checkbutton(config_frame, text="Skip weekends and holidays",
                        variable=self.business_days_only,
                        command=self.save_goal_tracker_config).grid(row=3, column=0, sticky='w', pady=5)

        # Email settings
        email_frame = ttk.LabelFrame(config_frame, text="Email Settings", padding=5)
        email_frame.grid(row=4, column=0, columnspan=2, sticky='ew', pady=10)

        self.email_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(email_frame, text="Email report after generation",
                        variable=self.email_enabled,
                        command=self.save_goal_tracker_config).pack(anchor='w')

        ttk.Label(email_frame, text="Recipients (comma-separated):").pack(anchor='w', pady=(5, 0))
        self.email_recipients = tk.StringVar()
        ttk.Entry(email_frame, textvariable=self.email_recipients, width=60).pack(fill='x', pady=2)

        # Email server config buttons
        email_button_frame = ttk.Frame(email_frame)
        email_button_frame.pack(pady=5)

        ttk.Button(email_button_frame, text="Configure Email Server",
                   command=self.configure_email_server).pack(side='left', padx=5)

        ttk.Button(email_button_frame, text="Test Email",
                   command=self.test_email).pack(side='left', padx=5)

        # Save button for email
        ttk.Button(email_frame, text="Save Email Settings",
                   command=self.save_goal_tracker_config).pack(pady=5)

        # Rep filter
        rep_frame = ttk.LabelFrame(config_frame, text="Sales Rep Filter", padding=5)
        rep_frame.grid(row=5, column=0, columnspan=2, sticky='ew', pady=10)

        self.filter_reps = tk.BooleanVar(value=True)
        ttk.Checkbutton(rep_frame, text="Filter by specific reps",
                        variable=self.filter_reps,
                        command=self.toggle_rep_list).pack(anchor='w')

        # Rep selection
        self.selected_reps = tk.StringVar(value="AL,CL,GM,HA,KG,PC,YD")
        self.rep_entry = ttk.Entry(rep_frame, textvariable=self.selected_reps, width=60)
        self.rep_entry.pack(fill='x', pady=2)

        ttk.Label(rep_frame, text="Enter rep codes separated by commas",
                  font=('Arial', 9, 'italic')).pack(anchor='w')

        # Manual generation
        manual_frame = ttk.LabelFrame(self.reports_frame, text="Manual Generation", padding=10)
        manual_frame.pack(fill='x', padx=10, pady=10)

        ttk.Button(manual_frame, text="Generate Report Now",
                   command=self.generate_goal_tracker_manual,
                   width=25).pack(side='left', padx=5)

        ttk.Button(manual_frame, text="View Last Report",
                   command=self.view_last_report,
                   width=25).pack(side='left', padx=5)

        ttk.Button(manual_frame, text="Open Reports Folder",
                   command=self.open_reports_folder,
                   width=25).pack(side='left', padx=5)

        # Status display
        status_frame = ttk.LabelFrame(self.reports_frame, text="Generation History", padding=10)
        status_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Create treeview for history
        columns = ('Date', 'Time', 'Trigger', 'Status', 'File Size', 'Duration')
        self.history_tree = ttk.Treeview(status_frame, columns=columns, show='tree headings', height=10)

        # Define columns
        self.history_tree.heading('#0', text='')
        self.history_tree.heading('Date', text='Date')
        self.history_tree.heading('Time', text='Time')
        self.history_tree.heading('Trigger', text='Trigger')
        self.history_tree.heading('Status', text='Status')
        self.history_tree.heading('File Size', text='Size')
        self.history_tree.heading('Duration', text='Duration')

        # Column widths
        self.history_tree.column('#0', width=0, stretch=False)
        self.history_tree.column('Date', width=100)
        self.history_tree.column('Time', width=80)
        self.history_tree.column('Trigger', width=100)
        self.history_tree.column('Status', width=100)
        self.history_tree.column('File Size', width=80)
        self.history_tree.column('Duration', width=80)

        # Scrollbar
        scrollbar = ttk.Scrollbar(status_frame, orient='vertical', command=self.history_tree.yview)
        self.history_tree.configure(yscroll=scrollbar.set)

        self.history_tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        # Refresh button
        ttk.Button(status_frame, text="Refresh History",
                   command=self.load_goal_tracker_history).pack(pady=5)

        # Load current config and history - but delay to avoid database conflicts
        if has_report_manager:
            self.root.after(500, self.load_goal_tracker_config)
            self.root.after(1000, self.load_goal_tracker_history)

    def create_output_console(self):
        """Create output console at bottom"""
        # Adjust console height based on screen resolution
        screen_height = self.root.winfo_screenheight()
        if screen_height <= 768:
            console_height = 6  # Even smaller console for very low res
        elif screen_height <= 900:
            console_height = 8  # Smaller console for low res
        else:
            console_height = 10

        # Console frame goes directly in root, not in scrollable area
        console_frame = ttk.LabelFrame(self.root, text="Output Console", padding=5)
        console_frame.pack(side='bottom', fill='x', padx=5, pady=5)

        # Text widget with scrollbar
        self.console_text = scrolledtext.ScrolledText(console_frame, height=console_height,
                                                      bg='black', fg='white',
                                                      font=('Consolas', 9))
        self.console_text.pack(fill='both', expand=True)

        # Console controls
        control_frame = ttk.Frame(console_frame)
        control_frame.pack(fill='x', pady=2)

        ttk.Button(control_frame, text="Clear",
                   command=self.clear_console).pack(side='left', padx=5)

        ttk.Button(control_frame, text="Save Log",
                   command=self.save_console_log).pack(side='left', padx=5)

        self.autoscroll_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(control_frame, text="Auto-scroll",
                        variable=self.autoscroll_var).pack(side='left', padx=5)

    def load_sync_status(self):
        """Load and display sync status from database"""
        try:
            db = SQLiteDatabase(DATABASE_CONFIG['sqlite'])
            db.connect()

            # Clear existing items
            for item in self.status_tree.get_children():
                self.status_tree.delete(item)

            # Get sync log data
            cursor = db.connection.cursor()
            cursor.execute("""
                SELECT table_name, last_sync_time, record_count, 
                       sync_duration_seconds, last_status, consecutive_failures
                FROM sync_log
                ORDER BY table_name
            """)

            for row in cursor.fetchall():
                table_name = row[0]
                last_sync = row[1] if row[1] else "Never"
                record_count = f"{row[2]:,}" if row[2] else "0"
                duration = f"{row[3]:.1f}s" if row[3] else "-"
                status = row[4] if row[4] else "Unknown"
                failures = row[5] if row[5] else 0

                if failures > 0 and status != "SUCCESS":
                    status = f"{status} ({failures} failures)"

                # Color code based on status
                tags = []
                if status == "SUCCESS":
                    tags = ['success']
                elif "ERROR" in status or "LOCKED" in status:
                    tags = ['error']
                elif "BUSY" in status or "EDITING" in status:
                    tags = ['warning']

                self.status_tree.insert('', 'end', values=(
                    table_name, last_sync, record_count, duration, status
                ), tags=tags)

            # Configure tags
            self.status_tree.tag_configure('success', foreground='green')
            self.status_tree.tag_configure('error', foreground='red')
            self.status_tree.tag_configure('warning', foreground='orange')

            db.disconnect()

        except Exception as e:
            self.log_output(f"Error loading sync status: {e}")

    def load_sync_schedule(self):
        """Load and display sync schedule"""
        try:
            db = SQLiteDatabase(DATABASE_CONFIG['sqlite'])
            db.connect()

            cursor = db.connection.cursor()
            cursor.execute("""
                SELECT table_name, business_hours_interval_minutes, 
                       after_hours_interval_minutes, weekend_interval_minutes, 
                       priority, is_enabled
                FROM sync_schedule
                WHERE table_name != '_GLOBAL_'
                ORDER BY priority, table_name
            """)

            self.schedule_text.delete(1.0, 'end')
            self.schedule_text.insert('end', "Table Sync Schedule:\n")
            self.schedule_text.insert('end', "=" * 80 + "\n\n")

            for row in cursor.fetchall():
                table = row[0]
                bh_int = row[1] if row[1] else "-"
                ah_int = row[2] if row[2] else "-"
                we_int = row[3] if row[3] else "-"
                priority = row[4]
                enabled = "Yes" if row[5] else "No"

                self.schedule_text.insert('end', f"{table:30} ")
                self.schedule_text.insert('end', f"Priority: {priority:2d}  ")
                self.schedule_text.insert('end', f"Business: {bh_int:>4}min  ")
                self.schedule_text.insert('end', f"After-hrs: {ah_int:>4}min  ")
                self.schedule_text.insert('end', f"Weekend: {we_int:>4}min  ")
                self.schedule_text.insert('end', f"Enabled: {enabled}\n")

            db.disconnect()

        except Exception as e:
            self.schedule_text.insert('end', f"\nError loading schedule: {e}")

    def sync_all_tables(self):
        """Sync all tables"""
        if self.is_sync_running():
            messagebox.showwarning("Sync Running", "A sync is already in progress!")
            return

        cmd = [sys.executable, self.main_script]

        if self.full_sync_var.get():
            cmd.append("--full")

        if self.skip_analysis_var.get():
            cmd.append("--skip-auto-analysis")

        self.run_command(cmd, "Syncing all tables...")

    def sync_selected_tables(self):
        """Sync selected tables"""
        if self.is_sync_running():
            messagebox.showwarning("Sync Running", "A sync is already in progress!")
            return

        selected_indices = self.table_listbox.curselection()
        if not selected_indices:
            messagebox.showwarning("No Selection", "Please select at least one table to sync.")
            return

        selected_tables = [self.table_listbox.get(i) for i in selected_indices]

        cmd = [sys.executable, self.main_script, "--tables"] + selected_tables

        if self.full_sync_var.get():
            cmd.append("--full")

        if self.skip_analysis_var.get():
            cmd.append("--skip-auto-analysis")

        self.run_command(cmd, f"Syncing {len(selected_tables)} selected tables...")

    def test_connection(self):
        """Test QuickBooks connection"""
        cmd = [sys.executable, self.main_script, "--test-connection"]
        self.run_command(cmd, "Testing QuickBooks connection...")

    def analyze_open_orders(self):
        """Run open order price analysis"""
        cmd = [sys.executable, self.main_script, "--analyze-open-orders"]

        max_orders = self.max_orders_var.get().strip()
        if max_orders:
            try:
                int(max_orders)
                cmd.extend(["--max-orders", max_orders])
            except ValueError:
                messagebox.showerror("Invalid Input", "Max orders must be a number")
                return

        self.run_command(cmd, "Analyzing open order prices...")

    def extract_historical_prices(self):
        """Extract historical prices"""
        cmd = [sys.executable, self.main_script, "--extract-historical-prices"]

        months = self.months_var.get()
        cmd.extend(["--months", months])

        if self.rebuild_var.get():
            cmd.append("--rebuild-prices")

        # Check for offline QB file
        offline_file = self.offline_qb_var.get().strip()
        if offline_file:
            if not Path(offline_file).exists():
                messagebox.showerror("File Not Found", f"QuickBooks file not found:\n{offline_file}")
                return
            cmd.extend(["--qb-file", offline_file])
            self.log_output(f"Using offline QB file: {offline_file}")

        self.run_command(cmd, f"Extracting {months} months of historical prices...")

    def run_both_analyses(self):
        """Run both price analyses"""
        cmd = [sys.executable, self.main_script, "--analyze-open-orders", "--extract-historical-prices"]

        months = self.months_var.get()
        cmd.extend(["--months", months])

        max_orders = self.max_orders_var.get().strip()
        if max_orders:
            try:
                int(max_orders)
                cmd.extend(["--max-orders", max_orders])
            except ValueError:
                pass

        # Check for offline QB file
        offline_file = self.offline_qb_var.get().strip()
        if offline_file:
            if not Path(offline_file).exists():
                messagebox.showerror("File Not Found", f"QuickBooks file not found:\n{offline_file}")
                return
            cmd.extend(["--qb-file", offline_file])
            self.log_output(f"Using offline QB file: {offline_file}")

        self.run_command(cmd, "Running both price analyses...")

    def verify_database(self):
        """Verify database"""
        cmd = [sys.executable, self.main_script, "--verify-db"]
        self.run_command(cmd, "Verifying database...")

        # Also update the database stats display
        try:
            db = SQLiteDatabase(DATABASE_CONFIG['sqlite'])
            db.connect()

            stats = db.verify_database()

            self.db_stats_text.delete(1.0, 'end')
            self.db_stats_text.insert('end', f"Database exists: {stats['exists']}\n")
            self.db_stats_text.insert('end', f"Size: {stats['size_mb']:.2f} MB\n")
            self.db_stats_text.insert('end', f"Tables: {len(stats['tables'])}\n")
            self.db_stats_text.insert('end', f"Custom fields tracked: {stats['custom_fields_count']}\n\n")

            # Show top 10 tables by size
            self.db_stats_text.insert('end', "Top tables by record count:\n")
            sorted_tables = sorted(stats['tables'], key=lambda x: x['record_count'], reverse=True)
            for table in sorted_tables[:10]:
                self.db_stats_text.insert('end', f"  {table['name']:30} {table['record_count']:>10,} records\n")

            db.disconnect()

        except Exception as e:
            self.db_stats_text.insert('end', f"\nError: {e}")

    def list_all_tables(self):
        """List all tables in console"""
        cmd = [sys.executable, self.main_script, "--list-tables"]
        self.run_command(cmd, "Listing all available tables...")

    def show_linkedtxn_stats(self):
        """Show LinkedTxn statistics"""
        try:
            db = SQLiteDatabase(DATABASE_CONFIG['sqlite'])
            db.connect()

            self.log_output("\n=== LinkedTxn Statistics ===")

            cursor = db.connection.cursor()

            # Check if linked_transactions table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='linked_transactions'
            """)

            if not cursor.fetchone():
                self.log_output("linked_transactions table does not exist yet.")
                self.log_output("Run a sync with the new system to populate it.")
            else:
                # Get statistics
                cursor.execute("SELECT COUNT(*) FROM linked_transactions")
                total = cursor.fetchone()[0]
                self.log_output(f"\nTotal linked transactions: {total:,}")

                # By parent type
                cursor.execute("""
                    SELECT ParentTxnType, COUNT(*) as cnt
                    FROM linked_transactions
                    GROUP BY ParentTxnType
                    ORDER BY cnt DESC
                """)

                self.log_output("\nBy parent transaction type:")
                for row in cursor.fetchall():
                    self.log_output(f"  {row[0]:20} {row[1]:>8,}")

                # By linked type
                cursor.execute("""
                    SELECT LinkedTxnType, COUNT(*) as cnt
                    FROM linked_transactions
                    GROUP BY LinkedTxnType
                    ORDER BY cnt DESC
                """)

                self.log_output("\nBy linked transaction type:")
                for row in cursor.fetchall():
                    self.log_output(f"  {row[0]:20} {row[1]:>8,}")

            db.disconnect()

        except Exception as e:
            self.log_output(f"Error getting LinkedTxn stats: {e}")

    def browse_qb_file(self):
        """Browse for QuickBooks file"""
        filename = filedialog.askopenfilename(
            title="Select QuickBooks Company File",
            filetypes=[
                ("QuickBooks Files", "*.QBW"),
                ("QuickBooks Backup", "*.QBB"),
                ("All Files", "*.*")
            ],
            initialdir=r"C:\Users\Public\Documents\Intuit\QuickBooks\Company Files"
        )

        if filename:
            self.offline_qb_var.set(filename)
            self.log_output(f"Selected offline QB file: {filename}")

    def open_db_location(self):
        """Open database location in file explorer"""
        db_path = Path(DATABASE_CONFIG['sqlite']['path'])
        if db_path.exists():
            if sys.platform == 'win32':
                os.startfile(db_path.parent)
            elif sys.platform == 'darwin':
                subprocess.Popen(['open', db_path.parent])
            else:
                subprocess.Popen(['xdg-open', db_path.parent])
        else:
            messagebox.showwarning("Not Found", "Database file doesn't exist yet.")

    def export_sync_log(self):
        """Export sync log to CSV"""
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"sync_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

        if not filename:
            return

        try:
            db = SQLiteDatabase(DATABASE_CONFIG['sqlite'])
            db.connect()

            cursor = db.connection.cursor()
            cursor.execute("""
                SELECT table_name, last_sync_time, record_count, 
                       sync_duration_seconds, last_status, last_error_message,
                       consecutive_failures
                FROM sync_log
                ORDER BY table_name
            """)

            import csv
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Table', 'Last Sync', 'Records', 'Duration (s)',
                                 'Status', 'Error Message', 'Consecutive Failures'])
                writer.writerows(cursor.fetchall())

            db.disconnect()

            self.log_output(f"Sync log exported to: {filename}")
            messagebox.showinfo("Export Complete", f"Sync log exported to:\n{filename}")

        except Exception as e:
            messagebox.showerror("Export Error", f"Error exporting sync log: {e}")

    def select_high_priority(self):
        """Select high priority tables"""
        self.table_listbox.select_clear(0, 'end')

        high_priority = [
            'invoices', 'sales_orders', 'bills', 'purchase_orders',
            'deposits', 'receive_payments', 'customers', 'vendors',
            'items_inventory'
        ]

        for i, table in enumerate(TABLE_CONFIGS):
            if table['name'] in high_priority:
                self.table_listbox.select_set(i)

    def toggle_timer(self):
        """Toggle auto-sync timer"""
        if self.timer_active:
            self.stop_timer()
        else:
            self.start_timer()

    def start_timer(self):
        """Start auto-sync timer"""
        try:
            interval_minutes = int(self.interval_var.get())
            if interval_minutes < 5:
                messagebox.showerror("Invalid Interval", "Minimum interval is 5 minutes")
                return

            self.timer_active = True
            self.timer_button.config(text="Stop Timer")
            self.timer_status.config(text="Timer: Active", foreground="green")

            # Set next sync time
            self.next_sync_time = datetime.now() + timedelta(minutes=interval_minutes)
            self.log_output(f"Next sync scheduled for: {self.next_sync_time.strftime('%H:%M:%S')}")

            # Start timer thread
            self.timer_thread = threading.Thread(target=self.timer_loop,
                                                 args=(interval_minutes,),
                                                 daemon=True)
            self.timer_thread.start()

            self.log_output(f"Auto-sync timer started with {interval_minutes} minute interval")

        except ValueError:
            messagebox.showerror("Invalid Interval", "Please enter a valid number of minutes")

    def stop_timer(self):
        """Stop auto-sync timer"""
        self.timer_active = False
        self.timer_button.config(text="Start Timer")
        self.timer_status.config(text="Timer: Inactive", foreground="red")
        self.next_sync_label.config(text="")
        self.timer_progress['value'] = 0

        self.log_output("Auto-sync timer stopped")

    def timer_loop(self, interval_minutes):
        """Timer loop for auto-sync"""
        while self.timer_active:
            # Calculate seconds until next sync
            total_seconds = interval_minutes * 60
            self.countdown_seconds = total_seconds

            # Wait for interval
            for remaining in range(total_seconds, 0, -1):
                if not self.timer_active:
                    break

                # Update countdown every second
                self.countdown_seconds = remaining
                mins, secs = divmod(remaining, 60)
                time_str = f"Next sync in: {mins:02d}:{secs:02d}"

                # Calculate progress
                elapsed = total_seconds - remaining
                progress = (elapsed / total_seconds) * 100

                # Send update through queue
                self.output_queue.put(('timer_update', (time_str, progress)))

                time.sleep(1)

            if self.timer_active:
                # Run sync
                self.output_queue.put(('timer_sync', None))

    def auto_sync(self):
        """Run automatic sync"""
        self.log_output("\n=== AUTO-SYNC TRIGGERED ===")
        self.log_output(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.last_sync_time = datetime.now()

        # Update last sync display
        self.last_sync_label.config(text=f"Last sync: {self.last_sync_time.strftime('%H:%M:%S')}")

        # Check if sync already running
        if self.is_sync_running():
            self.log_output("Sync already in progress, skipping auto-sync")
            return

        # Run incremental sync of all tables (skip auto-analysis for timer syncs)
        cmd = [sys.executable, self.main_script, "--skip-auto-analysis"]
        self.run_command(cmd, "Running scheduled incremental sync...")

    def monitor_output(self):
        """Monitor output queue and update console"""
        try:
            while True:
                msg_type, data = self.output_queue.get_nowait()

                if msg_type == 'progress':
                    # Update last line for progress
                    self.update_progress_line(data)
                elif msg_type == 'timer_update':
                    # Update timer display
                    time_str, progress = data
                    self.next_sync_label.config(text=time_str)
                    self.timer_progress['value'] = progress
                elif msg_type == 'timer_sync':
                    # Trigger auto sync
                    self.root.after(0, self.auto_sync)
                else:
                    # Normal output
                    self.log_output(data)
        except queue.Empty:
            pass

        # Schedule next check
        self.root.after(100, self.monitor_output)

    def update_progress_line(self, text):
        """Update the last line in console for progress display"""
        # Get current position
        current_pos = self.console_text.index("end-1c linestart")

        # Delete the last line if it's a progress line
        last_line = self.console_text.get(current_pos, "end-1c")
        if "Processing records" in last_line or "Processed" in last_line:
            self.console_text.delete(current_pos, "end-1c")

        # Insert the new progress line
        self.console_text.insert("end", text)

        if self.autoscroll_var.get():
            self.console_text.see('end')

    def log_output(self, text):
        """Log text to console"""
        # Update last output time for stall detection
        if hasattr(self, 'sync_process') and self.sync_process:
            self.last_output_time = time.time()

        self.console_text.insert('end', text + '\n')

        if self.autoscroll_var.get():
            self.console_text.see('end')

    def clear_console(self):
        """Clear console output"""
        self.console_text.delete(1.0, 'end')

    def save_console_log(self):
        """Save console output to file"""
        filename = filedialog.asksaveasfilename(
            defaultextension=".log",
            filetypes=[("Log files", "*.log"), ("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=f"sync_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        if filename:
            with open(filename, 'w') as f:
                f.write(self.console_text.get(1.0, 'end'))

            messagebox.showinfo("Save Complete", f"Console output saved to:\n{filename}")

    # Goal Tracker Methods
    def save_goal_tracker_config(self):
        """Save Goal Tracker configuration"""
        try:
            from report_manager import ReportManager

            # Get days of week
            selected_days = [day for day, var in self.day_vars.items() if var.get()]
            days_string = ','.join(selected_days)

            # Get selected reps
            reps_list = []
            if self.filter_reps.get():
                reps_string = self.selected_reps.get()
                if reps_string:
                    reps_list = [r.strip() for r in reps_string.split(',')]

            # Update configuration
            db_path = DATABASE_CONFIG['sqlite']['path']
            manager = ReportManager(db_path)
            try:
                manager.update_config(
                    'goal_tracker',
                    enabled=self.goal_tracker_enabled.get(),
                    preferred_time=self.goal_tracker_time.get(),
                    time_window_minutes=self.goal_tracker_window.get(),
                    business_days_only=self.business_days_only.get(),
                    days_of_week=days_string,
                    email_enabled=self.email_enabled.get(),
                    email_recipients=self.email_recipients.get()
                )
            finally:
                manager.close()

            # Save rep filter separately (could be in a JSON file or database)
            import json
            config = {
                'filter_enabled': self.filter_reps.get(),
                'selected_reps': reps_list
            }
            with open('goal_tracker_config.json', 'w') as f:
                json.dump(config, f)

            self.log_output("Goal Tracker configuration saved")

        except Exception as e:
            self.log_output(f"Error saving configuration: {e}")
            messagebox.showerror("Configuration Error", f"Failed to save configuration: {e}")

    def load_goal_tracker_config(self):
        """Load Goal Tracker configuration"""
        try:
            from report_manager import ReportManager

            db_path = DATABASE_CONFIG['sqlite']['path']
            manager = ReportManager(db_path)
            try:
                config = manager.get_config('goal_tracker')

                if config:
                    self.goal_tracker_enabled.set(config['enabled'])
                    self.goal_tracker_time.set(config['preferred_time'])
                    self.goal_tracker_window.set(config['time_window_minutes'])
                    self.business_days_only.set(config['business_days_only'])
                    self.email_enabled.set(config['email_enabled'])
                    self.email_recipients.set(config['email_recipients'] or '')

                    # Set days
                    selected_days = config['days_of_week'].split(',')
                    for day, var in self.day_vars.items():
                        var.set(day in selected_days)
            finally:
                manager.close()

            # Load rep filter
            import json
            try:
                with open('goal_tracker_config.json', 'r') as f:
                    rep_config = json.load(f)
                    self.filter_reps.set(rep_config.get('filter_enabled', True))
                    if rep_config.get('selected_reps'):
                        self.selected_reps.set(','.join(rep_config['selected_reps']))
            except:
                pass  # Use defaults if file doesn't exist

        except Exception as e:
            self.log_output(f"Error loading configuration: {e}")

    def load_goal_tracker_history(self):
        """Load Goal Tracker generation history"""
        try:
            from report_manager import ReportManager

            # Clear existing items
            for item in self.history_tree.get_children():
                self.history_tree.delete(item)

            db_path = DATABASE_CONFIG['sqlite']['path']
            manager = ReportManager(db_path)
            try:
                history = manager.get_report_history('goal_tracker', days=30)

                for row in history:
                    date_str = row['generation_date']
                    time_str = datetime.fromisoformat(row['generation_time']).strftime('%H:%M:%S')
                    trigger = row['trigger_source'] or 'unknown'
                    status = row['status']
                    size = f"{row['file_size_kb']} KB" if row['file_size_kb'] else '-'
                    duration = f"{row['generation_duration_seconds']:.1f}s" if row[
                        'generation_duration_seconds'] else '-'

                    # Color based on status
                    tags = []
                    if status == 'success':
                        tags = ['success']
                    elif status == 'failed':
                        tags = ['error']

                    self.history_tree.insert('', 'end', values=(
                        date_str, time_str, trigger, status, size, duration
                    ), tags=tags)

                # Configure tags
                self.history_tree.tag_configure('success', foreground='green')
                self.history_tree.tag_configure('error', foreground='red')
            finally:
                manager.close()

        except Exception as e:
            self.log_output(f"Error loading history: {e}")

    def generate_goal_tracker_manual(self):
        """Manually generate Goal Tracker report"""
        if messagebox.askyesno("Generate Report", "Generate Goal Tracker report now?"):
            self.log_output("\n=== Generating Goal Tracker Report ===")

            try:
                from report_manager import ReportManager

                # Get selected reps if filtering
                selected_reps = None
                if self.filter_reps.get():
                    reps_string = self.selected_reps.get()
                    if reps_string:
                        selected_reps = [r.strip() for r in reps_string.split(',')]

                db_path = DATABASE_CONFIG['sqlite']['path']
                manager = ReportManager(db_path)
                try:
                    success, result = manager.generate_goal_tracker('manual', selected_reps)

                    if success:
                        self.log_output(f"Report generated successfully: {result}")

                        # Check if email is enabled
                        if self.email_enabled.get() and self.email_recipients.get():
                            if messagebox.askyesno("Email Report", "Would you like to email the report?"):
                                self.send_goal_tracker_email(result)

                        # Refresh history
                        self.load_goal_tracker_history()

                        # Ask to open
                        if messagebox.askyesno("Open Report", "Would you like to open the report?"):
                            os.startfile(result)
                    else:
                        self.log_output(f"Report generation failed: {result}")
                        messagebox.showerror("Generation Failed", f"Failed to generate report:\n{result}")
                finally:
                    manager.close()

            except Exception as e:
                self.log_output(f"Error generating report: {e}")
                messagebox.showerror("Error", f"Error generating report: {e}")

    def send_goal_tracker_email(self, pdf_path):
        """Send Goal Tracker report via email"""
        try:
            from email_sender import EmailSender

            sender = EmailSender()
            recipients = [r.strip() for r in self.email_recipients.get().split(',')]

            # Create email content
            subject = f"Goal Tracker Report - {date.today().strftime('%B %d, %Y')}"
            body = f"""Daily Goal Tracker Report attached.

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Report Type: Goal Tracker III
Period: {date.today().strftime('%B %Y')}

This report includes:
- Month-to-date invoiced sales
- Bagged sales (projected month-end)
- Sales by representative
- Performance metrics vs. monthly target

Best regards,
QuickBooks Sync System
"""

            # Call send_report with correct parameter order
            success, message = sender.send_report(
                pdf_path,  # pdf_path (first parameter)
                recipients,  # recipients (second parameter)
                subject,  # subject (third parameter)
                body  # body (fourth parameter)
            )

            if success:
                self.log_output(f"Report emailed successfully: {message}")
                messagebox.showinfo("Email Sent", "Report emailed successfully!")
            else:
                self.log_output(f"Failed to send email: {message}")
                messagebox.showerror("Email Failed", f"Failed to send email:\n{message}")

        except Exception as e:
            self.log_output(f"Error sending email: {e}")
            messagebox.showerror("Email Error", f"Error sending email: {e}")

    def view_last_report(self):
        """Open the last generated report"""
        try:
            from report_manager import ReportManager

            db_path = DATABASE_CONFIG['sqlite']['path']
            manager = ReportManager(db_path)
            try:
                cursor = manager.conn.cursor()
                cursor.execute("""
                    SELECT last_pdf_path FROM report_tracker 
                    WHERE report_name = 'goal_tracker' 
                    AND generation_status = 'success'
                    ORDER BY last_generated_time DESC LIMIT 1
                """)
                result = cursor.fetchone()

                if result and result['last_pdf_path'] and os.path.exists(result['last_pdf_path']):
                    os.startfile(result['last_pdf_path'])
                else:
                    messagebox.showinfo("No Report", "No report found. Generate a report first.")
            finally:
                manager.close()

        except Exception as e:
            messagebox.showerror("Error", f"Error opening report: {e}")

    def open_reports_folder(self):
        """Open the reports archive folder"""
        reports_folder = Path("Reports")
        if not reports_folder.exists():
            reports_folder.mkdir(parents=True)

        if sys.platform == 'win32':
            os.startfile(reports_folder)
        elif sys.platform == 'darwin':
            subprocess.Popen(['open', reports_folder])
        else:
            subprocess.Popen(['xdg-open', reports_folder])

    def toggle_rep_list(self):
        """Enable/disable rep entry based on checkbox"""
        if self.filter_reps.get():
            self.rep_entry.configure(state='normal')
        else:
            self.rep_entry.configure(state='disabled')

    def check_goal_tracker_schedule(self):
        """Check if Goal Tracker should run (called after successful sync)"""
        if not hasattr(self, 'goal_tracker_enabled') or not self.goal_tracker_enabled.get():
            return

        try:
            from report_manager import ReportManager

            db_path = DATABASE_CONFIG['sqlite']['path']
            manager = ReportManager(db_path)
            try:
                should_run, reason = manager.should_generate_report('goal_tracker', 'post_sync')

                if should_run:
                    self.log_output("\n=== Goal Tracker scheduled to run ===")
                    self.log_output(f"Reason: Post-sync generation")

                    # Get selected reps
                    selected_reps = None
                    if self.filter_reps.get():
                        reps_string = self.selected_reps.get()
                        if reps_string:
                            selected_reps = [r.strip() for r in reps_string.split(',')]

                    success, result = manager.generate_goal_tracker('post_sync', selected_reps)

                    if success:
                        self.log_output(f"Goal Tracker generated: {result}")

                        # Send email if configured
                        if self.email_enabled.get() and self.email_recipients.get():
                            self.send_goal_tracker_email(result)
                    else:
                        self.log_output(f"Goal Tracker failed: {result}")
                else:
                    # Don't log every time, only if it's interesting
                    if "already generated" not in reason.lower():
                        self.log_output(f"Goal Tracker skipped: {reason}")
            finally:
                manager.close()

        except Exception as e:
            self.log_output(f"Error checking Goal Tracker schedule: {e}")

    def configure_email_server(self):
        """Open email server configuration dialog"""
        try:
            from email_sender import EmailSender, EmailConfigDialog

            # Create EmailSender instance
            sender = EmailSender()

            # Pass both root and sender to dialog
            dialog = EmailConfigDialog(self.root, sender)
            self.root.wait_window(dialog.dialog)

        except Exception as e:
            messagebox.showerror("Error", f"Error opening email configuration: {e}")

    def test_email(self):
        """Test email configuration"""
        try:
            from email_sender import EmailSender
            import tempfile

            sender = EmailSender()

            # First test the connection
            self.log_output("Testing email connection...")
            success, message = sender.test_connection()
            if not success:
                self.log_output(f"Email connection test failed: {message}")
                messagebox.showerror("Failed", f"Email connection failed: {message}")
                return

            self.log_output("Email connection test passed ✓")

            # Get test recipient
            test_recipient = self.email_recipients.get().split(',')[0].strip() if self.email_recipients.get() else None

            if not test_recipient:
                test_recipient = messagebox.askstring("Test Email", "Enter email address for test:")
                if not test_recipient:
                    return

            self.log_output(f"Sending test email to: {test_recipient}")

            # Create a test file
            with tempfile.NamedTemporaryFile(mode='w', suffix='_test.txt', delete=False) as f:
                f.write("QuickBooks Sync Manager - Test Email Attachment\n")
                f.write(f"Generated at: {datetime.now()}\n")
                f.write("This is a test attachment to verify email functionality.")
                test_file = f.name

            try:
                subject = "QuickBooks Sync Manager - Test Email"
                body = f"""This is a test email from QuickBooks Sync Manager.

If you received this email, your email configuration is working correctly.

Test sent at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

The attached file is a test attachment to verify the email system is working properly.

Best regards,
QuickBooks Sync System
"""

                # Call send_report with correct parameter order
                success, message = sender.send_report(
                    test_file,  # pdf_path (first parameter)
                    [test_recipient],  # recipients (second parameter)
                    subject,  # subject (third parameter)
                    body  # body (fourth parameter)
                )

                if success:
                    self.log_output(f"✓ Test email sent successfully: {message}")
                    messagebox.showinfo("Success", f"Test email sent successfully to:\n{test_recipient}")
                else:
                    self.log_output(f"Test email failed: {message}")
                    messagebox.showerror("Failed", f"Test email failed:\n{message}")

            finally:
                # Clean up temp file
                try:
                    os.unlink(test_file)
                except:
                    pass

        except Exception as e:
            self.log_output(f"Error in test email: {e}")
            messagebox.showerror("Error", f"Error in test email: {e}")


def main():
    """Main entry point"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='QuickBooks Sync Manager')
    parser.add_argument('--auto-start', type=int, metavar='MINUTES',
                        help='Auto-start timer with specified interval in minutes')
    parser.add_argument('--minimize', action='store_true',
                        help='Minimize window after starting (use with --auto-start)')

    args = parser.parse_args()

    # Create root window
    root = tk.Tk()

    # Set Windows DPI awareness for better scaling
    try:
        from ctypes import windll
        windll.shcore.SetProcessDpiAwareness(1)
    except:
        pass

    # Create app with auto-start parameters
    app = QuickBooksSyncManager(root,
                                auto_start_minutes=args.auto_start,
                                minimize_on_start=args.minimize)

    root.mainloop()


if __name__ == "__main__":
    main()