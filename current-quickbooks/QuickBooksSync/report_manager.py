"""
Report Manager - Handles automated report generation and tracking
"""
import sqlite3
import os
import subprocess
import sys
from datetime import datetime, date, time, timedelta
import shutil
import json
from pathlib import Path


class ReportManager:
    def __init__(self, db_path):
        self.db_path = db_path
        # Don't keep a persistent connection - create as needed
        self._create_tables()

    def _get_connection(self):
        """Get a database connection with proper settings"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)  # 30 second timeout
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrency
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _create_tables(self):
        """Create report tracking tables if they don't exist"""
        # Try to read schema file
        schema_content = None
        schema_locations = [
            'report_tracker_schema.sql',
            os.path.join(os.path.dirname(__file__), 'report_tracker_schema.sql')
        ]

        for location in schema_locations:
            if os.path.exists(location):
                with open(location, 'r') as f:
                    schema_content = f.read()
                break

        # If schema file not found, use embedded schema
        if not schema_content:
            schema_content = """
-- Report tracking tables for automated report generation

-- Track when reports are generated
CREATE TABLE IF NOT EXISTS report_tracker (
    report_name TEXT PRIMARY KEY,
    last_generated_date DATE,
    last_generated_time TIMESTAMP,
    last_pdf_path TEXT,
    generation_status TEXT CHECK(generation_status IN ('success', 'failed', 'pending')),
    error_message TEXT,
    email_sent BOOLEAN DEFAULT 0,
    email_sent_time TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Log all report generation attempts
CREATE TABLE IF NOT EXISTS report_generation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_name TEXT NOT NULL,
    generation_date DATE NOT NULL,
    generation_time TIMESTAMP NOT NULL,
    trigger_source TEXT,
    status TEXT CHECK(status IN ('started', 'success', 'failed', 'skipped')),
    pdf_path TEXT,
    file_size_kb INTEGER,
    generation_duration_seconds REAL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Report configuration
CREATE TABLE IF NOT EXISTS report_config (
    report_name TEXT PRIMARY KEY,
    enabled BOOLEAN DEFAULT 1,
    preferred_time TIME DEFAULT '08:00',
    time_window_minutes INTEGER DEFAULT 120,
    business_days_only BOOLEAN DEFAULT 1,
    days_of_week TEXT DEFAULT 'Mon,Tue,Wed,Thu,Fri',
    email_enabled BOOLEAN DEFAULT 0,
    email_recipients TEXT,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified_by TEXT
);

-- Email log
CREATE TABLE IF NOT EXISTS email_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_name TEXT NOT NULL,
    sent_date TIMESTAMP NOT NULL,
    recipients TEXT NOT NULL,
    pdf_path TEXT,
    status TEXT CHECK(status IN ('success', 'failed', 'pending')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default configuration for goal_tracker
INSERT OR IGNORE INTO report_config (
    report_name, 
    enabled, 
    preferred_time, 
    time_window_minutes,
    business_days_only,
    days_of_week
) VALUES (
    'goal_tracker',
    1,
    '08:00',
    120,
    1,
    'Mon,Tue,Wed,Thu,Fri'
);
"""

        # Use a separate connection for table creation
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Execute schema in smaller chunks to avoid locks
            statements = schema_content.split(';')
            for statement in statements:
                if statement.strip():
                    cursor.execute(statement + ';')

            conn.commit()
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                # If database is locked during creation, it's probably OK
                # The tables likely already exist
                print(f"Warning: Database locked during table creation. Tables may already exist.")
            else:
                raise
        finally:
            if conn:
                conn.close()

    def should_generate_report(self, report_name='goal_tracker', trigger_source='scheduled'):
        """Check if report should be generated based on configuration and tracking"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Get configuration
            cursor.execute("""
                SELECT * FROM report_config WHERE report_name = ?
            """, (report_name,))
            config = cursor.fetchone()

            if not config or not config['enabled']:
                return False, "Report is disabled"

            # Check if already generated today
            cursor.execute("""
                SELECT * FROM report_tracker 
                WHERE report_name = ? AND last_generated_date = DATE('now')
            """, (report_name,))

            if cursor.fetchone():
                return False, "Report already generated today"

            # Check if business day
            today = date.today()
            if config['business_days_only'] and today.weekday() >= 5:  # Weekend
                return False, "Not a business day"

            # Check day of week
            allowed_days = config['days_of_week'].split(',')
            current_day = today.strftime('%a')  # Mon, Tue, etc.
            if current_day not in allowed_days:
                return False, f"Not scheduled for {current_day}"

            # Check time window (only for scheduled triggers)
            if trigger_source == 'scheduled':
                now = datetime.now()
                preferred_time = datetime.strptime(config['preferred_time'], '%H:%M').time()
                preferred_datetime = datetime.combine(today, preferred_time)

                # Calculate window
                window_start = preferred_datetime
                window_end = preferred_datetime + timedelta(minutes=config['time_window_minutes'])

                if not (window_start <= now <= window_end):
                    return False, f"Outside time window ({preferred_time} +{config['time_window_minutes']}min)"

            return True, "OK to generate"

        finally:
            if conn:
                conn.close()

    def log_generation_attempt(self, report_name, status, trigger_source,
                             pdf_path=None, error_message=None, duration=None):
        """Log a report generation attempt"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            file_size_kb = None
            if pdf_path and os.path.exists(pdf_path):
                file_size_kb = os.path.getsize(pdf_path) // 1024

            cursor.execute("""
                INSERT INTO report_generation_log 
                (report_name, generation_date, generation_time, trigger_source, 
                 status, pdf_path, file_size_kb, generation_duration_seconds, error_message)
                VALUES (?, DATE('now'), CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?)
            """, (report_name, trigger_source, status, pdf_path, file_size_kb,
                  duration, error_message))

            conn.commit()
            return cursor.lastrowid

        finally:
            if conn:
                conn.close()

    def update_tracker(self, report_name, status, pdf_path=None, error_message=None):
        """Update the main report tracker"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            if status == 'success':
                cursor.execute("""
                    INSERT OR REPLACE INTO report_tracker 
                    (report_name, last_generated_date, last_generated_time, 
                     last_pdf_path, generation_status, error_message, retry_count)
                    VALUES (?, DATE('now'), CURRENT_TIMESTAMP, ?, ?, NULL, 0)
                """, (report_name, pdf_path, status))
            else:
                # Update retry count on failure
                cursor.execute("""
                    INSERT INTO report_tracker 
                    (report_name, last_generated_date, generation_status, 
                     error_message, retry_count)
                    VALUES (?, DATE('now'), ?, ?, 1)
                    ON CONFLICT(report_name) DO UPDATE SET
                        generation_status = excluded.generation_status,
                        error_message = excluded.error_message,
                        retry_count = retry_count + 1,
                        updated_at = CURRENT_TIMESTAMP
                """, (report_name, status, error_message))

            conn.commit()

        finally:
            if conn:
                conn.close()

    def generate_goal_tracker(self, trigger_source='manual', selected_reps=None):
        """Generate the Goal Tracker report"""
        report_name = 'goal_tracker'
        start_time = datetime.now()

        # Log attempt start
        log_id = self.log_generation_attempt(report_name, 'started', trigger_source)

        try:
            # Find the goal_tracker3.py script
            script_locations = [
                'goal_tracker3.py',  # Same directory
                'Reports/goal_tracker3.py',  # Reports subdirectory
                'C:/QuickBooksSync/Reports/goal_tracker3.py',  # Absolute path
                os.path.join(os.path.dirname(__file__), 'goal_tracker3.py'),  # Relative to this file
            ]

            script_path = None
            for location in script_locations:
                if os.path.exists(location):
                    script_path = location
                    break

            if not script_path:
                raise FileNotFoundError("Could not find goal_tracker3.py")

            # Build command
            cmd = [sys.executable, script_path]

            # Add rep filter if specified
            if selected_reps:
                # Modify goal_tracker3.py to accept command line args
                # For now, we'll use environment variable
                os.environ['GOAL_TRACKER_REPS'] = json.dumps(selected_reps)

            # Run report generation
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                # Find generated PDF
                today_str = date.today().strftime('%Y%m%d')
                pdf_patterns = [
                    f"Goal_Tracker_{today_str}.pdf",
                    f"goal_tracker_{today_str}.pdf",
                    "goal_tracker_report.pdf"
                ]

                pdf_path = None
                for pattern in pdf_patterns:
                    if os.path.exists(pattern):
                        pdf_path = os.path.abspath(pattern)
                        break

                if pdf_path:
                    # Success
                    duration = (datetime.now() - start_time).total_seconds()
                    self.log_generation_attempt(report_name, 'success', trigger_source,
                                              pdf_path, duration=duration)
                    self.update_tracker(report_name, 'success', pdf_path)

                    # Archive the report
                    self._archive_report(pdf_path)

                    return True, pdf_path
                else:
                    error = "PDF file not found after generation"
                    self.log_generation_attempt(report_name, 'failed', trigger_source,
                                              error_message=error)
                    self.update_tracker(report_name, 'failed', error_message=error)
                    return False, error
            else:
                # Generation failed
                error = f"Report generation failed: {result.stderr}"
                self.log_generation_attempt(report_name, 'failed', trigger_source,
                                          error_message=error)
                self.update_tracker(report_name, 'failed', error_message=error)
                return False, error

        except Exception as e:
            error = f"Exception during generation: {str(e)}"
            self.log_generation_attempt(report_name, 'failed', trigger_source,
                                      error_message=error)
            self.update_tracker(report_name, 'failed', error_message=error)
            return False, error

    def _archive_report(self, pdf_path):
        """Archive report to organized folder structure"""
        try:
            # Create archive structure: Reports/YYYY/MM/
            archive_base = Path("Reports")
            year_folder = archive_base / str(date.today().year)
            month_folder = year_folder / f"{date.today().month:02d}"
            month_folder.mkdir(parents=True, exist_ok=True)

            # Copy to archive
            archive_path = month_folder / os.path.basename(pdf_path)
            shutil.copy2(pdf_path, archive_path)

        except Exception as e:
            print(f"Warning: Failed to archive report: {e}")

    def handle_file_lock(self, original_path):
        """Handle case where PDF is locked (open in another program)"""
        base_name = os.path.splitext(original_path)[0]
        extension = os.path.splitext(original_path)[1]

        # Try alternative names
        for i in range(1, 10):
            alt_path = f"{base_name}_{i}{extension}"
            try:
                # Test if we can write to this path
                with open(alt_path, 'wb') as f:
                    f.write(b'test')
                os.remove(alt_path)
                return alt_path
            except:
                continue

        # If all attempts fail, use timestamp
        timestamp = datetime.now().strftime('%H%M%S')
        return f"{base_name}_{timestamp}{extension}"

    def get_report_history(self, report_name='goal_tracker', days=30):
        """Get report generation history"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM report_generation_log
                WHERE report_name = ? 
                AND generation_date >= DATE('now', '-' || ? || ' days')
                ORDER BY generation_time DESC
            """, (report_name, days))

            return cursor.fetchall()

        finally:
            if conn:
                conn.close()

    def get_config(self, report_name='goal_tracker'):
        """Get report configuration"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM report_config WHERE report_name = ?
            """, (report_name,))
            return cursor.fetchone()

        finally:
            if conn:
                conn.close()

    def update_config(self, report_name, **kwargs):
        """Update report configuration"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Build update query dynamically
            updates = []
            values = []
            for key, value in kwargs.items():
                if key in ['enabled', 'preferred_time', 'time_window_minutes',
                          'business_days_only', 'days_of_week', 'email_enabled',
                          'email_recipients']:
                    updates.append(f"{key} = ?")
                    values.append(value)

            if updates:
                values.extend([datetime.now(), os.getlogin() if os.name == 'nt' else os.environ.get('USER', 'unknown'), report_name])
                query = f"""
                    UPDATE report_config 
                    SET {', '.join(updates)}, last_modified = ?, last_modified_by = ?
                    WHERE report_name = ?
                """
                cursor.execute(query, values)
                conn.commit()
                return True
            return False

        finally:
            if conn:
                conn.close()

    def close(self):
        """Close database connection"""
        # No persistent connection to close in this version
        pass

    @property
    def conn(self):
        """Property for backward compatibility - returns a new connection"""
        return self._get_connection()