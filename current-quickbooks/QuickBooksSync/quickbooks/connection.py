"""
QuickBooks connection management
Handles login, logout, and session management
"""
import win32com.client as win32
import time
import logging
import os
import pywintypes
from typing import Optional, Dict, Any


class QuickBooksConnection:
    """Manages QuickBooks QBFC connection and session"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session_manager = None
        self.is_connected = False
        self.is_logged_in = False
        self.max_qbxml_version = None

    def connect(self, max_wait_seconds: Optional[int] = None) -> bool:
        """
        Connect to QuickBooks

        Args:
            max_wait_seconds: Maximum time to wait for connection

        Returns:
            bool: True if connection successful
        """
        if max_wait_seconds is None:
            max_wait_seconds = self.config.get('max_wait_seconds', 10)

        try:
            logging.info("Starting QuickBooks connection...")

            # Create session manager
            self.session_manager = self._create_session_manager()
            if not self.session_manager:
                return False

            # Open connection
            if not self._open_connection():
                return False

            # Begin session
            if not self._begin_session(max_wait_seconds):
                self._close_connection()
                return False

            # Get QBXML version info
            self._get_version_info()

            self.is_connected = True
            self.is_logged_in = True
            logging.info("Successfully connected to QuickBooks")
            return True

        except Exception as e:
            logging.error(f"Failed to connect to QuickBooks: {str(e)}", exc_info=True)
            self.disconnect()
            return False

    def disconnect(self) -> None:
        """Disconnect from QuickBooks"""
        if not self.session_manager:
            logging.warning("No QuickBooks session to disconnect")
            return

        try:
            if self.is_logged_in:
                logging.info("Ending QuickBooks session...")
                self.session_manager.EndSession()
                self.is_logged_in = False

            if self.is_connected:
                logging.info("Closing QuickBooks connection...")
                self.session_manager.CloseConnection()
                self.is_connected = False

            logging.info("Disconnected from QuickBooks successfully")

        except Exception as e:
            logging.error(f"Error disconnecting from QuickBooks: {str(e)}", exc_info=True)
        finally:
            self.session_manager = None

    def create_request(self) -> Any:
        """Create a new request message set"""
        if not self.is_logged_in:
            raise RuntimeError("Not connected to QuickBooks")

        qbxml_version = self.config.get('qbfc_version', 16)
        return self.session_manager.CreateMsgSetRequest("US", qbxml_version, 0)

    def do_requests(self, request_msg_set) -> Any:
        """Execute QuickBooks requests"""
        if not self.is_logged_in:
            raise RuntimeError("Not connected to QuickBooks")

        return self.session_manager.DoRequests(request_msg_set)

    def _create_session_manager(self) -> Optional[Any]:
        """Create QBFC session manager"""
        try:
            # Try different QBFC versions
            qbfc_versions = ["QBFC16", "QBFC15", "QBFC13"]

            for version in qbfc_versions:
                try:
                    session_manager = win32.Dispatch(f"{version}.QBSessionManager")
                    logging.info(f"Created {version} session manager successfully")
                    return session_manager
                except Exception as e:
                    logging.debug(f"Failed to create {version}: {e}")
                    continue

            logging.error("Failed to create any QBFC session manager")
            return None

        except Exception as e:
            logging.error(f"Error creating session manager: {e}", exc_info=True)
            return None

    def _open_connection(self) -> bool:
        """Open connection to QuickBooks"""
        try:
            app_name = "Fromm Packaging QuickBooks Integration"
            logging.info(f"Opening connection with app name: {app_name}")

            self.session_manager.OpenConnection2("", app_name, 1)
            logging.info("Connection opened successfully")
            return True

        except Exception as e:
            error_msg = str(e)
            if "0x80040408" in error_msg or "busy" in error_msg.lower():
                logging.info("QuickBooks is busy with another request")
            else:
                logging.error(f"Failed to open connection: {error_msg}")
            return False

    def _begin_session(self, max_wait_seconds: int) -> bool:
        """Begin QuickBooks session"""
        company_file = self.config.get('company_file', '')
        mode = self.config.get('connection_mode', 2)  # 2 = Multi-user

        # Verify file exists
        if company_file and not os.path.exists(company_file):
            logging.error(f"QuickBooks file not found: {company_file}")
            return False

        logging.info(f"Beginning session with: {company_file or 'currently open company'}")

        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            try:
                session_start = time.time()
                self.session_manager.BeginSession(company_file, mode)
                session_duration = time.time() - session_start

                logging.info(f"Session started successfully in {session_duration:.2f} seconds")
                return True

            except Exception as e:
                error_msg = str(e)

                if "could not start QuickBooks" in error_msg:
                    logging.warning("QuickBooks not running, waiting...")
                    time.sleep(2)
                elif "locked" in error_msg.lower() or "being used" in error_msg.lower():
                    logging.info("QuickBooks file is locked")
                    return False
                elif "0x80040408" in error_msg:
                    logging.info("QuickBooks is busy")
                    return False
                else:
                    logging.error(f"Error starting session: {error_msg}")
                    return False

        logging.warning(f"Timeout ({max_wait_seconds}s) waiting for QuickBooks")
        return False

    def _close_connection(self) -> None:
        """Close the connection (used during failed login)"""
        try:
            if self.session_manager:
                self.session_manager.CloseConnection()
        except:
            pass

    def _get_version_info(self) -> None:
        """Get QuickBooks XML version information"""
        try:
            if hasattr(self.session_manager, 'GetMaxQBXMLVersion'):
                self.max_qbxml_version = self.session_manager.GetMaxQBXMLVersion()
                logging.info(f"Max QBXML Version: {self.max_qbxml_version}")
            elif hasattr(self.session_manager, 'GetMaxVersionForCountry'):
                self.max_qbxml_version = self.session_manager.GetMaxVersionForCountry("US")
                logging.info(f"Max QBXML Version for US: {self.max_qbxml_version}")
        except Exception as e:
            logging.debug(f"Could not get QBXML version: {e}")

    def is_busy_error(self, error: Exception) -> bool:
        """Check if error indicates QuickBooks is busy"""
        if isinstance(error, pywintypes.com_error):
            error_msg = str(error)
            hresult = getattr(error, 'hresult', 0)

            return ("0x80040408" in error_msg or
                    hresult == -2147220472 or
                    "busy" in error_msg.lower())
        return False

    def is_session_invalid_error(self, error: Exception) -> bool:
        """Check if error indicates invalid session"""
        if isinstance(error, pywintypes.com_error):
            hresult = getattr(error, 'hresult', 0)
            return hresult == -2147220467
        return False