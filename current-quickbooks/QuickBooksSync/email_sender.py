"""
Email Sender Module for QuickBooks Sync Reports
Handles sending PDF reports via email with configuration options

SAVE THIS FILE AS: email_sender.py
IN THE SAME DIRECTORY AS: sync_manager.py
"""
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate
import os
import json
from pathlib import Path
from datetime import datetime


class EmailSender:
    def __init__(self, config_file='email_config.json'):
        self.config_file = config_file
        self.config = self.load_config()

    def load_config(self):
        """Load email configuration from file"""
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                return json.load(f)
        else:
            # Default configuration template
            return {
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'use_tls': True,
                'sender_email': '',
                'sender_password': '',
                'sender_name': 'QuickBooks Sync System',
                'reply_to': '',
                'enabled': False
            }

    def save_config(self, **kwargs):
        """Save email configuration"""
        self.config.update(kwargs)
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=4)
        return True

    def test_connection(self):
        """Test SMTP connection"""
        if not self.config.get('enabled'):
            return False, "Email is not enabled"

        if not self.config.get('sender_email') or not self.config.get('sender_password'):
            return False, "Email credentials not configured"

        try:
            # Create SMTP connection
            if self.config.get('use_tls'):
                context = ssl.create_default_context()
                server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
                server.starttls(context=context)
            else:
                server = smtplib.SMTP_SSL(self.config['smtp_server'], self.config['smtp_port'])

            # Login
            server.login(self.config['sender_email'], self.config['sender_password'])
            server.quit()

            return True, "Connection successful"

        except smtplib.SMTPAuthenticationError:
            return False, "Authentication failed. Check email and password."
        except smtplib.SMTPException as e:
            return False, f"SMTP error: {str(e)}"
        except Exception as e:
            return False, f"Connection error: {str(e)}"

    def send_report(self, pdf_path, recipients, subject=None, body=None, report_name="Goal Tracker"):
        """Send a PDF report via email"""
        if not self.config.get('enabled'):
            return False, "Email is not enabled"

        if not os.path.exists(pdf_path):
            return False, f"PDF file not found: {pdf_path}"

        if not recipients:
            return False, "No recipients specified"

        # Parse recipients
        if isinstance(recipients, str):
            recipient_list = [r.strip() for r in recipients.split(',') if r.strip()]
        else:
            recipient_list = recipients

        if not recipient_list:
            return False, "No valid recipients"

        try:
            # Create message
            msg = MIMEMultipart()

            # Headers
            msg['From'] = f"{self.config.get('sender_name', 'QuickBooks Sync')} <{self.config['sender_email']}>"
            msg['To'] = ', '.join(recipient_list)
            msg['Date'] = formatdate(localtime=True)

            # Subject
            if not subject:
                subject = f"{report_name} - {datetime.now().strftime('%B %d, %Y')}"
            msg['Subject'] = subject

            # Reply-to if configured
            if self.config.get('reply_to'):
                msg['Reply-To'] = self.config['reply_to']

            # Body
            if not body:
                body = f"""
Good morning,

Please find attached the {report_name} for {datetime.now().strftime('%B %d, %Y')}.

This report includes:
- Month-to-date sales performance
- Bagged sales (orders likely to ship by month-end)
- Sales by representative
- Progress toward monthly targets

This is an automated report from the QuickBooks Sync System.

Best regards,
QuickBooks Sync System
"""

            msg.attach(MIMEText(body, 'plain'))

            # Attach PDF
            with open(pdf_path, 'rb') as f:
                attach = MIMEApplication(f.read(), _subtype="pdf")
                attach.add_header('Content-Disposition', 'attachment',
                                  filename=os.path.basename(pdf_path))
                msg.attach(attach)

            # Send email
            if self.config.get('use_tls'):
                context = ssl.create_default_context()
                server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
                server.starttls(context=context)
            else:
                server = smtplib.SMTP_SSL(self.config['smtp_server'], self.config['smtp_port'])

            server.login(self.config['sender_email'], self.config['sender_password'])
            server.send_message(msg)
            server.quit()

            return True, f"Email sent successfully to {len(recipient_list)} recipients"

        except Exception as e:
            return False, f"Failed to send email: {str(e)}"

    def get_config_status(self):
        """Get current configuration status"""
        status = {
            'configured': bool(self.config.get('sender_email')),
            'enabled': self.config.get('enabled', False),
            'smtp_server': self.config.get('smtp_server', 'Not set'),
            'sender_email': self.config.get('sender_email', 'Not set')
        }
        return status


class EmailConfigDialog:
    """Tkinter dialog for email configuration"""

    def __init__(self, parent, email_sender):
        import tkinter as tk
        from tkinter import ttk, messagebox

        self.email_sender = email_sender
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Email Configuration")
        self.dialog.geometry("500x600")
        self.dialog.transient(parent)
        self.dialog.grab_set()

        # Create widgets
        self.create_widgets()

        # Load current config
        self.load_current_config()

        # Center on parent
        self.dialog.update_idletasks()
        x = (parent.winfo_x() + parent.winfo_width() // 2 -
             self.dialog.winfo_width() // 2)
        y = (parent.winfo_y() + parent.winfo_height() // 2 -
             self.dialog.winfo_height() // 2)
        self.dialog.geometry(f"+{x}+{y}")

    def create_widgets(self):
        import tkinter as tk
        from tkinter import ttk, messagebox

        # Main frame
        main_frame = ttk.Frame(self.dialog, padding="20")
        main_frame.pack(fill='both', expand=True)

        # Title
        title = ttk.Label(main_frame, text="Email Configuration",
                          font=('Arial', 14, 'bold'))
        title.grid(row=0, column=0, columnspan=2, pady=(0, 20))

        # Enable checkbox
        self.enabled_var = tk.BooleanVar()
        ttk.Checkbutton(main_frame, text="Enable email notifications",
                        variable=self.enabled_var,
                        command=self.toggle_fields).grid(row=1, column=0,
                                                         columnspan=2, sticky='w', pady=10)

        # SMTP Settings
        ttk.Label(main_frame, text="SMTP Settings",
                  font=('Arial', 11, 'bold')).grid(row=2, column=0,
                                                   columnspan=2, sticky='w', pady=(10, 5))

        # Server
        ttk.Label(main_frame, text="SMTP Server:").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        self.server_var = tk.StringVar()
        self.server_entry = ttk.Entry(main_frame, textvariable=self.server_var, width=40)
        self.server_entry.grid(row=3, column=1, sticky='w', pady=5)

        # Port
        ttk.Label(main_frame, text="Port:").grid(row=4, column=0, sticky='e', padx=5, pady=5)
        self.port_var = tk.IntVar()
        self.port_entry = ttk.Entry(main_frame, textvariable=self.port_var, width=10)
        self.port_entry.grid(row=4, column=1, sticky='w', pady=5)

        # TLS
        self.tls_var = tk.BooleanVar()
        ttk.Checkbutton(main_frame, text="Use TLS (recommended)",
                        variable=self.tls_var).grid(row=5, column=1, sticky='w', pady=5)

        # Credentials
        ttk.Label(main_frame, text="Credentials",
                  font=('Arial', 11, 'bold')).grid(row=6, column=0,
                                                   columnspan=2, sticky='w', pady=(20, 5))

        # Email
        ttk.Label(main_frame, text="Email Address:").grid(row=7, column=0, sticky='e', padx=5, pady=5)
        self.email_var = tk.StringVar()
        self.email_entry = ttk.Entry(main_frame, textvariable=self.email_var, width=40)
        self.email_entry.grid(row=7, column=1, sticky='w', pady=5)

        # Password
        ttk.Label(main_frame, text="Password:").grid(row=8, column=0, sticky='e', padx=5, pady=5)
        self.password_var = tk.StringVar()
        self.password_entry = ttk.Entry(main_frame, textvariable=self.password_var, width=40, show='*')
        self.password_entry.grid(row=8, column=1, sticky='w', pady=5)

        # App password note
        note_frame = ttk.Frame(main_frame)
        note_frame.grid(row=9, column=0, columnspan=2, pady=10)

        ttk.Label(note_frame, text="Note for Gmail users:",
                  font=('Arial', 9, 'bold')).pack(anchor='w')
        ttk.Label(note_frame, text="• Use an App Password instead of your regular password",
                  font=('Arial', 9)).pack(anchor='w')
        ttk.Label(note_frame, text="• Enable 2-factor authentication first",
                  font=('Arial', 9)).pack(anchor='w')
        ttk.Label(note_frame, text="• Create App Password at: myaccount.google.com/apppasswords",
                  font=('Arial', 9)).pack(anchor='w')

        # Additional Settings
        ttk.Label(main_frame, text="Additional Settings",
                  font=('Arial', 11, 'bold')).grid(row=10, column=0,
                                                   columnspan=2, sticky='w', pady=(20, 5))

        # Sender name
        ttk.Label(main_frame, text="Sender Name:").grid(row=11, column=0, sticky='e', padx=5, pady=5)
        self.sender_name_var = tk.StringVar()
        self.sender_name_entry = ttk.Entry(main_frame, textvariable=self.sender_name_var, width=40)
        self.sender_name_entry.grid(row=11, column=1, sticky='w', pady=5)

        # Reply-to
        ttk.Label(main_frame, text="Reply-To (optional):").grid(row=12, column=0, sticky='e', padx=5, pady=5)
        self.reply_to_var = tk.StringVar()
        self.reply_to_entry = ttk.Entry(main_frame, textvariable=self.reply_to_var, width=40)
        self.reply_to_entry.grid(row=12, column=1, sticky='w', pady=5)

        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=13, column=0, columnspan=2, pady=30)

        ttk.Button(button_frame, text="Test Connection",
                   command=self.test_connection).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Save",
                   command=self.save_config).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Cancel",
                   command=self.dialog.destroy).pack(side='left', padx=5)

        # Store entry widgets for enabling/disabling
        self.entry_widgets = [
            self.server_entry, self.port_entry, self.email_entry,
            self.password_entry, self.sender_name_entry, self.reply_to_entry
        ]

    def load_current_config(self):
        """Load current configuration into fields"""
        config = self.email_sender.config

        self.enabled_var.set(config.get('enabled', False))
        self.server_var.set(config.get('smtp_server', 'smtp.gmail.com'))
        self.port_var.set(config.get('smtp_port', 587))
        self.tls_var.set(config.get('use_tls', True))
        self.email_var.set(config.get('sender_email', ''))
        self.password_var.set(config.get('sender_password', ''))
        self.sender_name_var.set(config.get('sender_name', 'QuickBooks Sync System'))
        self.reply_to_var.set(config.get('reply_to', ''))

        self.toggle_fields()

    def toggle_fields(self):
        """Enable/disable fields based on checkbox"""
        state = 'normal' if self.enabled_var.get() else 'disabled'
        for widget in self.entry_widgets:
            widget.configure(state=state)

    def test_connection(self):
        """Test email connection"""
        from tkinter import messagebox

        # Save current settings temporarily
        self.save_config(show_message=False)

        # Test connection
        success, message = self.email_sender.test_connection()

        if success:
            messagebox.showinfo("Success", message)
        else:
            messagebox.showerror("Connection Failed", message)

    def save_config(self, show_message=True):
        """Save configuration"""
        from tkinter import messagebox

        try:
            self.email_sender.save_config(
                enabled=self.enabled_var.get(),
                smtp_server=self.server_var.get(),
                smtp_port=self.port_var.get(),
                use_tls=self.tls_var.get(),
                sender_email=self.email_var.get(),
                sender_password=self.password_var.get(),
                sender_name=self.sender_name_var.get(),
                reply_to=self.reply_to_var.get()
            )

            if show_message:
                messagebox.showinfo("Saved", "Email configuration saved successfully")
                self.dialog.destroy()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to save configuration: {str(e)}")


# Preset configurations for common email providers
EMAIL_PRESETS = {
    'gmail': {
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'use_tls': True,
        'note': 'Requires App Password with 2FA enabled'
    },
    'outlook': {
        'smtp_server': 'smtp-mail.outlook.com',
        'smtp_port': 587,
        'use_tls': True,
        'note': 'Use your Outlook.com email and password'
    },
    'office365': {
        'smtp_server': 'smtp.office365.com',
        'smtp_port': 587,
        'use_tls': True,
        'note': 'Use your Office 365 email and password'
    },
    'yahoo': {
        'smtp_server': 'smtp.mail.yahoo.com',
        'smtp_port': 587,
        'use_tls': True,
        'note': 'Requires App Password'
    }
}