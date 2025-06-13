#!/usr/bin/env python3
"""
Office 365 Email Test Script
Tests SMTP email sending without requiring Outlook
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import ssl
from datetime import datetime
import os
import getpass


def test_smtp_connection(email, password):
    """Test basic SMTP connection to Office 365"""
    print("\n1. Testing SMTP Connection...")
    print("-" * 50)

    smtp_server = "smtp.office365.com"
    port = 587

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, port) as server:
            print(f"✓ Connected to {smtp_server}:{port}")

            # Start TLS
            server.starttls(context=context)
            print("✓ TLS encryption started")

            # Login
            server.login(email, password)
            print("✓ Authentication successful")

            return True

    except smtplib.SMTPAuthenticationError:
        print("✗ Authentication failed - check email/password")
        print("  Note: You may need to use an app password instead of your regular password")
        return False
    except smtplib.SMTPException as e:
        print(f"✗ SMTP error: {e}")
        return False
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False


def send_test_email(sender_email, password, recipient_email):
    """Send a test email with a small attachment"""
    print("\n2. Sending Test Email...")
    print("-" * 50)

    smtp_server = "smtp.office365.com"
    port = 587

    # Create test file
    test_file = "test_attachment.txt"
    with open(test_file, 'w') as f:
        f.write("This is a test attachment from the Goal Tracker email test.\n")
        f.write(f"Generated at: {datetime.now()}\n")

    try:
        # Create message
        msg = MIMEMultipart()
        msg['Subject'] = f"Test Email - Goal Tracker {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg['From'] = sender_email
        msg['To'] = recipient_email

        # Email body
        body = f"""
This is a test email from the Goal Tracker automated reporting system.

Test Details:
- Date: {datetime.now().strftime('%B %d, %Y')}
- Time: {datetime.now().strftime('%I:%M %p')}
- Sender: {sender_email}
- Recipient: {recipient_email}

If you received this email, the SMTP configuration is working correctly!

This email includes a small text attachment to test file sending capabilities.
        """

        msg.attach(MIMEText(body, 'plain'))

        # Add attachment
        with open(test_file, "rb") as f:
            attach = MIMEApplication(f.read(), _subtype="txt")
            attach.add_header('Content-Disposition', 'attachment', filename=test_file)
            msg.attach(attach)

        # Send email
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, port) as server:
            server.starttls(context=context)
            server.login(sender_email, password)

            print("✓ Connected and authenticated")
            print("→ Sending email...")

            server.send_message(msg)

            print("✓ Email sent successfully!")
            print(f"  Check {recipient_email} for the test message")

        # Cleanup
        os.remove(test_file)
        return True

    except Exception as e:
        print(f"✗ Failed to send email: {e}")
        if os.path.exists(test_file):
            os.remove(test_file)
        return False


def test_large_attachment(sender_email, password, recipient_email):
    """Test sending a larger PDF-sized attachment"""
    print("\n3. Testing PDF-sized Attachment...")
    print("-" * 50)

    # Create a dummy PDF-sized file (about 500KB)
    test_pdf = "test_report.pdf"
    with open(test_pdf, 'wb') as f:
        # Write PDF header
        f.write(b'%PDF-1.4\n')
        # Write some dummy content to make it ~500KB
        dummy_content = b'0' * 500000
        f.write(dummy_content)

    try:
        msg = MIMEMultipart()
        msg['Subject'] = f"Test PDF Attachment - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg['From'] = sender_email
        msg['To'] = recipient_email

        body = "This tests sending a PDF-sized attachment (approx 500KB)."
        msg.attach(MIMEText(body, 'plain'))

        # Add PDF attachment
        with open(test_pdf, "rb") as f:
            attach = MIMEApplication(f.read(), _subtype="pdf")
            attach.add_header('Content-Disposition', 'attachment',
                              filename=f"test_report_{datetime.now().strftime('%Y%m%d')}.pdf")
            msg.attach(attach)

        # Send
        context = ssl.create_default_context()
        with smtplib.SMTP("smtp.office365.com", 587) as server:
            server.starttls(context=context)
            server.login(sender_email, password)
            server.send_message(msg)

        print("✓ Large attachment sent successfully!")

        # Cleanup
        os.remove(test_pdf)
        return True

    except Exception as e:
        print(f"✗ Failed to send large attachment: {e}")
        if os.path.exists(test_pdf):
            os.remove(test_pdf)
        return False


def main():
    """Run email tests"""
    print("=" * 60)
    print("Office 365 SMTP Email Test for Goal Tracker")
    print("=" * 60)

    print("\nThis will test sending emails through Office 365 SMTP.")
    print("You'll need:")
    print("1. Your Office 365 email address")
    print("2. Your password OR an app password (recommended)")
    print("3. A recipient email address for testing")

    print("\nTo create an app password:")
    print("- Go to https://mysignins.microsoft.com/security-info")
    print("- Add method → App password")
    print("- Name it 'Goal Tracker' and use that password here")

    print("\n" + "-" * 60)

    # Get credentials
    sender_email = input("\nEnter your Office 365 email address: ").strip()
    password = getpass.getpass("Enter your password (or app password): ")
    recipient_email = input("Enter recipient email for test (can be same as sender): ").strip()

    if not recipient_email:
        recipient_email = sender_email

    # Run tests
    print("\nStarting tests...")

    # Test 1: Connection
    if test_smtp_connection(sender_email, password):

        # Test 2: Simple email
        if send_test_email(sender_email, password, recipient_email):

            # Test 3: Large attachment
            proceed = input("\nTest large attachment? (y/n): ").lower()
            if proceed == 'y':
                test_large_attachment(sender_email, password, recipient_email)

    print("\n" + "=" * 60)
    print("Testing complete!")

    if input("\nSave configuration for future use? (y/n): ").lower() == 'y':
        config = f"""
# Email Configuration for Goal Tracker
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587
SENDER_EMAIL = "{sender_email}"
# IMPORTANT: Store password securely, not in plain text!
# Consider using environment variables or keyring library
RECIPIENTS = ["{recipient_email}"]
"""
        with open("email_config_template.py", 'w') as f:
            f.write(config)
        print("\nConfiguration template saved to: email_config_template.py")
        print("Remember to store the password securely!")


if __name__ == "__main__":
    main()