#!/usr/bin/env python3
"""
Goal Tracker III - Daily Sales Report Generator
Generates PDF reports for bagged and invoiced sales by rep
"""

import sqlite3
import json
import sys
from datetime import datetime, date, timedelta
from decimal import Decimal
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.graphics import renderPDF
import os


class GoalTrackerIII:
    def __init__(self, db_path, selected_reps=None):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.today = date.today()
        self.month_start = date(self.today.year, self.today.month, 1)
        self.selected_reps = selected_reps  # List of rep names to include, None = all reps

    def is_business_day(self, check_date):
        """Check if date is a business day (not weekend)"""
        if check_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        # Add specific holidays here if needed
        # For now, just checking weekends
        return True

    def get_monthly_target(self):
        """Get the monthly sales target"""
        year_month = self.today.strftime('%Y-%m')
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT target_amount FROM monthly_targets 
            WHERE year_month = ?
        """, (year_month,))
        result = cursor.fetchone()
        return float(result['target_amount']) if result else 0.0

    def get_bagged_sales(self):
        """Get bagged sales (orders likely to ship by month-end)"""
        cursor = self.conn.cursor()

        # Overall bagged sales - NO REP FILTER (company-wide total)
        cursor.execute("""
            SELECT 
                SUM(Remainder) as total_bagged
            FROM open_sales_orders_view
            WHERE 
                (
                    -- Include Released, Date Wait, Need Payment, Stock Transfer, New always
                    (CustomField_Line_Line_Status IN ('Released', 'Date Wait', 'Need Payment', 'Stock Transfer', 'New')
                     OR CustomField_Line_Line_Status IS NULL)
                    -- Include Stock Wait ONLY if promised date is NOT in last 5 days of month
                    OR (CustomField_Line_Line_Status = 'Stock Wait' 
                        AND DATE(CustomField_Line_Promised_Date) < DATE('now', 'start of month', '+1 month', '-5 days'))
                )
                AND CustomField_Line_Line_Status NOT IN ('Blanket', 'Credit Hold')
                AND DATE(CustomField_Line_Promised_Date) >= DATE('now', 'start of month')
                AND DATE(CustomField_Line_Promised_Date) <= DATE('now', 'start of month', '+1 month', '-1 day')
                AND TemplateRef_FullName LIKE '%Sales Order%'
        """)
        total_result = cursor.fetchone()
        total_bagged = float(total_result['total_bagged'] or 0)

        # Bagged sales by rep - WITH REP FILTER (only selected reps in breakdown)
        rep_filter = ""
        if self.selected_reps:
            rep_list = "','".join(self.selected_reps)
            rep_filter = f"AND SalesRepRef_FullName IN ('{rep_list}')"

        cursor.execute(f"""
            SELECT 
                COALESCE(SalesRepRef_FullName, 'Unassigned') as rep_name,
                SUM(Remainder) as rep_bagged
            FROM open_sales_orders_view
            WHERE 
                (
                    -- Include Released, Date Wait, Need Payment, Stock Transfer, New always
                    (CustomField_Line_Line_Status IN ('Released', 'Date Wait', 'Need Payment', 'Stock Transfer', 'New')
                     OR CustomField_Line_Line_Status IS NULL)
                    -- Include Stock Wait ONLY if promised date is NOT in last 5 days of month
                    OR (CustomField_Line_Line_Status = 'Stock Wait' 
                        AND DATE(CustomField_Line_Promised_Date) < DATE('now', 'start of month', '+1 month', '-5 days'))
                )
                AND CustomField_Line_Line_Status NOT IN ('Blanket', 'Credit Hold')
                AND DATE(CustomField_Line_Promised_Date) >= DATE('now', 'start of month')
                AND DATE(CustomField_Line_Promised_Date) <= DATE('now', 'start of month', '+1 month', '-1 day')
                AND TemplateRef_FullName LIKE '%Sales Order%'
                {rep_filter}
            GROUP BY SalesRepRef_FullName
            ORDER BY rep_bagged DESC
        """)

        rep_data = {}
        for row in cursor.fetchall():
            rep_data[row['rep_name']] = float(row['rep_bagged'])

        return total_bagged, rep_data

    def get_invoiced_sales_today(self):
        """Get today's invoiced sales"""
        cursor = self.conn.cursor()

        # Overall invoiced today - NO REP FILTER (company-wide total)
        cursor.execute("""
            SELECT 
                SUM(TotalCAD) as total_invoiced
            FROM invoiced_view
            WHERE DATE(InvoiceDate) = DATE('now')
        """)
        total_result = cursor.fetchone()
        total_invoiced = float(total_result['total_invoiced'] or 0)

        # Invoiced by rep today - WITH REP FILTER (only selected reps in breakdown)
        rep_filter = ""
        if self.selected_reps:
            rep_list = "','".join(self.selected_reps)
            rep_filter = f"AND SalesRep IN ('{rep_list}')"

        cursor.execute(f"""
            SELECT 
                COALESCE(SalesRep, 'Unassigned') as rep_name,
                SUM(TotalCAD) as rep_invoiced
            FROM invoiced_view
            WHERE DATE(InvoiceDate) = DATE('now')
                {rep_filter}
            GROUP BY SalesRep
            ORDER BY rep_invoiced DESC
        """)

        rep_data = {}
        for row in cursor.fetchall():
            rep_data[row['rep_name']] = float(row['rep_invoiced'])

        return total_invoiced, rep_data

    def get_sales_by_gl_account(self):
        """Get sales breakdown by GL Account"""
        cursor = self.conn.cursor()

        # Get invoiced sales by GL Account (MTD)
        cursor.execute("""
            SELECT 
                GLAccount,
                SUM(TotalCAD) as invoiced_amount
            FROM invoiced_view
            WHERE DATE(InvoiceDate) >= DATE('now', 'start of month')
                AND DATE(InvoiceDate) <= DATE('now')
            GROUP BY GLAccount
        """)

        gl_invoiced = {}
        for row in cursor.fetchall():
            gl_invoiced[row['GLAccount']] = float(row['invoiced_amount'])

        # Get bagged sales by GL Account
        # First need to join open_sales_orders_view with item tables to get GL accounts
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN acc.AccountNumber IS NOT NULL AND acc.Name IS NOT NULL 
                    THEN acc.AccountNumber || ' · ' || acc.Name
                    WHEN acc.FullName IS NOT NULL 
                    THEN acc.FullName
                    ELSE 'No GL Account'
                END AS GLAccount,
                SUM(osov.Remainder) as bagged_amount
            FROM open_sales_orders_view osov
            -- Join to item tables to get income account
            LEFT JOIN items_inventory ii ON ii.ListID = osov.ItemRef_ListID
            LEFT JOIN items_inventory_assembly iia ON iia.ListID = osov.ItemRef_ListID
            LEFT JOIN items_noninventory ini ON ini.ListID = osov.ItemRef_ListID
            LEFT JOIN items_service isi ON isi.ListID = osov.ItemRef_ListID
            LEFT JOIN items_other_charge ioc ON ioc.ListID = osov.ItemRef_ListID
            -- Join to accounts table
            LEFT JOIN accounts acc ON acc.ListID = COALESCE(
                ii.IncomeAccountRef_ListID,
                iia.IncomeAccountRef_ListID,
                ini.SalesAndPurchase_IncomeAccountRef_ListID,
                ini.SalesOrPurchase_AccountRef_ListID,
                isi.SalesAndPurchase_IncomeAccountRef_ListID,
                isi.SalesOrPurchase_AccountRef_ListID,
                ioc.SalesOrPurchase_AccountRef_ListID
            )
            WHERE 
                (
                    (osov.CustomField_Line_Line_Status IN ('Released', 'Date Wait', 'Need Payment', 'Stock Transfer', 'New')
                     OR osov.CustomField_Line_Line_Status IS NULL)
                    OR (osov.CustomField_Line_Line_Status = 'Stock Wait' 
                        AND DATE(osov.CustomField_Line_Promised_Date) < DATE('now', 'start of month', '+1 month', '-5 days'))
                )
                AND osov.CustomField_Line_Line_Status NOT IN ('Blanket', 'Credit Hold')
                AND DATE(osov.CustomField_Line_Promised_Date) >= DATE('now', 'start of month')
                AND DATE(osov.CustomField_Line_Promised_Date) <= DATE('now', 'start of month', '+1 month', '-1 day')
                AND osov.TemplateRef_FullName LIKE '%Sales Order%'
            GROUP BY GLAccount
        """)

        gl_bagged = {}
        for row in cursor.fetchall():
            gl_bagged[row['GLAccount']] = float(row['bagged_amount'])

        # Combine all GL accounts
        all_gl_accounts = set(gl_invoiced.keys()) | set(gl_bagged.keys())

        gl_data = []
        for gl_account in sorted(all_gl_accounts):
            invoiced = gl_invoiced.get(gl_account, 0)
            bagged = gl_bagged.get(gl_account, 0)
            gl_data.append({
                'gl_account': gl_account,
                'invoiced': invoiced,
                'bagged': bagged,
                'total': invoiced + bagged
            })

        return gl_data

    def get_month_to_date_sales(self):
        """Get month-to-date invoiced sales"""
        cursor = self.conn.cursor()

        # Overall MTD - NO REP FILTER (company-wide total)
        cursor.execute("""
            SELECT 
                SUM(TotalCAD) as total_mtd
            FROM invoiced_view
            WHERE DATE(InvoiceDate) >= DATE('now', 'start of month')
                AND DATE(InvoiceDate) <= DATE('now')
        """)
        total_result = cursor.fetchone()
        total_mtd = float(total_result['total_mtd'] or 0)

        # MTD by rep - WITH REP FILTER (only selected reps in breakdown)
        rep_filter = ""
        if self.selected_reps:
            rep_list = "','".join(self.selected_reps)
            rep_filter = f"AND SalesRep IN ('{rep_list}')"

        cursor.execute(f"""
            SELECT 
                COALESCE(SalesRep, 'Unassigned') as rep_name,
                SUM(TotalCAD) as rep_mtd
            FROM invoiced_view
            WHERE DATE(InvoiceDate) >= DATE('now', 'start of month')
                AND DATE(InvoiceDate) <= DATE('now')
                {rep_filter}
            GROUP BY SalesRep
            ORDER BY rep_mtd DESC
        """)

        rep_data = {}
        for row in cursor.fetchall():
            rep_data[row['rep_name']] = float(row['rep_mtd'])

        return total_mtd, rep_data

    def save_daily_snapshot(self, bagged_total, bagged_by_rep, invoiced_total, invoiced_by_rep):
        """Save or update daily snapshot"""
        cursor = self.conn.cursor()

        # Check if record exists
        cursor.execute("""
            SELECT track_date FROM daily_sales_tracker 
            WHERE track_date = DATE('now')
        """)
        exists = cursor.fetchone() is not None

        if exists:
            cursor.execute("""
                UPDATE daily_sales_tracker
                SET bagged_sales_total = ?,
                    invoiced_sales_total = ?,
                    bagged_by_rep = ?,
                    invoiced_by_rep = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE track_date = DATE('now')
            """, (bagged_total, invoiced_total,
                  json.dumps(bagged_by_rep), json.dumps(invoiced_by_rep)))
        else:
            cursor.execute("""
                INSERT INTO daily_sales_tracker 
                (track_date, bagged_sales_total, invoiced_sales_total, bagged_by_rep, invoiced_by_rep)
                VALUES (DATE('now'), ?, ?, ?, ?)
            """, (bagged_total, invoiced_total,
                  json.dumps(bagged_by_rep), json.dumps(invoiced_by_rep)))

        self.conn.commit()

    def get_historical_data(self, days=30):
        """Get historical daily data for trend analysis"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                track_date,
                bagged_sales_total,
                invoiced_sales_total
            FROM daily_sales_tracker
            WHERE track_date >= DATE('now', '-' || ? || ' days')
            ORDER BY track_date DESC
        """, (days,))

        return cursor.fetchall()

    def analyze_status_breakdown(self):
        """Analyze current month's orders by status for diagnostic purposes"""
        cursor = self.conn.cursor()
        print("\n=== DIAGNOSTIC: Current Month Status Breakdown ===")

        # Get the last day of current month minus 5 days
        cursor.execute("SELECT DATE('now', 'start of month', '+1 month', '-5 days') as cutoff_date")
        cutoff_date = cursor.fetchone()['cutoff_date']
        print(f"Stock Wait cutoff date (5 days before month end): {cutoff_date}")

        cursor.execute("""
            SELECT 
                CustomField_Line_Line_Status as status,
                COUNT(*) as count,
                SUM(Remainder) as total_value,
                SUM(CASE 
                    WHEN CustomField_Line_Line_Status = 'Stock Wait' 
                         AND DATE(CustomField_Line_Promised_Date) >= DATE('now', 'start of month', '+1 month', '-5 days')
                    THEN Remainder 
                    ELSE 0 
                END) as excluded_stock_wait,
                printf('%.1f%%', (SUM(Remainder) * 100.0 / 
                    (SELECT SUM(Remainder) FROM open_sales_orders_view 
                     WHERE DATE(CustomField_Line_Promised_Date) <= DATE('now', 'start of month', '+1 month', '-1 day')
                     AND DATE(CustomField_Line_Promised_Date) >= DATE('now')))) as pct_of_month
            FROM open_sales_orders_view
            WHERE DATE(CustomField_Line_Promised_Date) <= DATE('now', 'start of month', '+1 month', '-1 day')
                AND DATE(CustomField_Line_Promised_Date) >= DATE('now')
            GROUP BY CustomField_Line_Line_Status
            ORDER BY total_value DESC
        """)

        print(f"\n{'Status':<15} {'Count':>6} {'Value':>12} {'Excluded':>12} {'% of Month':>10}")
        print("-" * 65)
        for row in cursor.fetchall():
            status = row['status'] or 'None'
            excluded = f"${row['excluded_stock_wait']:,.0f}" if row['excluded_stock_wait'] > 0 else "-"
            print(
                f"{status:<15} {row['count']:>6} ${row['total_value']:>11,.0f} {excluded:>12} {row['pct_of_month']:>10}")

        print(
            "\nNote: Stock Wait orders with promised dates in the last 5 days of the month are excluded from bagged sales.")
        print("=" * 65)

    def format_currency(self, amount):
        """Format number as currency"""
        return f"${amount:,.2f}"

    def generate_pdf_report(self, output_path='goal_tracker_report.pdf'):
        """Generate the PDF report with file lock handling"""
        # Get all data
        bagged_total, bagged_by_rep = self.get_bagged_sales()
        invoiced_total, invoiced_by_rep = self.get_invoiced_sales_today()
        mtd_total, mtd_by_rep = self.get_month_to_date_sales()
        monthly_target = self.get_monthly_target()

        # Calculate month progress based on BUSINESS DAYS
        business_days_total = self.get_business_days_in_month()
        business_days_elapsed = self.get_business_days_elapsed()
        month_progress = (business_days_elapsed / business_days_total) * 100 if business_days_total > 0 else 0

        # Calculate projected total and gap
        projected_total = mtd_total + bagged_total
        gap_to_target = monthly_target - projected_total

        # Performance index: goal progress / time progress
        goal_progress = (projected_total / monthly_target * 100) if monthly_target > 0 else 0
        performance_index = (goal_progress / month_progress) if month_progress > 0 else 0

        # Save snapshot if business day
        if self.is_business_day(self.today):
            self.save_daily_snapshot(bagged_total, bagged_by_rep, invoiced_total, invoiced_by_rep)

        # Try to create PDF with file lock handling
        final_output_path = output_path
        attempt = 0
        max_attempts = 5

        while attempt < max_attempts:
            try:
                # Create PDF
                doc = SimpleDocTemplate(final_output_path, pagesize=landscape(letter))
                story = []
                styles = getSampleStyleSheet()

                # Title
                title_style = ParagraphStyle(
                    'CustomTitle',
                    parent=styles['Heading1'],
                    fontSize=24,
                    textColor=colors.HexColor('#1f4788'),
                    spaceAfter=30,
                    alignment=1  # Center
                )
                story.append(Paragraph(f"Goal Tracker III - Daily Sales Report", title_style))
                story.append(Paragraph(f"{self.today.strftime('%B %d, %Y')}", styles['Normal']))
                story.append(Spacer(1, 0.5 * inch))

                # Progress indicators
                progress_data = [
                    ['Business Days Progress',
                     f"{business_days_elapsed}/{business_days_total} days ({month_progress:.1f}%)"],
                    ['Performance Index', f"{performance_index:.2f}" + (" 🟢" if performance_index >= 1.0 else " 🔴")],
                ]

                progress_table = Table(progress_data, colWidths=[2.5 * inch, 2.5 * inch])
                progress_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, -1), colors.lightyellow),
                    ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 11),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('ALIGN', (1, 0), (1, -1), 'CENTER'),
                ]))
                story.append(progress_table)
                story.append(Spacer(1, 0.3 * inch))

                # Summary metrics with Gap
                summary_data = [
                    ['Metric', 'Amount', 'Progress'],
                    ['Monthly Target', self.format_currency(monthly_target), ''],
                    ['MTD Invoiced Sales', self.format_currency(mtd_total),
                     f"{(mtd_total / monthly_target * 100):.1f}%" if monthly_target > 0 else "N/A"],
                    ['Bagged Sales (Month-End)', self.format_currency(bagged_total), ''],
                    ['Projected Month Total', self.format_currency(projected_total),
                     f"{(projected_total / monthly_target * 100):.1f}%" if monthly_target > 0 else "N/A"],
                    ['Gap to Target', self.format_currency(abs(gap_to_target)),
                     "OVER TARGET!" if gap_to_target < 0 else "SHORT"],
                    ['Today\'s Invoiced', self.format_currency(invoiced_total), '']
                ]

                summary_table = Table(summary_data, colWidths=[3 * inch, 2 * inch, 1.5 * inch])
                summary_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 12),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                    # Highlight the gap row
                    ('BACKGROUND', (0, 5), (-1, 5), colors.lightcoral if gap_to_target > 0 else colors.lightgreen),
                    ('FONTNAME', (0, 5), (-1, 5), 'Helvetica-Bold'),
                ]))
                story.append(summary_table)
                story.append(Spacer(1, 0.5 * inch))

                # Visual Progress Bar
                drawing = Drawing(600, 50)

                # Background bar
                bar_width = 500
                bar_height = 30
                bar_x = 50
                bar_y = 10

                # Background
                drawing.add(Rect(bar_x, bar_y, bar_width, bar_height,
                                 fillColor=colors.lightgrey, strokeColor=colors.black))

                # Progress bar (goal progress)
                progress_width = min(bar_width * (goal_progress / 100), bar_width)
                progress_color = colors.green if goal_progress >= month_progress else colors.orange
                drawing.add(Rect(bar_x, bar_y, progress_width, bar_height,
                                 fillColor=progress_color, strokeColor=None))

                # Month progress line (vertical line showing where we should be)
                month_line_x = bar_x + (bar_width * month_progress / 100)
                drawing.add(Rect(month_line_x - 2, bar_y - 5, 4, bar_height + 10,
                                 fillColor=colors.red, strokeColor=None))

                # Labels
                drawing.add(String(bar_x + progress_width / 2, bar_y + bar_height / 2,
                                   f"Sales: {goal_progress:.1f}%",
                                   fontSize=12, fillColor=colors.white, textAnchor='middle'))
                drawing.add(String(month_line_x, bar_y - 10,
                                   f"Day {business_days_elapsed} of {business_days_total}",
                                   fontSize=10, fillColor=colors.red, textAnchor='middle'))

                story.append(drawing)
                story.append(Spacer(1, 0.2 * inch))

                # Add dynamic performance explanation
                perf_explanation_style = ParagraphStyle(
                    'PerfExplanation',
                    parent=styles['Normal'],
                    fontSize=10,
                    textColor=colors.HexColor('#444444'),
                    alignment=1,  # Center
                    spaceAfter=20
                )

                # Create dynamic explanation based on performance
                if performance_index >= 1.0:
                    perf_status = "ahead of schedule"
                    perf_color = "green"
                    perf_emoji = "✅"
                    perf_detail = f"You're {(performance_index - 1) * 100:.1f}% ahead of where you need to be!"
                else:
                    perf_status = "behind schedule"
                    perf_color = "orange"
                    perf_emoji = "⚠️"
                    deficit_percent = (1 - performance_index) * 100
                    perf_detail = f"You need to pick up the pace by {deficit_percent:.1f}% to meet your goal."

                explanation_text = (
                    f"<b>Performance Index: {performance_index:.2f}</b> {perf_emoji}<br/>"
                    f"You are {business_days_elapsed} business days into the month (out of {business_days_total} total). "
                    f"This represents {month_progress:.1f}% of available selling days.<br/>"
                    f"Your sales progress ({goal_progress:.1f}%) is <font color='{perf_color}'>{perf_status}</font>. "
                    f"{perf_detail}"
                )

                story.append(Paragraph(explanation_text, perf_explanation_style))
                story.append(Spacer(1, 0.3 * inch))

                # Sales by Rep - MTD (Keep together with its table)
                rep_section = []
                rep_heading_style = ParagraphStyle(
                    'RepHeading',
                    parent=styles['Heading2'],
                    keepWithNext=True,  # This keeps the heading with the table
                    spaceAfter=12
                )
                rep_section.append(Paragraph("Month-to-Date Sales by Representative", rep_heading_style))

                # Combine all reps
                all_reps = set(mtd_by_rep.keys()) | set(bagged_by_rep.keys())
                rep_data = [['Sales Rep', 'MTD Invoiced', 'Bagged Sales', 'Projected Total']]

                for rep in sorted(all_reps):
                    mtd = mtd_by_rep.get(rep, 0)
                    bagged = bagged_by_rep.get(rep, 0)
                    rep_data.append([
                        rep,
                        self.format_currency(mtd),
                        self.format_currency(bagged),
                        self.format_currency(mtd + bagged)
                    ])

                # Add totals row
                rep_data.append([
                    'TOTAL',
                    self.format_currency(mtd_total),
                    self.format_currency(bagged_total),
                    self.format_currency(mtd_total + bagged_total)
                ])

                rep_table = Table(rep_data, colWidths=[3 * inch, 2 * inch, 2 * inch, 2 * inch])
                rep_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 11),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
                    ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                ]))

                # Use KeepTogether to prevent splitting
                from reportlab.platypus import KeepTogether
                story.append(KeepTogether([
                    Paragraph("Month-to-Date Sales by Representative", rep_heading_style),
                    rep_table
                ]))

                story.append(Spacer(1, 0.4 * inch))

                # Sales by GL Account (also keep together)
                gl_heading_style = ParagraphStyle(
                    'GLHeading',
                    parent=styles['Heading2'],
                    keepWithNext=True,
                    spaceAfter=12
                )

                gl_data = self.get_sales_by_gl_account()
                if gl_data:
                    gl_elements = []
                    gl_elements.append(Paragraph("Sales by GL Account", gl_heading_style))

                    gl_table_data = [['GL Account', 'MTD Invoiced', 'Bagged Sales', 'Total']]

                    # Sort by total descending
                    gl_data.sort(key=lambda x: x['total'], reverse=True)

                    total_invoiced_gl = 0
                    total_bagged_gl = 0

                    # Limit to top 15 GL accounts to fit on page
                    display_limit = 15
                    other_invoiced = 0
                    other_bagged = 0

                    for i, gl in enumerate(gl_data):
                        if i < display_limit:
                            gl_table_data.append([
                                gl['gl_account'][:50] + '...' if len(gl['gl_account']) > 50 else gl['gl_account'],
                                self.format_currency(gl['invoiced']),
                                self.format_currency(gl['bagged']),
                                self.format_currency(gl['total'])
                            ])
                        else:
                            other_invoiced += gl['invoiced']
                            other_bagged += gl['bagged']

                        total_invoiced_gl += gl['invoiced']
                        total_bagged_gl += gl['bagged']

                    # Add "Other accounts" row if needed
                    if len(gl_data) > display_limit:
                        gl_table_data.append([
                            f'Other ({len(gl_data) - display_limit} accounts)',
                            self.format_currency(other_invoiced),
                            self.format_currency(other_bagged),
                            self.format_currency(other_invoiced + other_bagged)
                        ])

                    # Add totals row
                    gl_table_data.append([
                        'TOTAL',
                        self.format_currency(total_invoiced_gl),
                        self.format_currency(total_bagged_gl),
                        self.format_currency(total_invoiced_gl + total_bagged_gl)
                    ])

                    gl_table = Table(gl_table_data, colWidths=[3.5 * inch, 2 * inch, 2 * inch, 2 * inch])
                    gl_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 11),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
                        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black),
                        ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                        ('FONTSIZE', (0, 1), (0, -2), 9),  # Smaller font for GL account names
                    ]))

                    gl_elements.append(gl_table)

                    # Keep GL section together
                    story.append(KeepTogether(gl_elements))
                else:
                    story.append(Paragraph("No GL Account data available.", styles['Normal']))

                # Add page break
                story.append(PageBreak())

                # Today's activity by rep
                story.append(Paragraph("Today's Invoiced Sales by Representative", styles['Heading2']))

                if invoiced_by_rep:
                    today_data = [['Sales Rep', 'Amount']]
                    for rep, amount in sorted(invoiced_by_rep.items(), key=lambda x: x[1], reverse=True):
                        today_data.append([rep, self.format_currency(amount)])
                    today_data.append(['TOTAL', self.format_currency(invoiced_total)])

                    today_table = Table(today_data, colWidths=[3 * inch, 2 * inch])
                    today_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 11),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
                        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black),
                        ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                    ]))
                    story.append(today_table)
                else:
                    story.append(Paragraph("No sales invoiced today.", styles['Normal']))

                # Build PDF
                doc.build(story)
                print(f"Report generated: {final_output_path}")
                return final_output_path

            except PermissionError as e:
                # File is locked, try alternative name
                attempt += 1
                if attempt >= max_attempts:
                    # Final attempt: use timestamp
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    final_output_path = f"Goal_Tracker_{timestamp}.pdf"
                else:
                    # Try numbered alternatives
                    base_name = os.path.splitext(output_path)[0]
                    final_output_path = f"{base_name}_{attempt}.pdf"

                print(f"File locked, trying: {final_output_path}")
                continue

            except Exception as e:
                print(f"Error generating PDF: {e}")
                raise

        # If we get here, all attempts failed
        raise Exception(f"Could not generate PDF after {max_attempts} attempts")
        """Generate the PDF report"""
        # Get all data
        bagged_total, bagged_by_rep = self.get_bagged_sales()
        invoiced_total, invoiced_by_rep = self.get_invoiced_sales_today()
        mtd_total, mtd_by_rep = self.get_month_to_date_sales()
        monthly_target = self.get_monthly_target()

        # Calculate month progress based on BUSINESS DAYS
        business_days_total = self.get_business_days_in_month()
        business_days_elapsed = self.get_business_days_elapsed()
        month_progress = (business_days_elapsed / business_days_total) * 100 if business_days_total > 0 else 0

        # Calculate projected total and gap
        projected_total = mtd_total + bagged_total
        gap_to_target = monthly_target - projected_total

        # Performance index: goal progress / time progress
        goal_progress = (projected_total / monthly_target * 100) if monthly_target > 0 else 0
        performance_index = (goal_progress / month_progress) if month_progress > 0 else 0

        # Save snapshot if business day
        if self.is_business_day(self.today):
            self.save_daily_snapshot(bagged_total, bagged_by_rep, invoiced_total, invoiced_by_rep)

        # Create PDF
        doc = SimpleDocTemplate(output_path, pagesize=landscape(letter))
        story = []
        styles = getSampleStyleSheet()

        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1f4788'),
            spaceAfter=30,
            alignment=1  # Center
        )
        story.append(Paragraph(f"Goal Tracker III - Daily Sales Report", title_style))
        story.append(Paragraph(f"{self.today.strftime('%B %d, %Y')}", styles['Normal']))
        story.append(Spacer(1, 0.5 * inch))

        # Progress indicators
        progress_data = [
            ['Business Days Progress', f"{business_days_elapsed}/{business_days_total} days ({month_progress:.1f}%)"],
            ['Performance Index', f"{performance_index:.2f}" + (" 🟢" if performance_index >= 1.0 else " 🔴")],
        ]

        progress_table = Table(progress_data, colWidths=[2.5 * inch, 2.5 * inch])
        progress_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.lightyellow),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
        ]))
        story.append(progress_table)
        story.append(Spacer(1, 0.3 * inch))

        # Summary metrics with Gap
        summary_data = [
            ['Metric', 'Amount', 'Progress'],
            ['Monthly Target', self.format_currency(monthly_target), ''],
            ['MTD Invoiced Sales', self.format_currency(mtd_total),
             f"{(mtd_total / monthly_target * 100):.1f}%" if monthly_target > 0 else "N/A"],
            ['Bagged Sales (Month-End)', self.format_currency(bagged_total), ''],
            ['Projected Month Total', self.format_currency(projected_total),
             f"{(projected_total / monthly_target * 100):.1f}%" if monthly_target > 0 else "N/A"],
            ['Gap to Target', self.format_currency(abs(gap_to_target)),
             "OVER TARGET!" if gap_to_target < 0 else "SHORT"],
            ['Today\'s Invoiced', self.format_currency(invoiced_total), '']
        ]

        summary_table = Table(summary_data, colWidths=[3 * inch, 2 * inch, 1.5 * inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
            # Highlight the gap row
            ('BACKGROUND', (0, 5), (-1, 5), colors.lightcoral if gap_to_target > 0 else colors.lightgreen),
            ('FONTNAME', (0, 5), (-1, 5), 'Helvetica-Bold'),
        ]))
        story.append(summary_table)
        story.append(Spacer(1, 0.5 * inch))

        # Visual Progress Bar
        drawing = Drawing(600, 50)

        # Background bar
        bar_width = 500
        bar_height = 30
        bar_x = 50
        bar_y = 10

        # Background
        drawing.add(Rect(bar_x, bar_y, bar_width, bar_height,
                         fillColor=colors.lightgrey, strokeColor=colors.black))

        # Progress bar (goal progress)
        progress_width = min(bar_width * (goal_progress / 100), bar_width)
        progress_color = colors.green if goal_progress >= month_progress else colors.orange
        drawing.add(Rect(bar_x, bar_y, progress_width, bar_height,
                         fillColor=progress_color, strokeColor=None))

        # Month progress line (vertical line showing where we should be)
        month_line_x = bar_x + (bar_width * month_progress / 100)
        drawing.add(Rect(month_line_x - 2, bar_y - 5, 4, bar_height + 10,
                         fillColor=colors.red, strokeColor=None))

        # Labels
        drawing.add(String(bar_x + progress_width / 2, bar_y + bar_height / 2,
                           f"Sales: {goal_progress:.1f}%",
                           fontSize=12, fillColor=colors.white, textAnchor='middle'))
        drawing.add(String(month_line_x, bar_y - 10,
                           f"Day {business_days_elapsed} of {business_days_total}",
                           fontSize=10, fillColor=colors.red, textAnchor='middle'))

        story.append(drawing)
        story.append(Spacer(1, 0.2 * inch))

        # Add dynamic performance explanation
        perf_explanation_style = ParagraphStyle(
            'PerfExplanation',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.HexColor('#444444'),
            alignment=1,  # Center
            spaceAfter=20
        )

        # Create dynamic explanation based on performance
        if performance_index >= 1.0:
            perf_status = "ahead of schedule"
            perf_color = "green"
            perf_emoji = "✅"
            perf_detail = f"You're {(performance_index - 1) * 100:.1f}% ahead of where you need to be!"
        else:
            perf_status = "behind schedule"
            perf_color = "orange"
            perf_emoji = "⚠️"
            deficit_percent = (1 - performance_index) * 100
            perf_detail = f"You need to pick up the pace by {deficit_percent:.1f}% to meet your goal."

        explanation_text = (
            f"<b>Performance Index: {performance_index:.2f}</b> {perf_emoji}<br/>"
            f"You are {business_days_elapsed} business days into the month (out of {business_days_total} total). "
            f"This represents {month_progress:.1f}% of available selling days.<br/>"
            f"Your sales progress ({goal_progress:.1f}%) is <font color='{perf_color}'>{perf_status}</font>. "
            f"{perf_detail}"
        )

        story.append(Paragraph(explanation_text, perf_explanation_style))
        story.append(Spacer(1, 0.3 * inch))

        # Sales by Rep - MTD (Keep together with its table)
        rep_section = []
        rep_heading_style = ParagraphStyle(
            'RepHeading',
            parent=styles['Heading2'],
            keepWithNext=True,  # This keeps the heading with the table
            spaceAfter=12
        )
        rep_section.append(Paragraph("Month-to-Date Sales by Representative", rep_heading_style))

        # Combine all reps
        all_reps = set(mtd_by_rep.keys()) | set(bagged_by_rep.keys())
        rep_data = [['Sales Rep', 'MTD Invoiced', 'Bagged Sales', 'Projected Total']]

        for rep in sorted(all_reps):
            mtd = mtd_by_rep.get(rep, 0)
            bagged = bagged_by_rep.get(rep, 0)
            rep_data.append([
                rep,
                self.format_currency(mtd),
                self.format_currency(bagged),
                self.format_currency(mtd + bagged)
            ])

        # Add totals row
        rep_data.append([
            'TOTAL',
            self.format_currency(mtd_total),
            self.format_currency(bagged_total),
            self.format_currency(mtd_total + bagged_total)
        ])

        rep_table = Table(rep_data, colWidths=[3 * inch, 2 * inch, 2 * inch, 2 * inch])
        rep_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
        ]))

        # Use KeepTogether to prevent splitting
        from reportlab.platypus import KeepTogether
        story.append(KeepTogether([
            Paragraph("Month-to-Date Sales by Representative", rep_heading_style),
            rep_table
        ]))

        story.append(Spacer(1, 0.4 * inch))

        # Sales by GL Account (also keep together)
        gl_heading_style = ParagraphStyle(
            'GLHeading',
            parent=styles['Heading2'],
            keepWithNext=True,
            spaceAfter=12
        )

        gl_data = self.get_sales_by_gl_account()
        if gl_data:
            gl_elements = []
            gl_elements.append(Paragraph("Sales by GL Account", gl_heading_style))

            gl_table_data = [['GL Account', 'MTD Invoiced', 'Bagged Sales', 'Total']]

            # Sort by total descending
            gl_data.sort(key=lambda x: x['total'], reverse=True)

            total_invoiced_gl = 0
            total_bagged_gl = 0

            # Limit to top 15 GL accounts to fit on page
            display_limit = 15
            other_invoiced = 0
            other_bagged = 0

            for i, gl in enumerate(gl_data):
                if i < display_limit:
                    gl_table_data.append([
                        gl['gl_account'][:50] + '...' if len(gl['gl_account']) > 50 else gl['gl_account'],
                        self.format_currency(gl['invoiced']),
                        self.format_currency(gl['bagged']),
                        self.format_currency(gl['total'])
                    ])
                else:
                    other_invoiced += gl['invoiced']
                    other_bagged += gl['bagged']

                total_invoiced_gl += gl['invoiced']
                total_bagged_gl += gl['bagged']

            # Add "Other accounts" row if needed
            if len(gl_data) > display_limit:
                gl_table_data.append([
                    f'Other ({len(gl_data) - display_limit} accounts)',
                    self.format_currency(other_invoiced),
                    self.format_currency(other_bagged),
                    self.format_currency(other_invoiced + other_bagged)
                ])

            # Add totals row
            gl_table_data.append([
                'TOTAL',
                self.format_currency(total_invoiced_gl),
                self.format_currency(total_bagged_gl),
                self.format_currency(total_invoiced_gl + total_bagged_gl)
            ])

            gl_table = Table(gl_table_data, colWidths=[3.5 * inch, 2 * inch, 2 * inch, 2 * inch])
            gl_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
                ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                ('FONTSIZE', (0, 1), (0, -2), 9),  # Smaller font for GL account names
            ]))

            gl_elements.append(gl_table)

            # Keep GL section together
            story.append(KeepTogether(gl_elements))
        else:
            story.append(Paragraph("No GL Account data available.", styles['Normal']))

        # Add page break
        story.append(PageBreak())

        # Today's activity by rep
        story.append(Paragraph("Today's Invoiced Sales by Representative", styles['Heading2']))

        if invoiced_by_rep:
            today_data = [['Sales Rep', 'Amount']]
            for rep, amount in sorted(invoiced_by_rep.items(), key=lambda x: x[1], reverse=True):
                today_data.append([rep, self.format_currency(amount)])
            today_data.append(['TOTAL', self.format_currency(invoiced_total)])

            today_table = Table(today_data, colWidths=[3 * inch, 2 * inch])
            today_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
                ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
            ]))
            story.append(today_table)
        else:
            story.append(Paragraph("No sales invoiced today.", styles['Normal']))

        # Build PDF
        doc.build(story)
        print(f"Report generated: {output_path}")

        return output_path

    def get_all_reps(self):
        """Get list of all sales reps in the system"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT SalesRepRef_FullName as rep_name
            FROM open_sales_orders_view
            WHERE SalesRepRef_FullName IS NOT NULL
            UNION
            SELECT DISTINCT SalesRep as rep_name
            FROM invoiced_view
            WHERE SalesRep IS NOT NULL
            ORDER BY rep_name
        """)
        return cursor.fetchall()

    def get_business_days_in_month(self):
        """Calculate total business days in current month"""
        import calendar
        year = self.today.year
        month = self.today.month

        # Get first and last day of month
        first_day = date(year, month, 1)
        last_day = date(year, month, calendar.monthrange(year, month)[1])

        # Count business days using timedelta for proper date arithmetic
        business_days = 0
        current = first_day
        while current <= last_day:
            if self.is_business_day(current):
                business_days += 1
            current += timedelta(days=1)  # Use timedelta for safe date increment

        return business_days

    def get_business_days_elapsed(self):
        """Calculate business days elapsed in current month"""
        first_day = date(self.today.year, self.today.month, 1)

        # Count business days from start of month to today
        business_days = 0
        current = first_day
        while current <= self.today:
            if self.is_business_day(current):
                business_days += 1
            current += timedelta(days=1)  # Use timedelta for safe date increment

        return business_days

    def close(self):
        """Close database connection"""
        self.conn.close()


# Main execution
if __name__ == "__main__":
    # Configuration
    DB_PATH = r"C:\quickbookssync\data\quickbooks_data.db"
    OUTPUT_PATH = f"Goal_Tracker_{date.today().strftime('%Y%m%d')}.pdf"

    # CONFIGURATION: Select specific reps or None for all reps
    # Check environment variable first (from report manager)
    import os
    import json

    SELECTED_REPS = None
    if 'GOAL_TRACKER_REPS' in os.environ:
        try:
            SELECTED_REPS = json.loads(os.environ['GOAL_TRACKER_REPS'])
        except:
            pass

    # If no environment variable, check config file
    if not SELECTED_REPS:
        try:
            with open('goal_tracker_config.json', 'r') as f:
                config = json.load(f)
                if config.get('filter_enabled') and config.get('selected_reps'):
                    SELECTED_REPS = config['selected_reps']
        except:
            # Default if no config
            SELECTED_REPS = ['AL', 'CL', 'GM', 'HA', 'KG', 'PC', 'YD']

    # Create tables if they don't exist
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create tracking table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_sales_tracker (
            track_date DATE PRIMARY KEY,
            bagged_sales_total REAL,
            invoiced_sales_total REAL,
            bagged_by_rep TEXT,
            invoiced_by_rep TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create targets table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS monthly_targets (
            year_month TEXT PRIMARY KEY,
            target_amount REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert sample target for current month (adjust as needed)
    current_month = date.today().strftime('%Y-%m')
    cursor.execute("""
        INSERT OR IGNORE INTO monthly_targets (year_month, target_amount)
        VALUES (?, ?)
    """, (current_month, 1000000.0))  # Set your target here

    # Create report tracking tables
    try:
        # Read and execute the schema
        schema_file = 'report_tracker_schema.sql'
        if os.path.exists(schema_file):
            with open(schema_file, 'r') as f:
                cursor.executescript(f.read())
    except:
        pass  # Schema might already exist

    conn.commit()
    conn.close()

    # Generate report
    tracker = GoalTrackerIII(DB_PATH, selected_reps=SELECTED_REPS)
    try:
        # Show available reps if needed
        print("\nAvailable Sales Reps:")
        all_reps = tracker.get_all_reps()
        if all_reps:
            for i, rep in enumerate(all_reps, 1):
                print(f"  {i}. {rep['rep_name']}")
        else:
            print("  No sales reps found in the database.")

        # Show diagnostic info first
        tracker.analyze_status_breakdown()

        # Generate the PDF report
        tracker.generate_pdf_report(OUTPUT_PATH)
        print(f"\nReport successfully generated: {OUTPUT_PATH}")

        if SELECTED_REPS:
            print(f"Report filtered for reps: {', '.join(SELECTED_REPS)}")
        else:
            print("Report includes all sales reps")

    except Exception as e:
        print(f"\nError generating report: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)  # Exit with error code for report manager
    finally:
        tracker.close()