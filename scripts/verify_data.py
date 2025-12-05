"""
Databricks Data Verification Script

Verifies that CDC transactions (Inserts, Updates, Deletes) were correctly
applied to the Databricks Delta tables.

IMPORTANT: The IDs for verification (inserts, updates, deletes) are hardcoded
within this script. They MUST be manually adjusted to match the IDs generated
by the `simulate_transactions.py` script for the current test run.

Usage:
    uv run scripts/verify_data.py
"""

import os
import sys
import dlt
from databricks.sql import connect
from rich.panel import Panel
from rich.console import Console
from rich.table import Table

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from postgres_cdc.utils.logger import setup_logger

logger = setup_logger(__name__)
console = Console()

def verify():
    console.print(Panel.fit(
        "[bold cyan]DATABRICKS DATA VERIFICATION[/bold cyan]\n"
        "[italic]Checking Bronze Tables for CDC Changes[/italic]",
        border_style="cyan"
    ))

    # Load credentials
    try:
        host = dlt.secrets.get("destination.databricks.credentials.server_hostname", str)
        http_path = dlt.secrets.get("destination.databricks.credentials.http_path", str)
        token = dlt.secrets.get("destination.databricks.credentials.access_token", str)
    except Exception as e:
        logger.error(f"Failed to load secrets: {e}")
        return

    target_table = "dev_chinook_lakehouse.bronze.invoice"

    with connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cursor:
            
            # 1. Verify INSERTS (IDs 1595 - 1604)
            logger.info("Verifying [bold green]INSERTS[/bold green] (Expected IDs: 1595-1604)...")
            query_inserts = f"""
                SELECT invoice_id, customer_id, invoice_date, total, _dlt_load_id
                FROM {target_table}
                WHERE invoice_id BETWEEN 1595 AND 1604
                ORDER BY invoice_id
            """
            cursor.execute(query_inserts)
            rows = cursor.fetchall()
            
            if rows:
                table = Table(title="New Invoices Found", show_header=True)
                table.add_column("InvoiceId", style="cyan")
                table.add_column("Total", style="green")
                table.add_column("Date", style="yellow")
                for row in rows:
                    table.add_row(str(row.invoice_id), str(row.total), str(row.invoice_date))
                console.print(table)
                
                if len(rows) == 10:
                    logger.info("[bold green]✓ Success:[/bold green] All 10 inserted records found.")
                else:
                    logger.warning(f"[bold yellow]![/bold yellow] Found {len(rows)}/10 records.")
            else:
                logger.error("[bold red]✗ Failure:[/bold red] No inserted records found!")

            # 2. Verify DELETES (ID 811)
            logger.info("\nVerifying [bold red]DELETES[/bold red] (ID 811)...")
            query_delete = f"SELECT * FROM {target_table} WHERE invoice_id = 811"
            cursor.execute(query_delete)
            deleted_rows = cursor.fetchall()
            
            if not deleted_rows:
                logger.info("[bold green]✓ Success:[/bold green] Invoice 811 does not exist (Confirmed Deleted).")
            else:
                # Check if it's soft deleted (if columns exist) or raw row
                logger.error(f"[bold red]✗ Failure:[/bold red] Invoice 811 still exists! (Found {len(deleted_rows)} row(s))")
                console.print(deleted_rows)

            # 3. Verify UPDATES (IDs 1110, 479)
            logger.info("\nVerifying [bold blue]UPDATES[/bold blue] (IDs 1110, 479)...")
            query_updates = f"""
                SELECT invoice_id, total, billing_address
                FROM {target_table}
                WHERE invoice_id IN (1110, 479)
            """
            cursor.execute(query_updates)
            update_rows = cursor.fetchall()
            
            if update_rows:
                table = Table(title="Updated Invoices", show_header=True)
                table.add_column("InvoiceId", style="cyan")
                table.add_column("BillingAddress", style="white")
                for row in update_rows:
                    table.add_row(str(row.invoice_id), str(row.billing_address))
                console.print(table)
                logger.info(f"[bold green]✓ Success:[/bold green] Found {len(update_rows)}/2 updated records.")
            else:
                logger.error("[bold red]✗ Failure:[/bold red] Updated records not found.")

if __name__ == "__main__":
    verify()
