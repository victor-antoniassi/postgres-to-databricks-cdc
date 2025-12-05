"""
Databricks Table Inspection Script

Inspects table schema and control columns to determine delete strategy (Soft vs Hard)
and visibility of metadata.

Usage:
    uv run scripts/inspect_table.py
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

def inspect():
    console.print(Panel.fit(
        "[bold cyan]TABLE INSPECTION: chinook_lakehouse.bronze.invoice[/bold cyan]",
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

    target_table = "chinook_lakehouse.bronze.invoice"

    with connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cursor:
            
            # 1. Get Columns (Describe)
            logger.info("Fetching table schema...")
            cursor.execute(f"DESCRIBE {target_table}")
            columns = cursor.fetchall()
            
            schema_table = Table(title="Table Schema (Columns)", show_header=True)
            schema_table.add_column("Column Name", style="cyan")
            schema_table.add_column("Type", style="green")
            schema_table.add_column("Comment", style="yellow")
            
            dlt_cols = []
            has_deleted_ts = False
            
            for col in columns:
                name = col.col_name
                dtype = col.data_type
                comment = col.comment if col.comment else ""
                
                if name.startswith("_dlt") or name in ["lsn", "deleted_ts"]:
                    dlt_cols.append(name)
                    schema_table.add_row(name, dtype, "[bold magenta]Control Column[/bold magenta]")
                else:
                    schema_table.add_row(name, dtype, comment)
                
                if name == "deleted_ts":
                    has_deleted_ts = True
            
            console.print(schema_table)
            
            # 2. Analysis
            console.print("\n[bold]Analysis:[/bold]")
            if has_deleted_ts:
                console.print("- Found [magenta]deleted_ts[/magenta] column.")
            else:
                console.print("- [magenta]deleted_ts[/magenta] column NOT found.")
                
            # 3. Check for Soft Deleted rows (if column exists)
            if has_deleted_ts:
                cursor.execute(f"SELECT count(*) FROM {target_table} WHERE deleted_ts IS NOT NULL")
                soft_deleted_count = cursor.fetchone()[0]
                console.print(f"- Rows with deleted_ts IS NOT NULL: [bold]{soft_deleted_count}[/bold]")
                
                if soft_deleted_count == 0:
                    console.print("  (If records were deleted but this is 0, it implies [bold red]HARD DELETE[/bold red] logic is purging them)")
            
            # 4. Sample Data with Control Columns
            logger.info("\nFetching sample row with control columns...")
            # Construct query selecting known control columns explicitly if they verify existing
            cols_to_select = ["invoice_id", "_dlt_load_id", "_dlt_id"]
            if "lsn" in dlt_cols:
                cols_to_select.append("lsn")
            if "deleted_ts" in dlt_cols:
                cols_to_select.append("deleted_ts")
            
            query = f"SELECT {', '.join(cols_to_select)} FROM {target_table} LIMIT 1"
            
            try:
                cursor.execute(query)
                row = cursor.fetchone()
                if row:
                    sample_table = Table(title="Sample Control Data", show_header=True)
                    for col in cols_to_select:
                        sample_table.add_column(col)
                    
                    # Map row values dynamically
                    vals = [str(getattr(row, c)) for c in cols_to_select]
                    sample_table.add_row(*vals)
                    console.print(sample_table)
            except Exception as e:
                logger.error(f"Could not fetch sample: {e}")

if __name__ == "__main__":
    inspect()
