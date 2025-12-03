
"""
Simulates test transactions (inserts, updates, and deletes) using an external
simulator wrapper.

This script acts as a wrapper to call the main.py simulate script from the
external project 'day-1_sales_data_generator'.

External Project: https://github.com/victor-antoniassi/day-1_sales_data_generator
Note: You must have the external project cloned locally at the path defined in
CHINOOK_DB_PROJECT_PATH for this script to work.

Usage:
    uv run scripts/simulate_transactions.py [inserts] [updates] [deletes]
"""


import sys
import os
import subprocess
from pathlib import Path
from rich.panel import Panel
from rich.console import Console
from rich.table import Table

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
import random
import time
from datetime import datetime
from rich.console import Console
from rich.panel import Panel

import dlt
from dlt.sources.credentials import ConnectionStringCredentials
import psycopg2

# Updated import to point to the installed package
from postgres_cdc.utils.logger import setup_logger

logger = setup_logger(__name__)
console = Console()

logger = setup_logger(__name__)
console = Console()

# Path to the external chinook_db project
CHINOOK_DB_PROJECT_PATH = "/home/onne/projects/chinook_db/"


def run_external_simulation(inserts: int = 10, updates: int = 2, deletes: int = 1):
    """
    Executes the external simulation script with specified parameters.
    """
    console.print(Panel.fit(
        "[bold magenta]TRANSACTION SIMULATOR[/bold magenta]\n"
        "[italic]Generating test PostgreSQL transactions[/italic]",
        border_style="magenta"
    ))
    
    # Display parameters
    params_table = Table(title="Simulation Parameters", show_header=False, box=None)
    params_table.add_column("Operation", style="cyan")
    params_table.add_column("Count", style="yellow", justify="right")
    params_table.add_row("Inserts", str(inserts))
    params_table.add_row("Updates", str(updates))
    params_table.add_row("Deletes", str(deletes))
    console.print(params_table)
    
    # Construct the command to run the external simulator
    command_input = f"{inserts} {updates} {deletes}"
    full_command = f'echo "{command_input}" | uv run src/main.py simulate'
    
    try:
        logger.info(f"Executing external simulator in [cyan]{CHINOOK_DB_PROJECT_PATH}[/cyan]")
        logger.debug(f"Command: {full_command}")
        
        # Execute the command in the specified directory
        process = subprocess.run(
            full_command,
            shell=True,
            check=True,
            cwd=CHINOOK_DB_PROJECT_PATH,
            capture_output=True,
            text=True
        )
        
        logger.info("[bold green]✓[/bold green] External simulation completed successfully!")
        
        # Show simulator output
        if process.stdout:
            console.print(Panel(
                process.stdout.strip(),
                title="[cyan]Simulator Output[/cyan]",
                border_style="cyan"
            ))
        
        if process.stderr:
            logger.warning("Simulator stderr:")
            console.print(Panel(
                process.stderr.strip(),
                title="[yellow]Simulator Warnings[/yellow]",
                border_style="yellow"
            ))
        
        # Display next steps
        console.print()
        next_steps = Table(title="Next Steps", show_header=False, box=None)
        next_steps.add_column("Step", style="cyan")
        next_steps.add_column("Command", style="yellow")
        next_steps.add_row("1. Capture CDC changes", "uv run cdc_load.py")
        next_steps.add_row("2. Via orchestrator", "uv run pipeline_main.py --mode cdc")
        console.print(next_steps)
        
    except subprocess.CalledProcessError as e:
        logger.error("[bold red]✗[/bold red] Error executing external simulation")
        logger.error(f"Command: {e.cmd}")
        logger.error(f"Return Code: {e.returncode}")
        
        if e.stdout:
            logger.debug(f"Stdout: {e.stdout}")
        if e.stderr:
            logger.error(f"Stderr: {e.stderr}")
        
        logger.info("\n[yellow]Troubleshooting tips:[/yellow]")
        logger.info(f"  • Ensure '{CHINOOK_DB_PROJECT_PATH}main.py' exists and is executable")
        logger.info(f"  • Check if 'uv' is installed in '{CHINOOK_DB_PROJECT_PATH}'")
        logger.info(f"  • Verify the external script's dependencies are installed")
        
    except FileNotFoundError:
        logger.error("✗ 'uv' command not found. Ensure uv is installed and in your PATH")
        
    except Exception as e:
        logger.error(f"✗ Unexpected error occurred: {e}", exc_info=True)
    
    console.print(Panel.fit(
        "[bold green]✓ SIMULATION COMPLETED[/bold green]",
        border_style="green"
    ))


if __name__ == "__main__":
    # Parse arguments for inserts, updates, deletes
    args = sys.argv[1:]
    inserts_val = int(args[0]) if len(args) > 0 and args[0].isdigit() else 10
    updates_val = int(args[1]) if len(args) > 1 and args[1].isdigit() else 2
    deletes_val = int(args[2]) if len(args) > 2 and args[2].isdigit() else 1
    
    run_external_simulation(inserts_val, updates_val, deletes_val)

