"""
Pipeline Main - Orchestrator for Databricks Lakeflow Jobs

This is the main entry point for the data pipeline, designed to be called by
Databricks Lakeflow Jobs (formerly Workflows). It routes execution to either
full_load or cdc_load based on the mode parameter.

Usage:
    # Full snapshot load
    uv run pipeline_main.py --mode full_load --catalog chinook --dataset bronze
    
    # CDC incremental load
    uv run pipeline_main.py --mode cdc --catalog chinook --dataset bronze
    
    # Via Lakeflow Jobs (pass as parameter)
    PIPELINE_MODE=full_load uv run pipeline_main.py
    
Environment Variables:
    PIPELINE_MODE: Either 'full_load' or 'cdc' (required if --mode not provided)
    LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR) - defaults to INFO
    All credentials are loaded from .dlt/secrets.toml
"""

import sys
import os
import argparse
import types
from rich.panel import Panel
from rich.console import Console

# Databricks specific workaround:
# Issue: Databricks Serverless pre-loads internal dlt (Delta Live Tables) modules
# which conflict with the dlt library (dlthub).
# Fix: Patch sys.meta_path and sys.modules before importing dlt to remove
# Databricks' internal hooks.

# 1. Drop Databricks' post-import hook
sys.meta_path = [h for h in sys.meta_path if 'PostImportHook' not in repr(h)]

# 2. Purge half-initialized Delta-Live-Tables modules
for name, module in list(sys.modules.items()):
    if not isinstance(module, types.ModuleType):
        continue
    module_file = getattr(module, '__file__', '') or ''
    if module_file.startswith('/databricks/spark/python/dlt'):
        del sys.modules[name]

# Package Imports
from .utils.logger import setup_logger
from .full_load import run_full_load
from .cdc_load import run_cdc_load

logger = setup_logger(__name__)
console = Console()


def main():
    """
    Main orchestrator function.
    
    Routes execution to appropriate pipeline module based on mode parameter.
    Enforces exclusive execution - only runs ONE mode per invocation.
    """
    parser = argparse.ArgumentParser(
        description="PostgreSQL to Databricks Pipeline Orchestrator"
    )
    parser.add_argument(
        "--mode",
        choices=["full_load", "cdc"],
        help="Pipeline mode: 'full_load' for initial load or 'cdc' for incremental CDC"
    )
    parser.add_argument(
        "--catalog",
        help="Target Unity Catalog name (overrides TARGET_CATALOG env var)"
    )
    parser.add_argument(
        "--dataset",
        help="Target dataset/schema name (overrides TARGET_DATASET env var)"
    )
    
    args = parser.parse_args()
    
    # Get mode from argument or environment variable
    mode = args.mode or os.getenv("PIPELINE_MODE")
    
    # Handle catalog and dataset overrides
    if args.catalog:
        os.environ["TARGET_CATALOG"] = args.catalog
        logger.info(f"Target Catalog set via CLI: [cyan]{args.catalog}[/cyan]")
        
    if args.dataset:
        os.environ["TARGET_DATASET"] = args.dataset
        logger.info(f"Target Dataset set via CLI: [cyan]{args.dataset}[/cyan]")
    
    if not mode:
        logger.error("Pipeline mode must be specified!")
        logger.info("Usage:")
        logger.info("  uv run pipeline_main.py --mode full_load")
        logger.info("  uv run pipeline_main.py --mode cdc")
        logger.info("")
        logger.info("Or via environment variable:")
        logger.info("  PIPELINE_MODE=full_load uv run pipeline_main.py")
        logger.info("  PIPELINE_MODE=cdc uv run pipeline_main.py")
        sys.exit(1)
    
    if mode not in ["full_load", "cdc"]:
        logger.error(f"Invalid mode '{mode}'. Must be 'full_load' or 'cdc'.")
        sys.exit(1)
    
    # Display pipeline header
    console.print(Panel.fit(
        f"[bold cyan]POSTGRES TO DATABRICKS PIPELINE[/bold cyan]\n"
        f"Mode: [bold yellow]{mode.upper()}[/bold yellow]",
        border_style="cyan"
    ))
    
    # Route to appropriate pipeline module
    if mode == "full_load":
        logger.info("Routing to [bold green]FULL LOAD[/bold green] pipeline...")
        run_full_load()
    
    elif mode == "cdc":
        logger.info("Routing to [bold blue]CDC LOAD[/bold blue] pipeline...")
        run_cdc_load()
    
    console.print(Panel.fit(
        "[bold green]✓ PIPELINE EXECUTION COMPLETED[/bold green]",
        border_style="green"
    ))


if __name__ == "__main__":
    main()