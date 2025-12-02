"""
Pipeline Main - Orchestrator for Databricks Lakeflow Jobs

This is the main entry point for the data pipeline, designed to be called by
Databricks Lakeflow Jobs (formerly Workflows). It routes execution to either
full_load or cdc_load based on the mode parameter.

Usage:
    # Full snapshot load
    uv run pipeline_main.py --mode snapshot
    
    # CDC incremental load
    uv run pipeline_main.py --mode cdc
    
    # Via Lakeflow Jobs (pass as parameter)
    PIPELINE_MODE=snapshot uv run pipeline_main.py
    
Environment Variables:
    PIPELINE_MODE: Either 'snapshot' or 'cdc' (required if --mode not provided)
    LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR) - defaults to INFO
    All credentials are loaded from .dlt/secrets.toml
"""

import sys
import os
import argparse
from rich.panel import Panel
from rich.console import Console

from utils.logger import setup_logger

# Databricks specific workaround:
# Issue: Databricks Serverless pre-loads internal dlt (Delta Live Tables) modules
# which conflict with the dlt library (dlthub).
# Fix: Patch sys.meta_path and sys.modules before importing dlt to remove
# Databricks' internal hooks.
import types

# 1. Drop Databricks' post-import hook
sys.meta_path = [h for h in sys.meta_path if 'PostImportHook' not in repr(h)]

# 2. Purge half-initialized Delta-Live-Tables modules
for name, module in list(sys.modules.items()):
    if not isinstance(module, types.ModuleType):
        continue
    module_file = getattr(module, '__file__', '') or ''
    if module_file.startswith('/databricks/spark/python/dlt'):
        del sys.modules[name]

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
        choices=["snapshot", "cdc"],
        help="Pipeline mode: 'snapshot' for full load or 'cdc' for incremental CDC"
    )
    
    args = parser.parse_args()
    
    # Get mode from argument or environment variable
    mode = args.mode or os.getenv("PIPELINE_MODE")
    
    if not mode:
        logger.error("Pipeline mode must be specified!")
        logger.info("Usage:")
        logger.info("  uv run pipeline_main.py --mode snapshot")
        logger.info("  uv run pipeline_main.py --mode cdc")
        logger.info("")
        logger.info("Or via environment variable:")
        logger.info("  PIPELINE_MODE=snapshot uv run pipeline_main.py")
        logger.info("  PIPELINE_MODE=cdc uv run pipeline_main.py")
        sys.exit(1)
    
    if mode not in ["snapshot", "cdc"]:
        logger.error(f"Invalid mode '{mode}'. Must be 'snapshot' or 'cdc'.")
        sys.exit(1)
    
    # Display pipeline header
    console.print(Panel.fit(
        f"[bold cyan]POSTGRES TO DATABRICKS PIPELINE[/bold cyan]\n"
        f"Mode: [bold yellow]{mode.upper()}[/bold yellow]",
        border_style="cyan"
    ))
    
    # Route to appropriate pipeline module
    if mode == "snapshot":
        logger.info("Routing to [bold green]FULL LOAD[/bold green] pipeline...")
        from full_load import run_full_load
        run_full_load()
    
    elif mode == "cdc":
        logger.info("Routing to [bold blue]CDC LOAD[/bold blue] pipeline...")
        from cdc_load import run_cdc_load
        run_cdc_load()
    
    console.print(Panel.fit(
        "[bold green]✓ PIPELINE EXECUTION COMPLETED[/bold green]",
        border_style="green"
    ))


if __name__ == "__main__":
    main()

