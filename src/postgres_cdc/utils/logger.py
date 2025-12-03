"""
Centralized Logging Configuration

Provides consistent logging setup across all pipeline scripts with rich console formatting.
Supports configurable log levels via LOG_LEVEL environment variable.

Usage:
    from utils.logger import setup_logger
    
    logger = setup_logger(__name__)
    logger.info("Processing started")
    logger.warning("High memory usage detected")
"""

import logging
import os
from rich.logging import RichHandler
from rich.console import Console


def setup_logger(name: str, level: str = None) -> logging.Logger:
    """
    Configure and return a logger with rich console formatting.
    
    Args:
        name: Logger name (typically __name__ from calling module)
        level: Optional log level override. If not provided, uses LOG_LEVEL env var or defaults to INFO
    
    Returns:
        Configured logger instance with RichHandler
    """
    # Determine log level
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Map string level to logging constant
    numeric_level = getattr(logging, level, logging.INFO)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)
    
    # Avoid duplicate handlers if logger already configured
    if logger.handlers:
        return logger
    
    # Configure rich console handler
    console = Console(stderr=True)
    rich_handler = RichHandler(
        console=console,
        show_time=True,
        show_path=False,
        markup=True,
        rich_tracebacks=True,
        tracebacks_show_locals=True,
    )
    
    # Set format for log messages
    formatter = logging.Formatter(
        fmt="%(message)s",
        datefmt="[%Y-%m-%d %H:%M:%S]"
    )
    rich_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(rich_handler)
    
    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger instance.
    
    Alias for setup_logger() for convenience.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return setup_logger(name)
