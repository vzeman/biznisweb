#!/usr/bin/env python3
"""
Centralized logging configuration for all BizniWeb scripts
"""

import os
import logging
from datetime import datetime
from pathlib import Path


def setup_logger(name: str = None, level: str = None) -> logging.Logger:
    """
    Setup and return a configured logger instance

    Args:
        name: Logger name (defaults to 'biznisweb')
        level: Logging level (defaults to INFO, or DEBUG if DEBUG env var is set)

    Returns:
        Configured logger instance
    """
    # Get logger name
    logger_name = name or 'biznisweb'

    # Get or create logger
    logger = logging.getLogger(logger_name)

    # Only configure if not already configured
    if not logger.handlers:
        # Determine log level
        if level:
            log_level = getattr(logging, level.upper(), logging.INFO)
        elif os.getenv('DEBUG'):
            log_level = logging.DEBUG
        else:
            log_level = logging.INFO

        logger.setLevel(log_level)

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)

        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)

        # Add handler to logger
        logger.addHandler(console_handler)

        # Prevent propagation to root logger
        logger.propagate = False

    return logger


def get_logger(name: str = None) -> logging.Logger:
    """
    Get or create a logger instance with the standard configuration

    Args:
        name: Logger name (defaults to 'biznisweb')

    Returns:
        Configured logger instance
    """
    return setup_logger(name)


# Create default logger instance
logger = get_logger()
