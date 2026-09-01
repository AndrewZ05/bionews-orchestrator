#!/usr/bin/env python3
"""
Centralized Logging Configuration
==================================

Provides consistent, configurable logging levels across all extractors
and shared modules. Supports three verbosity modes:
- minimal: Only errors, warnings, and key milestones
- normal: Essential info (default)
- verbose: Full detailed logging

Author: Data Pipeline Team
Created: 2025-10-11
"""

import logging
from typing import Optional

# Global log level
_CURRENT_LOG_LEVEL = 'normal'

# Log level mappings
LOG_LEVELS = {
    'minimal': logging.WARNING,  # Only warnings and errors
    'normal': logging.INFO,       # Standard info + warnings + errors
    'verbose': logging.DEBUG      # Everything including debug
}

def set_log_level(level: str) -> None:
    """
    Set the global log level for all extractors and shared modules.

    Args:
        level: One of 'minimal', 'normal', 'verbose'
    """
    global _CURRENT_LOG_LEVEL

    if level not in LOG_LEVELS:
        raise ValueError(f"Invalid log level: {level}. Must be one of: {', '.join(LOG_LEVELS.keys())}")

    _CURRENT_LOG_LEVEL = level
    python_level = LOG_LEVELS[level]

    # Set root logger level
    logging.getLogger().setLevel(python_level)

    # Configure specific loggers for orchestrator modules
    for logger_name in get_orchestrator_loggers():
        logging.getLogger(logger_name).setLevel(python_level)


def get_log_level() -> str:
    # Get the current log level setting.
    return _CURRENT_LOG_LEVEL


def get_orchestrator_loggers() -> list:
    """
    Return list of all orchestrator module logger names.
    These will be configured to respect the global log level.
    """
    return [
        '__main__',
        'plugins.facebook_extractor',
        'plugins.wordpress_extractor',
        'plugins.generic_extractor',
        'shared.schema_evolution',
        'shared.external_tables',
        'shared.bigquery_utils',
        'shared.gcs_storage',
        'shared.gcs_pipeline',
        'shared.extraction_utils',
        'shared.data_processor',
        'shared.data_validator',
        'shared.checkpoint_manager',
        'shared.monitoring',
        'shared.notifications',
        'shared.schema_builder',
        'shared.transform',
        'shared.pipeline_utils',
        'shared.rate_limiter',
    ]


def should_log_detail(logger: logging.Logger, message_type: str = 'progress') -> bool:
    """
    Helper to determine if detailed logging should occur based on current level.

    Args:
        logger: The logger instance
        message_type: Type of message ('progress', 'detail', 'debug')

    Returns:
        True if this type of message should be logged at current level

    Examples:
        # In extractor code:
        if should_log_detail(logger, 'progress'):
            logger.info(f"  Job {i}/{total} created: {job_id}")
        else:
            # Log summary only
            logger.info(f"Created {total} jobs")
    """
    current_level = get_log_level()

    if message_type == 'debug':
        # Only log in verbose mode
        return current_level == 'verbose'
    elif message_type == 'detail':
        # Log in normal and verbose, skip in minimal
        return current_level in ('normal', 'verbose')
    elif message_type == 'progress':
        # Log in normal and verbose, skip in minimal
        return current_level in ('normal', 'verbose')

    # Default to always log (for warnings/errors)
    return True


def create_log_level_filter():
    """
    Create a logging filter function for conditional logging based on message importance.

    Returns a filter function that can be added to logging handlers.

    Usage:
        handler.addFilter(create_log_level_filter())

    The filter checks records for an 'importance' attribute:
        logger.info("Processing item 1/100", extra={'importance': 'detail'})
    """
    def log_filter(record: logging.LogRecord) -> bool:
        # Filter log records based on importance and current log level.
        importance = getattr(record, 'importance', 'normal')
        current_level = get_log_level()

        # Always allow warnings and errors
        if record.levelno >= logging.WARNING:
            return True

        # Filter based on importance and current level
        if importance == 'detail' and current_level == 'minimal':
            return False
        if importance == 'debug' and current_level != 'verbose':
            return False

        return True

    return log_filter


def configure_logging(level: str = 'normal', log_file: Optional[str] = None) -> None:
    """
    Configure logging for the entire orchestrator system.

    Args:
        level: Log level ('minimal', 'normal', 'verbose')
        log_file: Optional path to log file
    """
    # Set global level
    set_log_level(level)

    # Configure formatters
    if level == 'minimal':
        # Simpler format for minimal logs
        fmt = '%(levelname)s - %(message)s'
    else:
        # Detailed format for normal/verbose
        fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    formatter = logging.Formatter(fmt)

    # Configure handlers
    handlers = []

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.addFilter(create_log_level_filter())
    handlers.append(console_handler)

    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(create_log_level_filter())
        handlers.append(file_handler)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    for handler in handlers:
        root_logger.addHandler(handler)

    # Suppress noisy third-party loggers
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('facebook_business').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)


def get_log_level_description(level: str) -> str:
    # Get a description of what a log level includes.
    descriptions = {
        'minimal': 'Errors, warnings, and key milestones only (~50 lines)',
        'normal': 'Essential operations and progress (~100 lines) [DEFAULT]',
        'verbose': 'Full detailed logging including debug info (~300+ lines)'
    }
    return descriptions.get(level, 'Unknown level')
