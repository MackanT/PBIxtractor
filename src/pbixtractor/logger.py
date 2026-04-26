"""Centralized logging configuration for PBI-Ixtractor."""

import logging
import sys
from io import StringIO
from typing import Optional


class LogCapture:
    """Captures log messages in a string buffer for later retrieval."""

    def __init__(self):
        self.buffer = StringIO()
        self.handler = logging.StreamHandler(self.buffer)
        self.handler.setLevel(logging.DEBUG)

        # Format: severity level + message
        formatter = logging.Formatter("%(levelname)s: %(message)s")
        self.handler.setFormatter(formatter)

    def get_logs(self) -> str:
        """Get captured log messages."""
        return self.buffer.getvalue()

    def clear(self):
        """Clear captured log messages."""
        self.buffer.truncate(0)
        self.buffer.seek(0)


def setup_logger(
    name: str = "pbixtractor", level: int = logging.INFO, capture: bool = False
) -> tuple[logging.Logger, Optional[LogCapture]]:
    """
    Configure and return a logger instance.

    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        capture: If True, capture logs in a buffer for later retrieval

    Returns:
        Tuple of (logger, log_capture). log_capture is None if capture=False
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Console handler for terminal output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # Format: severity + message (with line breaks for long messages)
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    # Optional capture handler
    log_capture = None
    if capture:
        log_capture = LogCapture()
        logger.addHandler(log_capture.handler)

    return logger, log_capture


def get_logger(name: str = "pbixtractor") -> logging.Logger:
    """
    Get an existing logger or create a new one.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)

    # If logger has no handlers, set it up with defaults
    if not logger.handlers:
        logger, _ = setup_logger(name)

    return logger


def wrap_long_message(message: str, max_length: int = 122) -> str:
    """
    Wrap long messages to specified line length.

    Args:
        message: Message to wrap
        max_length: Maximum line length

    Returns:
        Wrapped message
    """
    if len(message) <= max_length:
        return message

    lines = []
    for line in message.split("\n"):
        if len(line) <= max_length:
            lines.append(line)
        else:
            # Split long lines
            while line:
                lines.append(line[:max_length])
                line = line[max_length:]

    return "\n".join(lines)
