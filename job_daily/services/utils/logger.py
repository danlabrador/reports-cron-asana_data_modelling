import logging
import os

import colorlog


def setup_logger(
    name: str = "cron_job", log_file: str = None, level: int = logging.DEBUG
) -> logging.Logger:
    """
    Set up and return a logger with console and optional file handlers.

    Args:
        name (str): Name of the logger.
        log_file (str): Path to the log file. If None, only console logging is used.
        level (int): Logging level (e.g., logging.DEBUG, logging.INFO).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent adding multiple handlers to the logger if it's already configured.
    if logger.handlers:
        return logger

    # Formatter for log messages.
    formatter = logging.Formatter(
        "[%(asctime)s] %(funcName)s - %(levelname)s - %(message)s",
        datefmt="%y-%m-%d %H:%M:%S",
    )

    # Console handler.
    ch = logging.StreamHandler()
    ch.setLevel(level)
    try:
        formatter = colorlog.ColoredFormatter(
            "[%(asctime)s] %(filename)s/%(funcName)s - %(log_color)s%(levelname)s%(reset)s - %(message)s",
            datefmt="%y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
    except ImportError:
        formatter = logging.Formatter(
            "[%(asctime)s] %(filename)s/%(funcName)s - %(levelname)s - %(message)s",
            datefmt="%y-%m-%d %H:%M:%S",
        )
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler (if a log file is specified).
    if log_file:
        # Ensure the directory for the log file exists.
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        try:
            formatter = colorlog.ColoredFormatter(
                "[%(asctime)s] %(filename)s/%(funcName)s - %(log_color)s%(levelname)s%(reset)s - %(message)s",
                datefmt="%y-%m-%d %H:%M:%S",
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
            )
        except ImportError:
            formatter = logging.Formatter(
                "[%(asctime)s] %(filename)s/%(funcName)s - %(levelname)s - %(message)s",
                datefmt="%y-%m-%d %H:%M:%S",
            )
        fh = logging.FileHandler(log_file)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


# Define a default log file location relative to the project root.
# Adjust the path if needed based on your project structure.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_LOG_FILE = os.path.join(PROJECT_ROOT, "logs", "cron_job.log")

# Create a default logger instance for the project.
app_logger = setup_logger(log_file=DEFAULT_LOG_FILE, level=logging.DEBUG)

# cspell: ignore levelname
