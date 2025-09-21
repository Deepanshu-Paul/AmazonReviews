import logging
import os

def setup_logger(name, log_file=None, level=logging.INFO, to_console=False):
    """
    Setup a logger that writes to file only by default.
    Set to_console=True to enable console logging.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # File handler if log_file is specified
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # Add console handler only if explicitly enabled
    if to_console:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger
