import logging
import sys
from logging import Logger


def set_logger(name: str = 'edm', severity: str = 'info'):
    logger = logging.getLogger(name)

    if (not logger.hasHandlers()):
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            '%H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    set_severity(logger, severity)


def set_severity(logger: Logger, severity: str):

    if severity.lower() == 'info':
        logger.setLevel(logging.INFO)

    if severity.lower() == 'debug':
        logger.setLevel(logging.DEBUG)

    if severity.lower() == 'warning':
        logger.setLevel(logging.WARNING)

    if severity.lower() == 'error':
        logger.setLevel(logging.ERROR)
