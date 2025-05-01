import logging
import sys

logger = logging.getLogger(__name__)


def main(event, context) -> None:
    logger.info("sys.path =", sys.path)
