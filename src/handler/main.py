import logging
import sys

from handler.handler import handle_event

logger = logging.getLogger(__name__)


def main(event, context) -> None:
    logger.info("sys.path =", sys.path)
    return handle_event(event, context)
