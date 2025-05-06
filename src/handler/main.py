import logging

from handler.handler import handle_event
from handler.logger import setup_logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main(event, context) -> None:
    setup_logging()
    logger.debug("Starting main function")
    return handle_event(event, context)
