import logging

from handler.handler import handle_event

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main(event, context) -> None:
    logger.info("Starting main function")
    return handle_event(event, context)
