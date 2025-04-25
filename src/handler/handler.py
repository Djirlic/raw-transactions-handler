import logging

logger = logging.getLogger(__name__)


def handle_event(event, context) -> None:
    logger.info("Starting event handling")
