import logging
import os
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main(event, context):
    logger.info("===== DEBUG START =====")
    logger.info(f"sys.path = {sys.path}")
    logger.info(f"/opt/python contents = {os.listdir('/opt/python')}")
    logger.info("===== DEBUG END =====")
    return {"status": "debug printed"}
