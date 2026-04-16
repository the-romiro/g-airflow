import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


def log_message(message: str):
    logging.info(message)
