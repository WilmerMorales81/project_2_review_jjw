import logging
from pathlib import Path

Path("logs").mkdir(exist_ok=True)


def get_logger(name: str = "basic_logger") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler("logs/basic.log", mode="a")
        file_handler.setLevel(logging.ERROR)

        console_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_format = logging.Formatter(
            "{'time':'%(asctime)s', 'name': '%(name)s', "
            "'level': '%(levelname)s', 'message': '%(message)s'}"
        )

        console_handler.setFormatter(console_format)
        file_handler.setFormatter(file_format)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
