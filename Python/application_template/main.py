import sys
import time
from lib import binary_logger, utils

if __name__ == "__main__":
    logger = binary_logger.logger

    logger.info("**********")

    logger.error("Alphabet not allowed")

    logger.info("Success!!!")