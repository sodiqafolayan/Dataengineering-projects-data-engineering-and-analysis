import sys
import time
from lib import countdownlogger, utils

if __name__ == "__main__":
    logger = countdownlogger.logger
    co = utils.count_down(utils.date_validity_check(utils.user_collect()))
    # user = utils.user_collect()
    # print(user)
    print(co)
    logger.info("**********")

    logger.error("Alphabet not allowed")

    logger.info("Success!!!")
