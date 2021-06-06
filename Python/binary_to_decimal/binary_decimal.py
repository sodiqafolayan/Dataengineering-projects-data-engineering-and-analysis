import sys
import time
from lib import binary_logger, utils

if __name__ == "__main__":
    logger = binary_logger.logger

    binary = input("Enter a binary number: ")
    logger.info("A new input")
    time.sleep(1)
    print("We are now processing your input.....")
    time.sleep(2)
    for _ in range(6):
        print(".", end="")
        time.sleep(1)
    print()
    # This is giving error
    for item in binary:
        if item.isalpha():
            print(f"Something is wrong with your input: {binary}")
            time.sleep(2)
            print("We found alphabet(s) in your input")
            time.sleep(1)
            print("Try again some next time")
            logger.error("Alphabet not allowed")
            sys.exit()

        elif int(item) > 1 or len(binary) > 8:
            print(f"Something is wrong with your input: {binary}")
            time.sleep(2)
            print("Your input is either not in binary or or you enter more than eight (8) number long")
            time.sleep(1)
            print("Try again some next time")
            logger.error("Wrong input")
            sys.exit()
    time.sleep(2)
    print("Here is your conversion")
    converted = utils.binary_to_decimal(binary)
    time.sleep(1)
    print(f"{binary} converted to binary: {converted}")
    logger.info("Success!!!")
