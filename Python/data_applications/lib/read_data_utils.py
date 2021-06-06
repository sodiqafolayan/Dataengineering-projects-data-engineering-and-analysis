import sys
import time
import os
import pandas as pd

from lib import logger, utils

logger = logger.logger


def file_format_reader(file=input("Enter a link to your file: ")):
    print("We are now reading your file format")
    time.sleep(2)
    if not file or "." not in file:
        logger.error(f"{file} is in wrong format")
        print("Seems there is something wrong with the provided file path or name.")
        time.sleep(2)
        print("Let's figure it out to avoid future mistake")
        time.sleep(2)
        print("From our analysis, file path or name is in the wrong format")
        time.sleep(2)
        print("Usage: File path or name must must exist and end with '.' and 'format")
        time.sleep(1)
        print("You can try again next time")
        sys.exit(-1)
    else:
        file_size = os.path.getsize(file)
        if file_size > 5e+6:
            logger.error(f"{file} is {round((file_size/1000000), 2)} and bigger than 5MB")
            print("Your file is bigger than 5MB!!!")
            sys.exit(-1)
        else:
            print(f"Your file size is {round((file_size/1000000), 2)} and it is within our required limit")
            time.sleep(2)
            file_format = file.split(".")[-1]
            print(f"Your file format is: {file_format.upper()}")
    return file


def file_reader(file):
    if file.split(".")[-1] == "csv":
        logger.info(f"File format is csv")
        file_df = pd.read_csv(file)
    elif file.split(".")[-1] == "json":
        logger.info(f"File format is json")
        file_df = pd.read_json(file)
    elif file.split(".")[-1] == "xlsx":
        logger.info(f"File format is excel")
        file_df = pd.read_excel(file, sheet_name=input("Enter the sheet name: "))

    else:
        print("Unsupported format")
        logger.error(f"{file} is not currently supported")
        sys.exit(-1)
    data_columns = [item for item in file_df.columns]
    print(data_columns)
    file_df = file_df.fillna(0)
    total_amount = input("Enter your total amount columns as it appears in the columns above: ")
    logger.info(f"{total_amount} is the column to work with")
    file_df[total_amount] = file_df[total_amount].replace("$", "")
    total_spent = round(file_df[total_amount].astype(float).sum(), 2)
    mean_spend = round(file_df[total_amount].astype(float).mean(), 2)
    maximum_spend = round(file_df[total_amount].astype(float).max(), 2)
    minimum_spend = round(file_df[total_amount].astype(float).min(), 2)
    portfolio = {"Total": total_spent, "Average": mean_spend, "Maximum": maximum_spend, "Minimum": minimum_spend}
    return portfolio
