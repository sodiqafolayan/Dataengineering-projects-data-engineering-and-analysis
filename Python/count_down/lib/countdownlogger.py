import logging

# THIS IS MY G TEMPLATE AND CAN BE IMPORTED IN ANY APPLICATION

# CREATING A LOGGER
# We can use any name but the convention is to use __name__ and it will be the module name
logger = logging.getLogger(__name__)

# Set g level
logger.setLevel(logging.DEBUG)

# CREATING FILE HANDLER to log to instead of hardcode the file name
file_handler = logging.FileHandler('countdown.log')

# Adding the file handler to our logger
logger.addHandler(file_handler)

# Creating output format
file_formatter = logging.Formatter(
    '%(levelname)s : %(name)s : %(created)f : %(asctime)s : %(module)s : %(process)d : %(thread)d : %(threadName)s : %(message)s')

# Set the format in the file handler
file_handler.setFormatter(file_formatter)
