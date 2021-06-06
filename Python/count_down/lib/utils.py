import sys
import time
from datetime import datetime


def user_collect():
    date_list = []
    request_list = ["year", "month", "date", "hour", "minute"]
    # item_select = request_list[0]
    try:
        for i in range(len(request_list)):
            user_input = input(f"What {request_list[i]} is your event?: ")
            if i == 0 and len(user_input) == 2:
                user_input = "20" + user_input
                int_conversion = int(user_input)
                date_list.append(int_conversion)
            elif i == 1 and user_input.isalpha() and len(user_input) == 3:
                user_input.title()
                user_input = datetime.strptime(user_input, "%b").month
                date_list.append(user_input)
            else:
                int_conversion = int(user_input)
                date_list.append(int_conversion)
            i += 1
    except ValueError:
        print("It seems something is wrong with your input. Let's quickly figure it out")
        time.sleep(2)
        print(f"{user_input} is not in the correct format")
        time.sleep(2)
        print("See acceptable parameters below:")
        print("Year: It can either be 4 digits or 2")
        print("Month: Can either be 2 digits or 3 lettered short form for month eg Mar")
        print("Date: This must be 2 digits date of the month")
        print("Hour: This must be 24 hours format")
        print("Minute: This must be 2 digits")
        sys.exit(-1)

    return date_list


current_date = datetime.now()
year = current_date.year
month = current_date.month
date = current_date.day
hour = current_date.hour


def date_validity_check(date_list):
    time_conversion = datetime(*date_list)
    validity_check = time_conversion > datetime.now()
    if validity_check:
        return f"{time_conversion} is valid"
    return f"{time_conversion} is already in the past and invalid."


def count_down(datetime_obj: datetime):
    pass


def count_down(time_conversion):
    while True:
        difference = datetime(time_conversion) - datetime.now()
        count_hours, rem = divmod(difference.seconds, 3600)
        count_minutes, count_seconds = divmod(rem, 60)
        if difference.days == 0 and count_hours == 0 and count_minutes == 0 and count_seconds == 0:
            print("Good bye!")
            break
        print('The count is: '
              + str(difference.days) + " day(s) "
              + str(count_hours) + " hour(s) "
              + str(count_minutes) + " minute(s) "
              + str(count_seconds) + " second(s) "
              )
        time.sleep(1)



