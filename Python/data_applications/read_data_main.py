from lib import newutils

if __name__ == "__main__":
    user_file_format = newutils.file_format_reader()
    print(user_file_format)

    print(newutils.file_reader(user_file_format))

