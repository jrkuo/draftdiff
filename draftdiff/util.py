import datetime


def get_current_ds():
    return datetime.datetime.now().strftime("%Y-%m-%d")


def databricks_print_test():
    print("draftdiff imported")


if __name__ == "__main__":
    get_current_ds()
