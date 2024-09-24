"""
python3 generate.py <dir to put files> <%Y-%m-%d date> <days_count> <unique emails count> <events count>
Ex.
python3 generate.py input 2024-09-10 30 10 2000
"""

import datetime
import os
import random
import string
import sys


EMAIL_PROVIDERS = [
    "gmail.com",
    "ya.ru",
    "mail.ru",
]

ACTION_TYPES = [
    "CREATE",
    "READ",
    "UPDATE",
    "DELETE",
]


def random_char(char_num):
    return "".join(random.choice(string.ascii_letters) for _ in range(char_num))


def generate_email():
    return f"{random_char(random.randrange(5, 15))}@{random.choice(EMAIL_PROVIDERS)}"


if __name__ == "__main__":

    dirname = sys.argv[1]
    dt = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d")
    days_cnt = int(sys.argv[3])
    emails_cnt = int(sys.argv[4])
    emails = [generate_email() for _ in range(emails_cnt)]
    events_cnt = int(sys.argv[5])

    for i in range(days_cnt):
        current_dt = dt + datetime.timedelta(days=i)
        filepath = os.path.join(dirname, f"{current_dt.strftime('%Y-%m-%d')}.csv")
        with open(filepath, "w") as out:
            out.write(
                "\n".join(
                    f"{random.choice(emails)},{random.choice(ACTION_TYPES)},{current_dt + datetime.timedelta(seconds=random.randrange(0, 60*60*24))}"
                    for _ in range(events_cnt)
                )
            )
