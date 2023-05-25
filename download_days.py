from datetime import date, timedelta
import requests, lzma, os
from threading import Thread as T
from time import sleep

# Adjust this as needed for your connection
# 0 = no throttling
# 1 = .1s delay between requests (+.2s when throttled)
# 2 = .2s delay between requests (+.4s when throttled)
# ...
SLOW_DOWN = 1

BASE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestresHist/"
FILE_PATH_BASE = "responses/response"
GET_QUERY = "SELECT ideess, company, cp, address FROM stations"

DAY_COUNT = 20
END_DATE = date.today()
THREADS = 30
THREADS_THROTTLE = 1 / 3 * THREADS

# ANSI escape code to clear the current line
CLR = "\x1B[0K"


def main(end_date=END_DATE, delta=timedelta(days=DAY_COUNT), t_count=THREADS) -> None:
    print(f"Fetching {DAY_COUNT} days. It can take a while...\n- - -\n")

    threads = list()
    for current_date in daterange(end_date - delta, end_date):
        if os.path.exists(FILE_PATH_BASE + f"_{current_date}.json.xz"):
            print(f"Skipping {current_date}{CLR}", end="\r")
            continue

        # Rate limit
        if len(threads) >= t_count:
            threads.pop(0).join()
        elif len(threads) >= 10:
            sleep(0.2)

        print(f"Fetching data from {current_date}{CLR}", end="\r")
        threads.append(T(target=fetch_data, args=(current_date,)))
        threads[-1].start()
        sleep(0.1)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print(f"- - -{CLR}\nDone!")


def fetch_data(single_date: date) -> dict:
    """Get the data from the API and store it locally"""

    date_str = single_date.strftime("%d-%m-%Y")
    response = requests.get(f"{BASE_URL}/{date_str}")

    path = FILE_PATH_BASE + f"_{single_date}.json.xz"

    with lzma.open(path, "w") as f:
        f.write(response.text.encode("utf-8"))

    return response.json()


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(days=n)


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Store data from the API locally.")
    parser.add_argument(
        "-d",
        "--days",
        type=int,
        default=DAY_COUNT,
        help="Amount of days to download",
    )
    parser.add_argument(
        "-e",
        "--end-date",
        type=date.fromisoformat,
        default=END_DATE,
        help="Most recent day to fetch (YYYY-MM-DD)",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=THREADS,
        help="Number of threads to use",
    )
    args = parser.parse_args()

    DAY_COUNT = args.days
    END_DATE = args.end_date
    THREADS = args.threads

    main(END_DATE, timedelta(days=DAY_COUNT), THREADS)
