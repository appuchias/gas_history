from datetime import date, timedelta
import json, lzma, os, requests, sqlite3
from math import ceil
from threading import Thread as T
from time import sleep

from extras import daterange
from models import APIGasStation

BASE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestresHist/"
FILES_PATH = "responses/"
DAY_COUNT = 1200
END_DATE = date.today()
STORE = True

THREADS = 30
THREADS_THROTTLE = 1 / 3 * THREADS


def main(idmun: int) -> None:
    end_date = END_DATE
    start_date = end_date - timedelta(days=DAY_COUNT)

    print(f"Fetching prices from {end_date} backwards {DAY_COUNT} days.")

    threads = list()
    for current_date in daterange(start_date, end_date):
        if os.path.exists(FILES_PATH + f"_{current_date}.json.xz"):
            print(f"Skipping {current_date}")
            continue

        # Rate limit
        thread_count = len(threads)
        if thread_count >= THREADS:
            threads.pop(0).join()
        elif thread_count >= THREADS_THROTTLE:
            sleep(thread_count / THREADS)

        print(f"Fetching data from {current_date}")
        threads.append(T(target=populate_db, args=(current_date, idmun)))
        threads[-1].start()
        sleep(0.1)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()


def populate_db(single_date, idmun: int):
    conn = connect_db()
    data = fetch_data(single_date, idmun)

    if STORE:
        parsed_data = parse_json(data["ListaEESSPrecio"])

        save_stations(conn, parsed_data)
        save_prices(conn, parsed_data, single_date)


def fetch_data(single_date: date, idmun: int = 0) -> dict:
    """
    Get the data from the API and store it locally.
    Provide `idmun` to filter by locality
    """

    response_folder = f"{FILES_PATH}{idmun}{'/' if idmun else ''}"
    response_path = f"{response_folder}{single_date}.json.xz"

    if not os.path.isdir(response_folder):
        os.mkdir(response_folder)

    if not os.path.exists(response_path):
        date_str = single_date.strftime("%d-%m-%Y")
        if idmun:
            response = requests.get(f"{BASE_URL}/FiltroMunicipio/{date_str}/{idmun}")
        else:
            response = requests.get(f"{BASE_URL}/{date_str}")

        with lzma.open(response_path, "w") as f:
            f.write(response.text.encode("utf-8"))

        return response.json()

    with lzma.open(response_path) as f:
        return json.load(f)


def get_stations(conn: sqlite3.Connection) -> list:
    """Get the stations from the database"""

    # Get the stations from the database
    cursor = conn.cursor()
    cursor.execute("SELECT ideess, company, cp, address FROM stations")
    stations = cursor.fetchall()

    return stations


def save_stations(conn: sqlite3.Connection, parsed_data: list[APIGasStation]) -> None:
    """Add the stations to the database"""

    cursor = conn.cursor()

    ids = [station.ideess for station in parsed_data]
    cursor.execute(
        f"SELECT ideess FROM stations WHERE ideess IN ({','.join('?' * len(ids))})",
        ids,
    )
    existing_ids = [i[0] for i in cursor.fetchall()]

    for station in parsed_data:
        if int(station.ideess) in existing_ids:
            continue

        try:
            cursor.execute(
                "INSERT INTO stations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                station.as_sql_station(),
            )
        except sqlite3.IntegrityError:
            print(f"Station {station} was already in the database")

    conn.commit()


def save_prices(conn: sqlite3.Connection, data: list, single_date: date) -> None:
    """Add the prices to the database"""

    cursor = conn.cursor()

    for station in data:
        try:
            cursor.execute(
                "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?)",
                station.as_sql_prices(single_date),
            )
        except sqlite3.IntegrityError:
            print(f"Price already in DB [{single_date} {str(station)}]")
        except sqlite3.OperationalError:  # DB locked
            print("DB locked. Waiting 1s...")
            sleep(1)
            cursor.execute(
                "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?)",
                station.as_sql_prices(single_date),
            )

    conn.commit()

    print(f"Prices for {single_date} added to the database")


def parse_json(data: list) -> list[APIGasStation]:
    """Parse the data from the API"""

    str_to_price = (
        lambda price: float(price.replace(",", ".")) if len(price) > 0 else 0.0
    )

    parsed_data = []
    for station in data:
        parsed_data.append(
            APIGasStation(
                ideess=station["IDEESS"],
                rotulo=station["Rótulo"],
                c_p_=station["C.P."],
                direccion=station["Dirección"],
                latitud=station["Latitud"],
                longitud=station["Longitud (WGS84)"],
                municipio=station["Municipio"],
                provincia=station["Provincia"],
                precio_gasoleo_a=str_to_price(station["Precio Gasoleo A"]),
                precio_gasoleo_b=str_to_price(station["Precio Gasoleo B"]),
                precio_gasolina_95=str_to_price(station["Precio Gasolina 95 E5"]),
                precio_gasolina_98=str_to_price(station["Precio Gasolina 98 E5"]),
                precio_glp=str_to_price(station["Precio Gases licuados del petróleo"]),
            )
        )

    return parsed_data


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(days=n)


def connect_db() -> sqlite3.Connection:
    """Connect to the database"""
    conn = sqlite3.connect("db.sqlite3")
    return conn


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--days",
        type=int,
        default=15,
        help="'block size' for querying the API.",
    )
    parser.add_argument(
        "-e",
        "--end-date",
        type=date.fromisoformat,
        default=date.today(),
        help="Most recent day to fetch (YYYY-MM-DD)",
    )
    parser.add_argument(
        "-l",
        "--locality",
        type=int,
        default=0,
        help="Locality ID. See municipios.json for more info",
    )
    parser.add_argument(
        "-s",
        "--store",
        action="store_true",
        help="Store the data in the DB. If not specified, data will only be downloaded",
    )
    args = parser.parse_args()

    DAY_COUNT = args.days
    END_DATE = args.end_date
    STORE = args.store

    main(args.locality)
