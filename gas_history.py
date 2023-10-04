from datetime import date, timedelta
import json, lzma, logging, os, random, requests, sqlite3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from models import APIGasStation

API_BASE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestresHist/"
FILES_PATH = Path("responses")
DB_PATH = Path("db.sqlite3")

logging.basicConfig(level=logging.INFO)


def main(idmun: int) -> None:
    end_date = END_DATE
    start_date = end_date - timedelta(days=DAY_COUNT)
    response_folder = FILES_PATH / str(idmun) if idmun else FILES_PATH

    logging.debug(f"Fetching prices from {end_date} backwards {DAY_COUNT} days.")

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        executors = {}
        for current_date in daterange(start_date, end_date):
            if not STORE and os.path.exists(
                response_folder / f"{current_date}.json.xz"
            ):
                logging.debug(f"Skipping {current_date}")
                continue

            logging.debug(f"Fetching data from {current_date}")
            executors[executor.submit(populate_db, current_date, idmun)] = current_date


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

    response_folder = FILES_PATH / str(idmun) if idmun else FILES_PATH
    response_path = response_folder / f"{single_date}.json.xz"

    if not os.path.isdir(response_folder):
        os.mkdir(response_folder)

    if not os.path.exists(response_path):
        date_str = single_date.strftime("%d-%m-%Y")
        if idmun:
            response = requests.get(
                f"{API_BASE_URL}/FiltroMunicipio/{date_str}/{idmun}"
            )
        else:
            response = requests.get(f"{API_BASE_URL}/{date_str}")

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

    stations = [
        station.as_sql_station()
        for station in parsed_data
        if station.ideess not in existing_ids
    ]

    attempts = 1
    while attempts:
        try:
            cursor.executemany(
                "INSERT INTO stations VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                stations,
            )
            conn.commit()
            break
        except sqlite3.OperationalError:
            delay = 2**attempts * random.random()  # "Exponential backoff"
            logging.warning(
                f"Database locked (#{attempts}), retrying in {delay} seconds..."
            )
            sleep(delay)
            attempts += 1


def save_prices(conn: sqlite3.Connection, data: list, single_date: date) -> None:
    """Add the prices to the database"""

    cursor = conn.cursor()

    stations = [station.as_sql_prices(single_date) for station in data]

    attempts = 1
    while attempts:
        try:
            cursor.executemany(
                "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                stations,
            )
            conn.commit()
            break
        except sqlite3.OperationalError:
            delay = 2**attempts * random.random()  # "Exponential backoff"
            logging.warning(f"Database locked (#{attempts}), retrying in {delay:.3f}s")
            sleep(delay)
            attempts += 1

    logging.info(f"Prices for {single_date} added to the database")


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
    """Returns a generator for dates between both arguments, including both"""

    for n in range(int((end_date - start_date).days + 1)):
        yield start_date + timedelta(days=n)


def connect_db() -> sqlite3.Connection:
    """Connect to the database"""
    conn = sqlite3.connect(DB_PATH)
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
        "-t",
        "--threads",
        type=int,
        default=10,
        help="Number of threads to use for fetching data",
    )
    parser.add_argument(
        "-s",
        "--store",
        action="store_true",
        help="Store the data in the DB. If not specified, data will only be downloaded",
    )
    parser.add_argument(
        "-p",
        "--db-path",
        type=Path,
        default=DB_PATH,
        help="The path to the database file where to store the prices",
    )
    args = parser.parse_args()

    DAY_COUNT = args.days
    END_DATE = args.end_date
    THREADS = args.threads
    STORE = args.store
    DB_PATH = Path(args.db_path)

    main(args.locality)
