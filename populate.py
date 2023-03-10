from datetime import date, timedelta
import requests, sqlite3
from threading import Thread as T
from time import sleep

from models import Station

BASE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestresHist/"
FILE_PATH = "response.json"
GET_QUERY = "SELECT ideess, company, cp, address FROM stations"
DAY_DELTA = 20
ITERATIONS = 60
SLEEP_TIME = 2
END_DATE = date.today()


def main(idmun: int) -> None:
    delta = timedelta(days=DAY_DELTA)
    end_date = END_DATE
    start_date = end_date - delta

    print(
        f"Fetching prices from {end_date} backwards {ITERATIONS} times in {DAY_DELTA} day intervals."
    )
    print(f"Fetching {DAY_DELTA * ITERATIONS} days.\nIt can take a while...\n- - -\n")

    # Will fetch data from end_date backwards ITERATIONS times in DAY_DELTA day intervals
    for iteration in range(ITERATIONS):
        threads = list()

        print(f"Iteration {iteration + 1:02}/{ITERATIONS}")

        for single_date in daterange(start_date, end_date):
            print(f"Fetching data from {single_date}")
            threads.append(T(target=populate_db, args=(single_date, idmun)))
            threads[-1].start()

        for thread in threads:
            thread.join()

        start_date -= delta
        end_date -= delta

        del threads

        print(f"Sleeping for {SLEEP_TIME} seconds...\n- - -\n")
        sleep(SLEEP_TIME)


def populate_db(single_date, idmun: int):
    conn = connect_db()
    fetch_data(single_date, idmun)
    data = get_data(conn, single_date, idmun)
    add_prices(conn, data, single_date)


def connect_db() -> sqlite3.Connection:
    """Connect to the database"""
    conn = sqlite3.connect("db.sqlite3")
    return conn


def get_data(conn: sqlite3.Connection, single_date: date, idmun: int) -> list[Station]:
    """Get the data from the local file"""
    # try:
    #     with open(FILE_PATH, "r") as f:
    #         data = json.load(f)
    # except FileNotFoundError:
    #     fetch_data(date)
    #     with open(FILE_PATH, "r") as f:
    #         data = json.load(f)
    # data = data["ListaEESSPrecio"]

    data = fetch_data(single_date, idmun)["ListaEESSPrecio"]
    parsed_data = parse_json(data)

    cursor = conn.cursor()

    if not len(cursor.execute(GET_QUERY).fetchall()):
        for station in parsed_data:
            try:
                cursor.execute(
                    "INSERT INTO stations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    station.as_sql_station(),
                )
            except sqlite3.IntegrityError:
                print(f"Station {station} was already in the database")

        conn.commit()

    return parsed_data


def fetch_data(single_date: date, idmun: int) -> dict:
    """Get the data from the API and store it locally"""

    date_str = single_date.strftime("%d-%m-%Y")
    response = requests.get(f"{BASE_URL}/FiltroMunicipio/{date_str}/{idmun}")

    # with open(FILE_PATH, "w") as f:
    #     json.dump(response.json(), f)

    return response.json()


def get_stations(conn: sqlite3.Connection) -> list:
    """Get the stations from the database"""

    # Get the stations from the database
    cursor = conn.cursor()
    cursor.execute(GET_QUERY)
    stations = cursor.fetchall()

    return stations


def add_prices(conn: sqlite3.Connection, data: list, single_date: date) -> None:
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

    conn.commit()

    print(f"Prices for {single_date} added to the database")


def parse_json(data: list) -> list[Station]:
    """Parse the data from the API"""

    str_to_price = (
        lambda price: float(price.replace(",", ".")) if len(price) > 0 else 0.0
    )

    parsed_data = []
    for station in data:
        parsed_data.append(
            Station(
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


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--days",
        type=int,
        default=15,
        help="Number of days to fetch data from",
    )
    parser.add_argument(
        "-i",
        "--iterations",
        type=int,
        default=10,
        help="Number of iterations to fetch data",
    )
    parser.add_argument(
        "-s",
        "--sleep",
        type=int,
        default=5,
        help="Number of seconds to sleep between iterations",
    )
    parser.add_argument(
        "-e" "--end-date",
        type=date.fromisoformat,
        default=date.today(),
        help="Date to start fetching data from (YYYY-MM-DD)",
    )
    parser.add_argument(
        "-m" "--locality",
        type=int,
        default=4276,
        help="Locality ID. See municipios.json for more info",
    )
    args = parser.parse_args()

    DAY_DELTA = args.days
    ITERATIONS = args.iterations
    SLEEP_TIME = args.sleep
    END_DATE = args.e__end_date

    idmun = args.m__locality

    main(idmun)
