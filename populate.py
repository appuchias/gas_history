from datetime import date as datetime, timedelta
import requests, sqlite3
from threading import Thread as T
from time import sleep

from models import Station

BASE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestresHist/"
FILE_PATH = "response.json"
GET_QUERY = "SELECT ideess, company, cp, address FROM stations"
DAY_DELTA = 15
ITERATIONS = 10


def main() -> None:
    delta = timedelta(days=DAY_DELTA)
    end_date = datetime(2021, 3, 4)
    start_date = end_date - delta

    print(
        f"Fetching prices from {end_date} backwards {ITERATIONS} times in {DAY_DELTA} day intervals."
    )
    print(f"Fetching {DAY_DELTA * ITERATIONS} days.\nIt can take a while...\n- - -\n")

    # Will fetch data from 15 days ago to 15 days ago 10 times
    for _ in range(ITERATIONS):
        threads = list()

        for single_date in daterange(start_date, end_date):
            print(f"Fetching data from {single_date}")
            threads.append(T(target=populate_db, args=(single_date,)))
            threads[-1].start()

        for thread in threads:
            thread.join()

        start_date -= delta
        end_date -= delta

        del threads

        print("Sleeping for 5 seconds...\n- - -\n")
        sleep(5)


def populate_db(single_date):
    conn = connect_db()
    fetch_data(single_date)
    data = get_data(conn, single_date)
    add_prices(conn, data, single_date)


def connect_db() -> sqlite3.Connection:
    """Connect to the database"""
    conn = sqlite3.connect("db.sqlite3")
    return conn


def get_data(conn: sqlite3.Connection, date: datetime) -> list[Station]:
    """Get the data from the local file"""
    # try:
    #     with open(FILE_PATH, "r") as f:
    #         data = json.load(f)
    # except FileNotFoundError:
    #     fetch_data(date)
    #     with open(FILE_PATH, "r") as f:
    #         data = json.load(f)
    # data = data["ListaEESSPrecio"]

    data = fetch_data(date)["ListaEESSPrecio"]

    parsed_data = parse_json(data)

    cursor = conn.cursor()

    if len(parsed_data) == len(cursor.execute(GET_QUERY).fetchall()):
        return parsed_data

    for station in parsed_data:
        cursor.execute(
            "INSERT INTO stations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            station.as_sql_station(),
        )

    conn.commit()

    return parsed_data


def fetch_data(date: datetime, idmun: int = 2176) -> dict:
    """Get the data from the API and store it locally"""

    date_str = date.strftime("%d-%m-%Y")
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


def add_prices(conn: sqlite3.Connection, data: list, date: datetime) -> None:
    """Add the prices to the database"""

    cursor = conn.cursor()

    for station in data:
        cursor.execute(
            "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?)",
            station.as_sql_prices(date),
        )

    conn.commit()

    print(f"Prices for {date} added to the database")


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
                dirección=station["Dirección"],
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
    main()
