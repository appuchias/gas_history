#!/usr/bin/env python3

from datetime import date, timedelta
import json, lzma, logging, os, requests
from pathlib import Path
from multiprocessing import Pool

from db import DBConnection
from models import APIGasStation

API_BASE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestresHist/"
FILES_PATH = Path("responses")
DB_PATH = Path("db.sqlite3")

logging.basicConfig(level=logging.INFO)


def main(idmun: int) -> None:
    """Main function"""

    start_date = END_DATE - timedelta(days=DAY_COUNT)
    response_folder = FILES_PATH / str(idmun) if idmun else FILES_PATH

    logging.debug(f"Fetching prices from {END_DATE} backwards {DAY_COUNT} days.")

    with Pool(WORKERS) as executor:
        for current_date in daterange(start_date, END_DATE):
            if not STORE and os.path.exists(
                response_folder / f"{current_date}.json.xz"
            ):
                logging.debug(f"Skipping {current_date}")
                continue

            executor.apply_async(populate_db, (current_date, idmun))

        executor.close()
        print("[·] All tasks submitted. Waiting for completion...")
        executor.join()
        print("[✓] All tasks completed.")


def populate_db(single_date, idmun: int):
    db = DBConnection(DB_PATH)
    logging.debug(f"Fetching data from {single_date}")

    data = fetch_data(single_date, idmun)

    if STORE:
        parsed_data = parse_json(data["ListaEESSPrecio"])

        db.save_stations(parsed_data)
        db.save_prices(parsed_data, single_date)


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
        "-w",
        "--workers",
        type=int,
        default=10,
        help="Number of workers to use for fetching data",
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

    assert args.days > 0, "Days must be a positive integer"
    assert args.workers > 0, "Workers must be a positive integer"

    DAY_COUNT = args.days
    END_DATE = args.end_date
    WORKERS = args.workers
    STORE = args.store
    DB_PATH = Path(args.db_path)

    print(
        f"Fetching {DAY_COUNT} days of data up to {END_DATE} using {WORKERS} workers."
    )
    print(f"Storing data in {DB_PATH}" if STORE else "Not storing data in the DB")

    main(args.locality)
