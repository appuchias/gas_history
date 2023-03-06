from datetime import datetime, timedelta
import json, requests, pytz, sqlite3

from models import Station

FILE_PATH = "gas_history.json"
BASE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestresHist"
TZ = pytz.timezone("Europe/Madrid")


def main() -> None:
    date = datetime.now(TZ) - timedelta(days=1)

    # data = get_data(date)
    data = get_local_data()

    conn = connect_db()
    cursor = conn.cursor()

    parsed_data = parse_json(data)

    print(parsed_data)


def get_local_data() -> list:
    """Get the data from the local file"""
    with open(FILE_PATH, "r") as f:
        data = json.load(f)
    return data["ListaEESSPrecio"]


def get_data(date: datetime) -> list:
    """Get the data from the API"""

    date_str = date.strftime("%d-%m-%Y")
    response = requests.get(f"{BASE_URL}/{date_str}")

    with open(FILE_PATH, "w") as f:
        json.dump(response.json(), f)

    return response.json()["ListaEESSPrecio"]


def connect_db() -> sqlite3.Connection:
    """Connect to the database"""
    conn = sqlite3.connect("db.sqlite3")
    return conn


def parse_json(data: list) -> list[Station]:
    """Parse the data from the API"""

    parsed_data = []
    for station in data:
        parsed_data.append(
            Station(
                c_p_=station["C.P."],
                dirección=station["Dirección"],
                horario=station["Horario"],
                latitud=station["Latitud"],
                longitud=station["Longitud (WGS84)"],
                municipio=station["Municipio"],
                precio_glp=str_to_price(station["Precio Gases licuados del petróleo"]),
                precio_gasoleo_a=str_to_price(station["Precio Gasoleo A"]),
                precio_gasoleo_b=str_to_price(station["Precio Gasoleo B"]),
                precio_gasolina_95_e5=str_to_price(station["Precio Gasolina 95 E5"]),
                precio_gasolina_98_e5=str_to_price(station["Precio Gasolina 98 E5"]),
                provincia=station["Provincia"],
                rótulo=station["Rótulo"],
                ideess=station["IDEESS"],
            )
        )
    return parsed_data


def str_to_price(price: str) -> float:
    """Convert the price to a float"""

    if not price.replace(",", ".").isdigit():
        return 0.0

    return float(price.replace(",", "."))


if __name__ == "__main__":
    main()
