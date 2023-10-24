import sqlite3, random, logging
from time import sleep
from datetime import date
from typing import Optional, Union
from pathlib import Path

from models import APIGasStation

logger = logging.getLogger(__name__)


class DBConnection:
    """Class to handle the database"""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = self.connect_db()

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.conn.close()

    def connect_db(self) -> sqlite3.Connection:
        """Connect to the database"""

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        return conn

    def get_stations(self) -> list:
        """Get the stations from the database"""

        # Get the stations from the database
        cursor = self.conn.cursor()
        cursor.execute("SELECT ideess, company, cp, address FROM stations")
        stations = cursor.fetchall()

        return stations

    def save_stations(self, parsed_data: list[APIGasStation]) -> None:
        """Add the stations to the database"""

        cursor = self.conn.cursor()

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
                self.conn.commit()
                break
            except sqlite3.OperationalError:
                delay = 2**attempts * random.random()
                logger.warning(
                    f"Database locked (#{attempts}), retrying in {delay:.3f} seconds"
                )
                sleep(delay)
                attempts += 1

    def get_price(self, date: date, ideess: int) -> Optional[Union[float, int]]:
        """Get the price of a station on a date"""

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT precio_gasoleo_a FROM prices WHERE date = ? AND ideess = ?",
            (date, ideess),
        )
        price = cursor.fetchone()

        return price[0] if price else None

    def save_prices(self, data: list, single_date: date) -> None:
        """Add the prices to the database"""

        cursor = self.conn.cursor()

        stations = [station.as_sql_prices(single_date) for station in data]

        attempts = 1
        while attempts:
            try:
                cursor.executemany(
                    "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    stations,
                )
                self.conn.commit()
                break
            except sqlite3.OperationalError:
                delay = 2**attempts * random.random()
                logger.warning(
                    f"Database locked (#{attempts}), retrying in {delay:.3f} seconds"
                )
                sleep(delay)
                attempts += 1

        logger.info(f"Prices for {single_date} added to the database")

    # def get_prices(self, date: date) -> list:
    #     """Get the prices from the database"""

    #     cursor = self.conn.cursor()
    #     cursor.execute(
    #         "SELECT ideess, precio_gasoleo_a, precio_gasoleo_b, precio_gasolina_95, precio_gasolina_98, precio_glp FROM prices WHERE date = ?",
    #         (date,),
    #     )
    #     prices = cursor.fetchall()

    #     return prices

    # def get_price_range(self, start_date: date, end_date: date, ideess: int) -> list:
    #     """Get the price of a station on a date"""

    #     cursor = self.conn.cursor()
    #     cursor.execute(
    #         "SELECT date, precio_gasoleo_a FROM prices WHERE date BETWEEN ? AND ? AND ideess = ?",
    #         (start_date, end_date, ideess),
    #     )
    #     prices = cursor.fetchall()

    #     return prices
