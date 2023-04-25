#! /usr/bin/env python3

from argparse import ArgumentParser
import json, requests, sqlite3

BASE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
LOG_FILE = "log.txt"


def main(station_id: int) -> None:
    ...


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "-s",
        "--station-id",
        help="Station ID to log",
        type=int,
        required=False,
        default=0,
    )

    args = parser.parse_args()

    main(args.station_id)
