# Gas history

![Size](https://img.shields.io/github/repo-size/appuchias/gas_history?color=orange&style=flat-square)
[![Author](https://img.shields.io/badge/Project%20by-Appu-9cf?style=flat-square)](https://github.com/appuchias)

## How it works

For now, it's a Python script that fetches the gas prices from the Spanish government's [API](https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/help) and stores them in a SQLite database for plotting purposes.

## Usage

```bash
python gas_history.py [-h] [-d DAYS] [-e END_DATE] [-l LOCALITY] [-t THREADS] [-s]
```

Where

- `DAYS`: Number of days to query in each iteration. Default is 15. Max is around 30 in my experience.
- `END_DATE`: Date to start fetching data from. Default is today.
- `LOCALITY`: Locality ID. See `municipios.json` for more info. (CTRL+F will help you)
- `THREADS`: Number of threads to use. Default is 10. Increasing this number can break the script if you specify -s.
- Specify `-s` to store downloaded data in the database.

It expects a SQLite database (`db.sqlite3`) with the following tables:

```sql
CREATE TABLE "Stations" (
 "ideess" INTEGER NOT NULL UNIQUE,
 "company" TEXT,
 "cp" TEXT,
 "address" TEXT,
 "latitude" REAL,
 "longitude" REAL,
 "locality" TEXT,
 "province" TEXT,
 PRIMARY KEY("ideess")
);
```

```sql
CREATE TABLE "Prices" (
 "ideess" INTEGER NOT NULL,
 "date" TEXT NOT NULL,
 "precio_gasoleo_a" REAL,
 "precio_gasoleo_b" REAL,
 "precio_gasolina_95" REAL,
 "precio_gasolina_98" REAL,
 "glp" REAL,
 FOREIGN KEY("ideess") REFERENCES "Stations"("ideess"),
 PRIMARY KEY("ideess","date")
);
```

> A sample database is included in the repo. (You'll have to rename `example.sqlite3` to `db.sqlite3`)

The repo includes the file `graphs/grafana.sql` which you can use to plot `precio_gasoleo_a` and `precio_gasolina_95` in Grafana.
There are also some lower quality sample graphs in `graphs/*.png`.

## License

This code is licensed under the [GPLv3 license](https://github.com/appuchias/gas_history/blob/master/LICENSE).

Coded with 🖤 by Appu
