from dataclasses import dataclass
from datetime import date


@dataclass
class Station:
    ideess: str
    rotulo: str
    c_p_: str
    dirección: str
    latitud: str
    longitud: str
    municipio: str
    provincia: str
    precio_gasoleo_a: float
    precio_gasoleo_b: float
    precio_gasolina_95: float
    precio_gasolina_98: float
    precio_glp: float

    def __str__(self):
        return f"{self.rotulo} ({self.municipio}) - {self.precio_gasoleo_a}"

    def as_sql_station(self) -> tuple:
        return (
            self.ideess,
            self.rotulo,
            self.c_p_,
            self.dirección,
            self.latitud,
            self.longitud,
            self.municipio,
            self.provincia,
        )

    def as_sql_prices(self, date: date) -> tuple:
        return (
            self.ideess,
            date.toordinal(),
            self.precio_gasoleo_a,
            self.precio_gasoleo_b,
            self.precio_gasolina_95,
            self.precio_gasolina_98,
            self.precio_glp,
        )
