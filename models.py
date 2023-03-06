from dataclasses import dataclass, field


@dataclass
class Station:
    ideess: str
    c_p_: str
    dirección: str
    horario: str
    latitud: str
    longitud: str
    municipio: str
    precio_glp: float
    precio_gasoleo_a: float
    precio_gasoleo_b: float
    precio_gasolina_95_e5: float
    precio_gasolina_98_e5: float
    provincia: str
    rótulo: str

    def __str__(self):
        return f"{self.rótulo} ({self.municipio}) - {self.precio_gasoleo_a}"
