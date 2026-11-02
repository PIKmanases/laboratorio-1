"""Regla de reemplazo del modelo campeón.

El candidato reemplaza al campeón solo si cumple TODAS las condiciones, evaluadas
sobre el mismo backtest temporal (mismos folds, mismos datos):

1. Costo de negocio: baja al menos `mejora_minima_costo` (relativo).
2. WAPE: no es peor que el del campeón (más `tolerancia_wape`).
3. Estabilidad: gana en costo en al menos `folds_ganados_minimos` folds.
4. Servicio: fill rate >= `fill_rate_minimo`.
5. Sesgo: |sesgo| <= `sesgo_maximo`.
6. Intervalo: si tiene cuantiles, la cobertura de [p10, p90] está en `cobertura_intervalo`.

Sin métricas válidas (NaN) la condición falla: ante la duda se conserva el campeón.
Los umbrales vienen de `config/pipeline.yaml` (`modelado.reemplazo`).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Condicion:
    nombre: str
    cumple: bool
    detalle: str


@dataclass(frozen=True)
class Decision:
    reemplazar: bool
    condiciones: list[Condicion] = field(default_factory=list)

    @property
    def motivo(self) -> str:
        if self.reemplazar:
            return "el candidato cumple todas las condiciones"
        return "no cumple: " + "; ".join(c.nombre for c in self.condiciones if not c.cumple)

    def a_dict(self) -> dict:
        return {
            "reemplazar": self.reemplazar,
            "motivo": self.motivo,
            "condiciones": [{"nombre": c.nombre, "cumple": c.cumple, "detalle": c.detalle} for c in self.condiciones],
        }


def _valido(x) -> bool:
    return x is not None and not (isinstance(x, float) and math.isnan(x))


def decidir_reemplazo(campeon: dict, candidato: dict, reglas: dict) -> Decision:
    """Compara las métricas agregadas del backtest (`evaluacion.agregar`) con las reglas de config."""
    c, n = campeon, candidato
    cond: list[Condicion] = []

    mejora = 1 - n["costo"] / c["costo"] if c["costo"] else float("nan")
    cond.append(
        Condicion(
            "costo",
            _valido(mejora) and mejora >= reglas["mejora_minima_costo"],
            f"mejora {mejora:.2%} (mínimo {reglas['mejora_minima_costo']:.2%})" if _valido(mejora) else "sin costo del campeón",
        )
    )

    tope_wape = c["wape"] * (1 + reglas["tolerancia_wape"])
    cond.append(
        Condicion(
            "wape",
            _valido(n["wape"]) and n["wape"] <= tope_wape,
            f"candidato {n['wape']:.4f} vs máximo {tope_wape:.4f}",
        )
    )

    ganados = sum(a < b for a, b in zip(n["costo_por_fold"], c["costo_por_fold"], strict=True))
    cond.append(
        Condicion(
            "estabilidad",
            ganados >= reglas["folds_ganados_minimos"],
            f"gana {ganados} de {len(n['costo_por_fold'])} folds (mínimo {reglas['folds_ganados_minimos']})",
        )
    )

    cond.append(
        Condicion(
            "fill_rate",
            _valido(n["fill_rate"]) and n["fill_rate"] >= reglas["fill_rate_minimo"],
            f"{n['fill_rate']:.4f} (mínimo {reglas['fill_rate_minimo']})",
        )
    )

    cond.append(
        Condicion(
            "sesgo",
            _valido(n["sesgo"]) and abs(n["sesgo"]) <= reglas["sesgo_maximo"],
            f"{n['sesgo']:+.4f} (máximo ±{reglas['sesgo_maximo']})",
        )
    )

    cobertura = n.get("cobertura")
    if _valido(cobertura):
        bajo, alto = reglas["cobertura_intervalo"]
        cond.append(Condicion("cobertura", bajo <= cobertura <= alto, f"{cobertura:.3f} (rango {bajo}-{alto})"))

    return Decision(all(x.cumple for x in cond), cond)
