"""Uso: python -m laboratorio.inbox [--hasta AAAA-MM-DD]

Genera la bandeja simulada según config/pipeline.yaml. Sin --hasta entrega todo.
"""

import argparse
from datetime import date

from laboratorio.config import RAIZ, cargar_config
from laboratorio.inbox.simulador import desde_config, generar_bandeja


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hasta", type=date.fromisoformat, help="día simulado (AAAA-MM-DD)")
    args = p.parse_args()

    kw = desde_config(cargar_config())
    kw["ruta_zip"] = RAIZ / kw["ruta_zip"]
    kw["dir_bandeja"] = RAIZ / kw["dir_bandeja"]
    entregas = generar_bandeja(**kw, hasta=args.hasta)
    revs = sum(e.revision > 1 for e in entregas)
    print(f"{len(entregas)} archivos en {kw['dir_bandeja']} ({revs} revisiones _rev2)")


if __name__ == "__main__":
    main()
