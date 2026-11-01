"""Uso: python -m laboratorio.ingest --fecha-proceso AAAA-MM-DD"""

import argparse
from datetime import date

from laboratorio.config import RAIZ, cargar_config
from laboratorio.ingest.ingesta import desde_config, ingestar


def main() -> None:
    p = argparse.ArgumentParser(description="Ingesta diaria a la capa bronze.")
    p.add_argument("--fecha-proceso", type=date.fromisoformat, required=True)
    args = p.parse_args()

    historico, bandeja = ingestar(**desde_config(cargar_config(), RAIZ), fecha_proceso=args.fecha_proceso)
    print(f"[{args.fecha_proceso}] histórico: {historico.revisiones_nuevas or 'sin cambios'}")
    print(f"[{args.fecha_proceso}] bandeja:   {bandeja.revisiones_nuevas or 'sin cambios'} (revisiones nuevas)")


if __name__ == "__main__":
    main()
