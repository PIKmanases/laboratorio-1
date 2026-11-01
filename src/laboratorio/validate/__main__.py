"""Uso: python -m laboratorio.validate --fecha-proceso AAAA-MM-DD"""

import argparse
from datetime import date

from laboratorio.config import RAIZ, cargar_config
from laboratorio.validate.validacion import desde_config, validar


def main() -> None:
    p = argparse.ArgumentParser(description="Validación diaria de bronze a silver.")
    p.add_argument("--fecha-proceso", type=date.fromisoformat, required=True)
    args = p.parse_args()

    r = validar(**desde_config(cargar_config(), RAIZ), fecha_proceso=args.fecha_proceso)
    print(f"[{args.fecha_proceso}] válidas: {r.validas or 'sin cambios'} | cuarentena: {r.rechazadas or 'ninguna'}")


if __name__ == "__main__":
    main()
