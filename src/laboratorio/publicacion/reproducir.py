"""Reproducibilidad de un pronóstico publicado.

    python -m laboratorio.publicacion.reproducir 2017-08-14

Con el manifiesto del pronóstico:
1. `git worktree` en el SHA del manifiesto (el repositorio actual no se toca);
2. `dvc checkout` de esa versión (dato crudo y modelo campeón, desde la caché de DVC);
3. se regenera el pronóstico y el pedido de ese corte desde cero en esa copia (capas,
   features, pronóstico, pedido y manifiesto);
4. se compara el manifiesto regenerado con el original: debe ser idéntico, incluidos los
   hashes del contenido del pronóstico y del pedido.

Sale con código 0 si es idéntico y 1 si no.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

from laboratorio.config import RAIZ, cargar_config
from laboratorio.publicacion import manifiesto


def regenerar(fecha: date, cfg: dict | None = None) -> dict:
    """Pronóstico, pedido y manifiesto del corte `fecha` desde las capas, en el repositorio actual."""
    from laboratorio import tareas

    cfg = cfg or cargar_config()
    tareas.procesar_capas_hasta(fecha, cfg)
    tareas.preparar_features(fecha, cfg, historia=False)
    tareas.pronosticar(fecha, cfg)
    tareas.generar_pedido(fecha, cfg)
    tareas.publicar(fecha, cfg)
    return manifiesto.leer(RAIZ / cfg["capas"]["pronosticos"], fecha)


def diferencias(original: dict, regenerado: dict, ruta: str = "") -> list[str]:
    """Campos que no coinciden entre dos manifiestos (vacío = idénticos)."""
    if isinstance(original, dict) and isinstance(regenerado, dict):
        claves = sorted(original.keys() | regenerado.keys())
        return [d for k in claves for d in diferencias(original.get(k), regenerado.get(k), f"{ruta}{k}.")]
    return [] if original == regenerado else [f"{ruta[:-1]}: {original!r} != {regenerado!r}"]


def _ejecutar(cmd: list[str], cwd: Path, env: dict | None = None) -> str:
    r = subprocess.run(cmd, check=False, cwd=cwd, capture_output=True, text=True, env={**os.environ, **(env or {})})
    if r.returncode:
        raise RuntimeError(f"{' '.join(cmd)}\n{r.stdout}\n{r.stderr}")
    return r.stdout


def preparar_version(sha: str, destino: Path) -> None:
    """Deja en `destino` el código del commit `sha` y sus datos/modelo de DVC."""
    _ejecutar(["git", "worktree", "add", "--detach", str(destino), sha], RAIZ)
    local = RAIZ / ".dvc" / "config.local"
    if local.exists():  # remoto local de esta máquina (no se versiona)
        shutil.copy(local, destino / ".dvc" / "config.local")
    _ejecutar(["dvc", "config", "--local", "cache.dir", str(RAIZ / ".dvc" / "cache")], destino)
    _ejecutar(["dvc", "checkout"], destino)


def regenerar_en_version(sha: str, fecha: date, conservar: bool = False) -> dict:
    """Manifiesto del pronóstico regenerado desde cero en una copia del repositorio en `sha`."""
    cfg = cargar_config()
    base = Path(tempfile.mkdtemp(prefix="reproducir-"))
    copia = base / "version"
    try:
        preparar_version(sha, copia)
        entorno = {"PYTHONPATH": str(copia / "src")}
        _ejecutar([sys.executable, "-m", "laboratorio.publicacion.reproducir", "--regenerar", fecha.isoformat()], copia, entorno)
        return manifiesto.leer(copia / cfg["capas"]["pronosticos"], fecha)
    finally:
        if not conservar:
            subprocess.run(["git", "worktree", "remove", "--force", str(copia)], check=False, cwd=RAIZ, capture_output=True)
            shutil.rmtree(base, ignore_errors=True)


def reproducir(fecha: date, permitir_sucio: bool = False, conservar: bool = False, regenerador=None) -> tuple[bool, list[str]]:
    """Regenera el pronóstico publicado de `fecha` y lo compara con el original: (idéntico, diferencias)."""
    cfg = cargar_config()
    original = manifiesto.leer(RAIZ / cfg["capas"]["pronosticos"], fecha)
    if original["git"]["sucio"] and not permitir_sucio:
        return False, ["el pronóstico se generó con cambios sin commitear: no se puede garantizar su reproducción"]
    if not original["git"]["sha"]:
        return False, ["el manifiesto no tiene SHA de git"]
    regenerar_ = regenerador or (lambda sha, dia: regenerar_en_version(sha, dia, conservar))
    regenerado = regenerar_(original["git"]["sha"], fecha)
    dif = diferencias(original, regenerado)
    return not dif, dif


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("fecha", nargs="?", help="corte del pronóstico (AAAA-MM-DD)")
    ap.add_argument("--regenerar", metavar="FECHA", help="uso interno: regenera en el repositorio actual")
    ap.add_argument("--permitir-sucio", action="store_true")
    ap.add_argument("--conservar", action="store_true", help="no borra la copia temporal")
    args = ap.parse_args(argv)
    if args.regenerar:
        print(json.dumps(regenerar(date.fromisoformat(args.regenerar)), indent=2))
        return
    if not args.fecha:
        ap.error("falta la fecha del pronóstico")
    ok, dif = reproducir(date.fromisoformat(args.fecha), args.permitir_sucio, args.conservar)
    print("REPRODUCIBLE: el pronóstico regenerado es idéntico" if ok else "NO REPRODUCIBLE:\n  " + "\n  ".join(dif))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
