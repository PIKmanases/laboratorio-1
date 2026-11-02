"""Reporte de evidencia: ejecuciones en Airflow, comparación de modelos y seguimiento del desempeño.

    PYTHONPATH=src python -m laboratorio.publicacion.reporte [--asof AAAA-MM-DD] [--salida docs/evidencia/reporte.md]

Todo sale de lo que el flujo dejó en disco (pronósticos, pedidos, desempeño y `models/campeon/`) y, si Airflow
responde, de su API (corridas por DAG). No calcula nada nuevo salvo la comparación en vivo contra la línea base,
que usa las mismas funciones del seguimiento (`monitoring.desempeno.evaluar`) y del pedido.
"""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from datetime import date, datetime

import numpy as np
import pandas as pd

from laboratorio.capas import (
    COLUMNAS_CONTROL,
    leer_asof,
    leer_linaje,
    listar_particiones,
    listar_revisiones,
    ruta_particion,
)
from laboratorio.config import RAIZ, cargar_config
from laboratorio.forecast import modelos
from laboratorio.monitoring.desempeno import evaluar
from laboratorio.orders.pedido import calcular_pedido
from laboratorio.publicacion import manifiesto

DAGS = ["ingesta_validacion", "preparacion_features", "pronostico_pedido", "seguimiento_desempeno"]


def _num(x, fmt: str = "{:,.3f}") -> str:
    return "–" if x is None or (isinstance(x, float) and np.isnan(x)) else fmt.format(x)


def tabla(filas: list[list], encabezado: list[str]) -> str:
    """Tabla Markdown."""
    lineas = ["| " + " | ".join(encabezado) + " |", "|" + "|".join("---" for _ in encabezado) + "|"]
    lineas += ["| " + " | ".join(str(c) for c in f) + " |" for f in filas]
    return "\n".join(lineas)


def _sin_control(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=[c for c in [*COLUMNAS_CONTROL, "date"] if c in df.columns])


# --- 1. Airflow ------------------------------------------------------------------------


def _api(url: str, ruta: str, token: str | None = None, cuerpo: dict | None = None) -> dict:
    req = urllib.request.Request(
        url.rstrip("/") + ruta,
        data=json.dumps(cuerpo).encode() if cuerpo else None,
        headers={"Content-Type": "application/json", **({"Authorization": f"Bearer {token}"} if token else {})},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def corridas_airflow(url: str) -> dict[str, list[dict]] | None:
    """Corridas de cada DAG según la API de Airflow; None si no responde."""
    try:
        token = _api(url, "/auth/token", cuerpo={"username": "admin", "password": "admin"})["access_token"]
        return {
            dag: _api(url, f"/api/v2/dags/{dag}/dagRuns?limit=500&order_by=logical_date", token)["dag_runs"] for dag in DAGS
        }
    except (urllib.error.URLError, OSError, KeyError, ValueError):
        return None


def _duracion(r: dict) -> float | None:
    if not r.get("start_date") or not r.get("end_date"):
        return None
    f = datetime.fromisoformat
    return (f(r["end_date"].replace("Z", "+00:00")) - f(r["start_date"].replace("Z", "+00:00"))).total_seconds()


def seccion_airflow(corridas: dict[str, list[dict]] | None) -> str:
    if corridas is None:
        return "_Airflow no respondió: no se pudieron listar las corridas. Usa las capturas de la interfaz._"
    filas = []
    for dag, runs in corridas.items():
        estados = pd.Series([r["state"] for r in runs]).value_counts().to_dict()
        durs = [d for d in map(_duracion, runs) if d is not None]
        fechas = sorted(r["logical_date"][:10] for r in runs)
        filas.append([
            f"`{dag}`", len(runs), estados.get("success", 0), estados.get("failed", 0),
            f"{fechas[0]} a {fechas[-1]}" if fechas else "–", _num(float(np.mean(durs)) if durs else None, "{:.0f} s"),
        ])  # fmt: skip
    return tabla(filas, ["DAG", "Corridas", "Exitosas", "Fallidas", "Fechas lógicas", "Duración media"])


# --- 2. Comparación de modelos (backtest) ----------------------------------------------


def seccion_modelos(cfg: dict) -> str:
    ruta = modelos.dir_campeon(cfg)
    if not (ruta / "metricas.json").exists():
        return "_No hay `models/campeon/metricas.json`: corre `dvc repro`._"
    info = json.loads((ruta / "metricas.json").read_text(encoding="utf-8"))
    meta = json.loads((ruta / "meta.json").read_text(encoding="utf-8"))
    ant, cand = info["campeon_anterior"], info["candidato"]

    def fila(nombre, r):
        return [nombre, _num(r["wape"], "{:.4f}"), _num(r["sesgo"], "{:+.4f}"), _num(r["rmsle"], "{:.4f}"),
                _num(r["fill_rate"], "{:.4f}"), _num(r["costo"], "{:,.0f}"), _num(r["costo_por_unidad_vendida"], "{:.4f}"),
                _num(r.get("cobertura"), "{:.3f}")]  # fmt: skip

    out = [
        f"Backtest temporal (rolling origin), cortes de prueba: {', '.join(cand['cortes_prueba'])}.",
        "",
        tabla([fila(f"{ant['receta']['tipo']} (campeón anterior)", ant), fila(f"{cand['receta']['tipo']} (candidato)", cand)],
              ["Modelo", "WAPE", "Sesgo", "RMSLE", "Fill rate", "Costo", "Costo/unidad", "Cobertura p10–p90"]),
        "",
        "Costo por corte de prueba (menor es mejor):",
        "",
        tabla([[c, _num(a, "{:,.0f}"), _num(b, "{:,.0f}"), "candidato" if b < a else "campeón"]
               for c, a, b in zip(cand["cortes_prueba"], ant["costo_por_fold"], cand["costo_por_fold"], strict=True)],
              ["Corte", "Costo campeón anterior", "Costo candidato", "Gana"]),
        "",
        "Regla de reemplazo (`forecast/reemplazo.py`):",
        "",
        tabla([[c["nombre"], "sí" if c["cumple"] else "no", c["detalle"]] for c in info["decision"]["condiciones"]],
              ["Condición", "Cumple", "Detalle"]),
        "",
        f"**Decisión:** {'reemplazar' if info['decision']['reemplazar'] else 'conservar el campeón'} — {info['decision']['motivo']}.",
        "",
        f"**Campeón vigente:** `{meta['receta']['tipo']}`, versión `{meta['version']}`, entrenado con datos hasta "
        f"{meta.get('entrenado_con_datos_hasta')}, cortes de entrenamiento {meta.get('cortes_entrenamiento')}, "
        f"{meta.get('filas_entrenamiento'):,} filas." if meta.get("filas_entrenamiento") else f"**Campeón vigente:** `{meta['receta']['tipo']}`.",
    ]
    return "\n".join(out)


# --- 3. Seguimiento del desempeño ------------------------------------------------------


def _fecha_asof(cfg: dict) -> date | None:
    """Último día en que el seguimiento escribió algo."""
    dir_seg = RAIZ / cfg["capas"]["seguimiento"]
    dias = []
    for corte in listar_particiones(dir_seg, "desempeno"):
        revs = listar_revisiones(ruta_particion(dir_seg, "desempeno", corte))
        dias.append(date.fromisoformat(leer_linaje(revs[max(revs)])["available_at"]))
    return max(dias) if dias else None


def desempeno_por_corte(cfg: dict, asof: date) -> pd.DataFrame:
    """Fila TOTAL del desempeño vigente al día `asof` de cada pronóstico, con su número de revisiones."""
    dir_seg = RAIZ / cfg["capas"]["seguimiento"]
    filas = []
    for corte in sorted(listar_particiones(dir_seg, "desempeno")):
        df = leer_asof(dir_seg, "desempeno", asof, desde=corte, hasta=corte)
        if df.empty:
            continue
        total = df[df["family"] == "TOTAL"].iloc[0].to_dict()
        total["corte"] = corte
        total["revisiones"] = len(listar_revisiones(ruta_particion(dir_seg, "desempeno", corte)))
        filas.append(total)
    return pd.DataFrame(filas)


def seccion_desempeno(cfg: dict, asof: date) -> str:
    d = desempeno_por_corte(cfg, asof)
    if d.empty:
        return "_Todavía no hay desempeño: faltan días observados después de un pronóstico._"
    umbral = cfg["seguimiento"]["umbral_wape"]
    filas = [
        [r.corte, f"{r.dias_observados}/{r.dias_horizonte}", _num(r.wape, "{:.4f}"), _num(r.sesgo, "{:+.4f}"), _num(r.rmsle, "{:.4f}"),
         _num(getattr(r, "fill_rate", np.nan), "{:.4f}"), _num(getattr(r, "costo", np.nan), "{:,.0f}"),
         "ALERTA" if r.alerta else "ok", r.revisiones]
        for r in d.itertuples()
    ]  # fmt: skip
    completos = d[d["horizonte_completo"]]
    resumen = ""
    if not completos.empty:
        y = completos["venta_real"].sum()
        resumen = (
            f"\n\nPronósticos con la semana completa ({len(completos)}): WAPE agregado "
            f"**{(completos['wape'] * completos['venta_real']).sum() / y:.4f}**, sesgo agregado "
            f"**{(completos['sesgo'] * completos['venta_real']).sum() / y:+.4f}**."
        )
    return (
        f"Desempeño vigente al {asof} (umbral de alerta: WAPE > {umbral}). `Días obs.` = días del horizonte ya vendidos; el costo y "
        "el fill rate solo existen con el horizonte completo. `Rev.` > 1 indica que se recalculó por una corrección de datos.\n\n"
        + tabla(filas, ["Corte", "Días obs.", "WAPE", "Sesgo", "RMSLE", "Fill rate", "Costo", "Estado", "Rev."])
        + resumen
    )


def seccion_por_familia(cfg: dict, asof: date) -> str:
    dir_seg = RAIZ / cfg["capas"]["seguimiento"]
    d = desempeno_por_corte(cfg, asof)
    completos = d[d["horizonte_completo"]] if not d.empty else d
    if completos.empty:
        return ""
    corte = completos["corte"].max()
    df = leer_asof(dir_seg, "desempeno", asof, desde=corte, hasta=corte)
    df = df[df["family"] != "TOTAL"].sort_values("wape")
    cols = ["family", "venta_real", "wape", "sesgo", "fill_rate", "costo"]
    fmt = lambda f: [f.family, _num(f.venta_real, "{:,.0f}"), _num(f.wape, "{:.3f}"), _num(f.sesgo, "{:+.3f}"), _num(f.fill_rate, "{:.3f}"), _num(f.costo, "{:,.0f}")]
    enc = ["Familia", "Venta real", "WAPE", "Sesgo", "Fill rate", "Costo"]
    return (
        f"Pronóstico del {corte}, por familia: las 5 mejores y las 5 peores por WAPE.\n\n**Mejores**\n\n"
        + tabla([fmt(f) for f in df[cols].head(5).itertuples()], enc)
        + "\n\n**Peores**\n\n"
        + tabla([fmt(f) for f in df[cols].tail(5).iloc[::-1].itertuples()], enc)
    )


def comparacion_en_vivo(cfg: dict, asof: date) -> str:
    """Campeón vs. línea base sobre los mismos pronósticos del flujo ya observados por completo."""
    d = desempeno_por_corte(cfg, asof)
    if d.empty or not d["horizonte_completo"].any():
        return "_Aún no hay pronósticos con la semana completa observada para comparar._"
    caps = {n: RAIZ / cfg["capas"][n] for n in ("features", "pronosticos", "silver")}
    costos = cargar_config(cfg["pedido"]["costos"])
    umbral, base = cfg["seguimiento"]["umbral_wape"], cfg["modelado"]["baseline"]
    comp: dict[str, list[dict]] = {}
    filas = []
    for corte in d.loc[d["horizonte_completo"], "corte"]:
        pron = _sin_control(leer_asof(caps["pronosticos"], "pronosticos", asof, desde=corte, hasta=corte))
        pred = _sin_control(leer_asof(caps["features"], "predictores", corte, desde=corte, hasta=corte))
        objetivos = pd.to_datetime(pron["fecha_objetivo"])
        reales = leer_asof(caps["silver"], "ventas", asof, desde=objetivos.min().date(), hasta=objetivos.max().date())
        reales = reales[["date", "store_nbr", "family", "sales"]]
        campeon = pron["modelo"].iloc[0]
        ped_campeon = _sin_control(leer_asof(caps["pronosticos"], "pedidos", asof, desde=corte, hasta=corte))
        pron_base = modelos.pronosticar(pred, base)
        ped_base = calcular_pedido(pron_base, pred, costos)
        for nombre, p, o in ((campeon, pron, ped_campeon), (f"{base} (línea base)", pron_base, ped_base)):
            t = evaluar(p, o, reales, umbral).query("family == 'TOTAL'").iloc[0]
            comp.setdefault(nombre, []).append(t.to_dict())
            filas.append([corte, nombre, _num(t["wape"], "{:.4f}"), _num(t["sesgo"], "{:+.4f}"), _num(t["fill_rate"], "{:.4f}"), _num(t["costo"], "{:,.0f}")])
    tot = []
    for nombre, ts in comp.items():
        y = sum(t["venta_real"] for t in ts)
        tot.append([
            f"**{nombre}**", _num(sum(t["wape"] * t["venta_real"] for t in ts) / y, "{:.4f}"),
            _num(sum(t["sesgo"] * t["venta_real"] for t in ts) / y, "{:+.4f}"),
            _num(1 - sum(t["faltante"] for t in ts) / y, "{:.4f}"), _num(sum(t["costo"] for t in ts), "{:,.0f}"),
        ])  # fmt: skip
    return (
        "Los mismos pronósticos del flujo (semana completa observada), con el modelo vigente y con la línea base, "
        "ambos con la misma regla de pedido y los mismos costos.\n\n"
        + tabla(filas, ["Corte", "Modelo", "WAPE", "Sesgo", "Fill rate", "Costo"])
        + "\n\n**Agregado**\n\n"
        + tabla(tot, ["Modelo", "WAPE", "Sesgo", "Fill rate", "Costo"])
    )


def seccion_manifiestos(cfg: dict) -> str:
    dir_pron = RAIZ / cfg["capas"]["pronosticos"]
    filas = []
    for corte in sorted(listar_particiones(dir_pron, "pronosticos")):
        m = manifiesto.leer(dir_pron, corte)
        filas.append([corte, (m["git"]["sha"] or "–")[:10], m["modelo"]["nombre"], m["modelo"]["version"], m["datos"]["md5"][:10],
                      m["configuracion"]["conjunto"][:10], m["pronostico"]["hash_contenido"].split(":")[0][:10]])  # fmt: skip
    if not filas:
        return "_No hay pronósticos publicados._"
    return tabla(filas, ["Corte", "SHA Git", "Modelo", "Versión", "md5 datos", "Hash config", "Hash pronóstico"])


def generar(cfg: dict | None = None, asof: date | None = None, url_airflow: str | None = None) -> str:
    cfg = cfg or cargar_config()
    asof = asof or _fecha_asof(cfg)
    git = manifiesto.estado_git()
    partes = [
        "# Reporte de funcionamiento y desempeño",
        f"Generado con el código `{(git['sha'] or 'desconocido')[:10]}`"
        + (" (con cambios sin commitear)" if git["sucio"] else "")
        + f"; datos al {asof}.",
        "## 1. Ejecuciones en Airflow",
        seccion_airflow(corridas_airflow(url_airflow) if url_airflow else None),
        "## 2. Comparación de modelos",
        "### 2.1 Backtest temporal (decisión de reemplazo)",
        seccion_modelos(cfg),
    ]
    if asof:
        partes += ["### 2.2 En vivo: campeón vs. línea base en el flujo", comparacion_en_vivo(cfg, asof),
                   "## 3. Seguimiento del desempeño", seccion_desempeno(cfg, asof), seccion_por_familia(cfg, asof)]  # fmt: skip
    else:
        partes += ["## 3. Seguimiento del desempeño", "_Todavía no hay desempeño: el flujo no ha corrido._"]
    partes += ["## 4. Trazabilidad de cada pronóstico", seccion_manifiestos(cfg)]
    return "\n\n".join(p for p in partes if p) + "\n"


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--asof", help="día de corte del reporte (por defecto, el último con seguimiento)")
    ap.add_argument("--airflow", default="http://localhost:8080", help="URL de Airflow; 'no' para omitir las corridas")
    ap.add_argument("--salida", default="docs/evidencia/reporte.md")
    args = ap.parse_args(argv)
    texto = generar(asof=date.fromisoformat(args.asof) if args.asof else None, url_airflow=None if args.airflow == "no" else args.airflow)
    salida = RAIZ / args.salida
    salida.parent.mkdir(parents=True, exist_ok=True)
    salida.write_text(texto, encoding="utf-8")
    print(f"reporte escrito en {salida.relative_to(RAIZ)}")


if __name__ == "__main__":
    main()
