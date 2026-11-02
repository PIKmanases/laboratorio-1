# laboratorio-1

Pronóstico semanal de ventas y pedido sugerido por tienda y familia (competencia
[Store Sales – Time Series Forecasting](https://www.kaggle.com/competitions/store-sales-time-series-forecasting) de Kaggle),
construido como un producto de ML reproducible y orquestado con Airflow.

```
bandeja ──► ingesta ──► validación ──► features as-of ──► pronóstico (LightGBM por cuantiles) ──► pedido ──► manifiesto
 (D+1..D+3)  bronze        silver          (cada corte)        (lunes, 7 días)                  (costs.yaml)   (trazabilidad)
                 └── cuarentena                                                                       │
                                                      seguimiento diario del desempeño ◄──────────────┘
```

Cuatro DAGs (`dags/`): `ingesta_validacion` (diario), `preparacion_features` (diario), `pronostico_pedido` (semanal, lunes) y
`seguimiento_desempeno` (diario). Los DAGs solo orquestan; la lógica está en `src/laboratorio/`.
Las decisiones de diseño, con alternativas y justificaciones, están en [`DECISIONES.md`](DECISIONES.md).

## Puesta en marcha desde cero

Requisitos: Linux/macOS/WSL con **Git**, **Docker con Compose** y unos **8 GB de RAM** libres para Docker
(Airflow + Postgres + el entrenamiento inicial del modelo; es una estimación, no se probó el stack completo y con 4 GB probablemente no alcance).

**1. Clonar**

```bash
git clone https://github.com/PIKmanases/laboratorio-1.git
cd laboratorio-1
```

**2. Colocar los datos de Kaggle**

Descarga el zip de la competencia (botón *Download All* en la pestaña *Data*, o `kaggle competitions download -c store-sales-time-series-forecasting`)
y cópialo **sin descomprimir** exactamente en:

```
laboratorio-1/data/raw/kaggle/store-sales-time-series-forecasting.zip
```

Debe contener `train.csv`, `stores.csv`, `holidays_events.csv`, `oil.csv` y `transactions.csv`. Su md5 debe ser el del puntero
`data/raw/kaggle/store-sales-time-series-forecasting.zip.dvc` (el script lo comprueba).

**3. Iniciar todo**

```bash
./iniciar.sh
```

Un solo comando, sin pasos manuales. El script:

1. comprueba que el zip esté en su lugar y que coincida con el versionado en DVC;
2. crea `.env` (con tu UID y un secreto aleatorio para Airflow) si no existe;
3. construye la imagen y levanta Postgres y Airflow 3 (`docker compose up -d --build`);
4. el servicio de un solo uso `airflow-orquestador` espera a que Airflow cargue los DAGs, **los activa** (nacen en pausa) y
   **lanza el backfill** de todo el periodo (`orquestacion.inicio` a `orquestacion.fin` en `config/pipeline.yaml`: 2017-06-30 a 2017-08-21).

Airflow queda en <http://localhost:8080> (sin login en local). En la primera semana de pronóstico la tarea `pronosticar`
**entrena sola el modelo inicial** (unos minutos); no hay que correr nada antes.

**4. Seguir el avance**

```bash
docker compose ps                                   # servicios; airflow-orquestador termina en "Exited (0)"
docker compose logs airflow-orquestador             # DAGs activados y backfills creados
```

En la UI, cada DAG muestra una corrida por día (por lunes en `pronostico_pedido`). El flujo terminó cuando las 53 corridas diarias
de `ingesta_validacion` y `seguimiento_desempeno` y las 8 semanales de `pronostico_pedido` están en verde. No se ha medido el tiempo
total en Docker; en una laptop espera decenas de minutos (el primer `preparacion_features` prepara un año de cortes y el
entrenamiento inicial tarda ~2 min).

**5. Ver los resultados**

```
forecasts/pronosticos/fecha=<lunes>/rev=<N>.parquet             # pronóstico por tienda, familia y día (mediana y cuantiles p10/p90)
forecasts/pedidos/fecha=<lunes>/rev=<N>.parquet                 # pedido sugerido por tienda y familia
forecasts/pronosticos/fecha=<lunes>/rev=<N>.manifiesto.json     # trazabilidad: SHA de Git, hash de datos, modelo, corte, config
data/monitoring/desempeno/fecha=<lunes>/rev=<N>.parquet         # WAPE, sesgo, costo y fill rate contra lo realmente vendido
data/cuarentena/<fuente>/fecha=<D>/rev=<N>.parquet              # archivos rechazados (+ rev=<N>.motivos.json)
models/campeon/                                                  # modelo vigente (+ metricas.json con la decisión de reemplazo)
```

```python
# PYTHONPATH=src python
from laboratorio.capas import load_as_of
pedido = load_as_of("pedidos", "2017-08-14", capa="pronosticos")        # pedido sugerido del lunes 14
cols = ["store_nbr", "family", "demanda_esperada", "cantidad_pedido", "nivel_servicio"]
print(pedido[cols].sort_values("cantidad_pedido", ascending=False).head(10))
desempeno = load_as_of("desempeno", "2017-08-21", capa="seguimiento")  # cómo le fue a cada pronóstico
print(desempeno[desempeno.family == "TOTAL"][["date", "wape", "sesgo", "dias_observados", "alerta"]])
```

Detener y reiniciar:

```bash
docker compose down        # detiene; conserva datos y la base de Airflow
docker compose down -v     # además borra la base de Airflow
rm -rf data/bronze data/silver data/cuarentena data/inbox data/features data/monitoring forecasts/pronosticos forecasts/pedidos   # borra los resultados
./iniciar.sh               # repetirlo es seguro: lo ya procesado no se vuelve a escribir
```

## Qué hace el flujo sin intervención manual

| Situación | Qué hace el sistema |
|---|---|
| Un dato llega con retraso (ventas D+1, transacciones D+2, petróleo D+3) | Cada día se procesa lo que ya llegó; los features usan lo último conocido y `dias_desde_ultimo_dato` lo registra. Nada se lee "del futuro" (`load_as_of`). |
| Llega una corrección (`_rev2`) | Se guarda como revisión nueva (la anterior no se toca). El seguimiento recalcula el desempeño con el dato corregido; los pronósticos ya publicados no se reescriben (ver DECISIONES). |
| Un archivo es inválido (columnas, tipos, rangos, nulos, duplicados, tienda desconocida) | Va a `data/cuarentena/` con sus motivos; silver conserva la última revisión válida. El flujo sigue. |
| Se repite una tarea o un día completo (reintento, backfill, reinicio) | Es idempotente: misma entrada, mismo resultado, sin duplicar. Una tarea fallida se reintenta 1 vez a los 1 min. |
| Un DAG depende de otro que aún no termina | Sensores en modo `reschedule` (no ocupan slot) esperan hasta 6 h; los DAGs con orden estricto usan `depends_on_past`. |
| No existe modelo campeón (primera corrida) | `pronosticar` entrena el inicial con la carga histórica; el backtest y la regla de reemplazo deciden entre LightGBM y la línea base. |
| El desempeño empeora (WAPE de un pronóstico > `seguimiento.umbral_wape`) | Queda marcado `alerta` en el desempeño y se escribe una advertencia en el log. No cambia el modelo solo. |
| Se cambian los costos | Se edita `config/costs.yaml`; el siguiente pedido los usa y su manifiesto cambia de hash. No se toca código. |

Lo que **no** es automático (y está dicho en DECISIONES): el reentrenamiento periódico (se hace con `dvc repro`), el bloqueo del
pedido si una fuente crítica queda en cuarentena, y descontar el inventario real del pedido (no hay datos de inventario).

## Calidad de las recomendaciones

Backtest temporal con 4 folds (lunes 2017-05-29 a 2017-06-19), modelo entrenado solo con lo conocido antes de cada corte
(`models/campeon/metricas.json`):

| | WAPE | Sesgo | Fill rate | Costo | Cobertura [p10, p90] |
|---|---|---|---|---|---|
| Línea base (media del mismo día de la semana) | 0.146 | +0.4 % | 0.980 | 1 204 755 | – |
| LightGBM por cuantiles (campeón) | 0.126 | −1.9 % | 0.974 | 1 005 785 (−16.5 %) | 0.81 |

Cómo leerlo:

- Los costos de `config/costs.yaml` son **valores de referencia**: el costo y el "mejor" modelo cambian con los costos reales.
- El pedido supone **inventario inicial cero** y no promociones futuras conocidas; es una necesidad bruta, no una orden neta
  (ver más abajo y DECISIONES).
- Durante el flujo, `seguimiento_desempeno` mide cada pronóstico contra lo vendido; esa es la medida de calidad en operación.
- El modelo gana en 3 de 4 folds: la mejora es real pero no holgada.

### Usar el pedido sugerido

`cantidad_pedido` es lo que conviene tener disponible para cubrir los 7 días siguientes al lunes con el nivel de servicio
`costo_faltante / (costo_faltante + costo_sobrante)` de cada familia. Para convertirlo en una orden de compra:

```
orden = max(0, cantidad_pedido − inventario disponible − pedidos en tránsito)
```

El inventario no está en los datos de Kaggle; ese descuento lo hace quien tenga el dato (o se agrega como fuente, ver DECISIONES).

## Desarrollo local (sin Docker)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
export PYTHONPATH=src

pytest                 # pruebas con datos de ejemplo (tests/data/); el CI nunca usa dvc pull
ruff check .

python -m laboratorio.forecast.entrenamiento --preparar-capas   # = dvc repro: backtest, regla de reemplazo y modelo campeón
python -m laboratorio.tareas ingestar 2017-07-03                 # una tarea suelta (ver TAREAS en src/laboratorio/tareas.py)
```

## Datos, DVC y trazabilidad

Carpetas de datos (Git las ignora; en Git solo van `*.dvc` y `dvc.lock`):

```
data/raw/kaggle/   # zip de Kaggle                  (versionado con DVC: dvc add)
models/campeon/    # modelo vigente                 (versionado con DVC: stage `entrenar` de dvc.yaml)
forecasts/         # pronósticos, pedidos, manifiestos
```

```bash
dvc repro      # reentrena y decide si el candidato reemplaza al campeón (regla en src/laboratorio/forecast/reemplazo.py)
dvc push       # respalda en el remoto local (.dvc/config.local; solo esta máquina)
dvc pull       # lo recupera
```

Si cambia el zip: `dvc add data/raw/kaggle/store-sales-time-series-forecasting.zip` y commitea el `.dvc` en un PR.

Cada pronóstico tiene un manifiesto con el SHA de Git, el md5 del dato crudo (DVC), la versión del modelo y con qué datos se
entrenó, la fecha de corte y el hash de la configuración. Para comprobar que se regenera idéntico:

```bash
PYTHONPATH=src python -m laboratorio.publicacion.reproducir 2017-08-14
```

Hace `git worktree` + `dvc checkout` de esa versión, regenera el pronóstico desde cero y compara los manifiestos (requiere que el
pronóstico se haya generado con el árbol limpio). En Docker el SHA viene de `LABORATORIO_GIT_SHA`, que `iniciar.sh` exporta.

## Capas as-of (referencia)

```
data/bronze/<fuente>/fecha=<D>/rev=<N>.parquet      # ingesta: lo recibido, tal cual
data/silver/<fuente>/fecha=<D>/rev=<N>.parquet      # validación: revisiones válidas
data/cuarentena/<fuente>/fecha=<D>/rev=<N>.parquet  # revisiones rechazadas (+ rev=<N>.motivos.json)
```

- Carga histórica: datos con fecha <= `2017-06-30` (`bandeja.fecha_corte`), leídos del zip. Lo posterior llega por la bandeja
  (`data/inbox/`), un archivo por fecha y fuente, con su retraso; en algunas fechas el original trae errores y luego llega `_rev2`.
  Solo el simulador (`laboratorio.inbox`) escribe en la bandeja; la ingesta únicamente la lee.
- Cada fila lleva `revision`, `available_at` (el día en que el sistema la conoció) y `origen`. Cada escritura es atómica y
  una revisión escrita nunca se modifica.
- `load_as_of(fuente, corte)` devuelve, por fecha, la última revisión con `available_at <= corte` y falla (`FugaDeInformacion`)
  si hubiera filas posteriores al corte.
- Para que el as-of sea fiel, la ingesta corre **día por día** con la bandeja de ese mismo día simulado (lo hace el backfill).

Un día a mano, sin Airflow:

```bash
export PYTHONPATH=src
python -m laboratorio.inbox    --hasta         2017-07-01
python -m laboratorio.ingest   --fecha-proceso 2017-07-01
python -m laboratorio.validate --fecha-proceso 2017-07-01
```

Todos los parámetros (periodo, retrasos, esquemas de validación, modelo, reglas de reemplazo, umbrales) están en
`config/pipeline.yaml`; los costos en `config/costs.yaml`.
