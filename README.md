# laboratorio-1

Pronóstico semanal de ventas y pedido sugerido por tienda y familia (competencia
[Store Sales – Time Series Forecasting](https://www.kaggle.com/competitions/store-sales-time-series-forecasting) de Kaggle),
orquestado con Airflow y reproducible (Git + DVC).

## Puesta en marcha (copiar y pegar)

**Requisitos:** Git y Docker con Compose, y unos 8 GB de RAM libres para Docker (estimación: Airflow + Postgres + el
entrenamiento inicial; con 4 GB probablemente no alcance). No hace falta instalar Python ni dependencias.

```bash
# 0. Comprobar requisitos (ambos comandos deben responder con una versión)
git --version
docker compose version

# 1. Clonar el repositorio
git clone https://github.com/PIKmanases/laboratorio-1.git
cd laboratorio-1

# 2. Colocar el zip de Kaggle (SIN descomprimir) en data/raw/kaggle/ con este nombre exacto
cp ~/Descargas/store-sales-time-series-forecasting.zip data/raw/kaggle/store-sales-time-series-forecasting.zip

# 3. Iniciar todo (valida el zip, crea .env, levanta Airflow y lanza el flujo completo)
./iniciar.sh
```

Listo: Airflow queda en <http://localhost:8080> (sin login en local) y el flujo corre solo. Pasos 2 y 3 son los únicos
que hace una persona; no hay que activar DAGs ni lanzar nada en la UI.

- **El zip** se descarga de Kaggle (pestaña *Data* → *Download All*, o `kaggle competitions download -c store-sales-time-series-forecasting`).
  Debe contener `train.csv`, `stores.csv`, `holidays_events.csv`, `oil.csv` y `transactions.csv`. Si ya lo tienes en otra carpeta, solo cópialo con el comando 2
  (ajusta la ruta de origen). Su md5 debe ser el de `data/raw/kaggle/store-sales-time-series-forecasting.zip.dvc`; si no, `iniciar.sh` se detiene y lo dice.
- **WSL/Windows:** clona dentro del sistema de archivos de Linux (por ejemplo `~/`), no en `/mnt/c`, y con Docker Desktop con integración WSL activada.
  Si clonas desde Windows, usa `git config --global core.autocrlf false` antes, o `iniciar.sh` fallará por los saltos de línea.
- **Linux sin Docker Desktop:** tu usuario debe poder usar Docker (`sudo usermod -aG docker $USER` y volver a iniciar sesión).

Qué hace `./iniciar.sh`:

1. comprueba que el zip esté en su lugar y que coincida con el versionado en DVC;
2. crea `.env` (con tu UID y un secreto aleatorio para Airflow) si no existe;
3. construye la imagen y levanta Postgres y Airflow 3 (`docker compose up -d --build`; la primera vez descarga e instala, varios minutos);
4. el servicio de un solo uso `airflow-orquestador` espera a que Airflow cargue los DAGs, **los activa** (nacen en pausa) y
   **lanza el backfill** de todo el periodo (`orquestacion.inicio` a `orquestacion.fin` en `config/pipeline.yaml`: 2017-06-30 a 2017-08-21).

En la primera semana de pronóstico la tarea `pronosticar` **entrena sola el modelo inicial** (unos minutos); no hay que correr nada antes.

## Cómo comprobar que todo funciona

**En Docker (la prueba real)**

1. Servicios arriba:

   ```bash
   docker compose ps
   ```

   Esperado: `postgres`, `airflow-apiserver`, `airflow-scheduler` y `airflow-dag-processor` en *healthy*; `airflow-init` y
   `airflow-orquestador` en *Exited (0)* (son de un solo uso).

2. El orquestador activó los DAGs y creó los backfills:

   ```bash
   docker compose logs airflow-orquestador
   ```

   Esperado: `[orquestar] <dag> activo` para los 4 DAGs, `[orquestar] backfill 2017-06-30 .. 2017-08-21` y `listo`, sin errores.

3. En <http://localhost:8080> → *DAGs*: los 4 aparecen activos y van llenando corridas en verde. Terminó cuando hay **53 corridas diarias** en
   `ingesta_validacion` y `seguimiento_desempeno` y **8 semanales** (lunes 2017-07-03 a 2017-08-21) en `pronostico_pedido`. Una corrida en rojo
   se abre desde su *Log*; los sensores en espera aparecen como *up_for_reschedule* y es normal.

4. Los archivos esperados cuando termina:

   ```bash
   ls forecasts/pronosticos | wc -l                          # 8  (un pronóstico por lunes)
   ls forecasts/pedidos | wc -l                              # 8
   ls forecasts/pronosticos/*/*.manifiesto.json | wc -l      # 8  (un manifiesto por pronóstico)
   ls data/monitoring/desempeno | wc -l                      # 7  (lunes 07-03 a 08-14; el del 08-21 aún no tiene ventas observadas)
   ls models/campeon                                         # q10.txt q50.txt q90.txt cuantiles.json meta.json metricas.json
   ```

5. Los datos tienen sentido (con Python y las dependencias: ver *Desarrollo local*):

   ```python
   # PYTHONPATH=src python
   from laboratorio.capas import load_as_of
   pedido = load_as_of("pedidos", "2017-08-14", capa="pronosticos")        # pedido sugerido del lunes 14
   cols = ["store_nbr", "family", "demanda_esperada", "cantidad_pedido", "nivel_servicio"]
   print(pedido[cols].sort_values("cantidad_pedido", ascending=False).head(10))
   desempeno = load_as_of("desempeno", "2017-08-21", capa="seguimiento")  # cómo le fue a cada pronóstico
   print(desempeno[desempeno.family == "TOTAL"][["date", "wape", "sesgo", "dias_observados", "alerta"]])
   ```

   Esperado: `cantidad_pedido >= demanda_esperada` y `nivel_servicio` entre 0.55 y 0.8 (según el costo de la familia), WAPE del orden de 0.1–0.2 en pronósticos con la semana
   completa observada (el del 08-14 solo tiene 1 día observado), y `alerta = False` salvo que el WAPE supere `seguimiento.umbral_wape` (0.35).

6. Trazabilidad: abre un manifiesto (`forecasts/pronosticos/fecha=2017-08-14/rev=1.manifiesto.json`) y comprueba que lista el SHA de Git,
   el md5 del zip, la versión del modelo, la fecha de corte y el hash de la configuración. Para demostrar que se regenera idéntico
   (necesita Python, Git y DVC en tu máquina):

   ```bash
   PYTHONPATH=src python -m laboratorio.publicacion.reproducir 2017-08-14   # "REPRODUCIBLE: ... idéntico"
   ```

**Sin Docker (solo Python)**

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
export PYTHONPATH=src

ruff check .                                                  # estilo
pytest -q                                                     # pruebas con datos de ejemplo (tests/data); en equipos justos: pytest -q tests/test_modelado.py tests/test_publicacion.py
python -m laboratorio.publicacion.reproducir --regenerar 2017-08-14   # ~4 min: capas -> features -> pronóstico -> pedido -> manifiesto de un lunes
```

El último comando recorre el flujo completo hasta ese lunes (carga el histórico, ingesta día por día, entrena el modelo si falta, pronostica,
calcula el pedido y escribe el manifiesto) y deja los resultados en `forecasts/` y `data/`. Si termina sin error, el flujo funciona de punta a punta.

## Correr un DAG en Airflow

Con el zip ya en `data/raw/kaggle/`, **`./iniciar.sh` basta**: no hay que correr nada a mano. Los DAGs son del periodo simulado
(2017-06-30 a 2017-08-21), por eso se ejecutan con **backfill** y no con *Trigger* a secas: un *Trigger* usa la fecha de hoy, que está fuera del
periodo, y procesaría una fecha que no corresponde.

| Quiero… | Comando |
|---|---|
| Relanzar todo (solo reintenta lo fallido, no repite lo que ya está bien) | `docker compose run --rm airflow-orquestador` |
| Correr un DAG en un rango de fechas | `docker compose exec airflow-scheduler airflow backfill create --dag-id ingesta_validacion --from-date 2017-07-03 --to-date 2017-07-10 --reprocess-behavior failed --max-active-runs 1` |
| Rehacer corridas que ya estaban en verde | el mismo comando con `--reprocess-behavior completed` |
| Pausar / activar un DAG | `docker compose exec airflow-scheduler airflow dags pause ingesta_validacion` / `... dags unpause ...` |
| Ver logs de una tarea | UI → DAG → corrida → tarea → *Log* |
| Ver por qué falló el servicio de arranque | `docker compose logs airflow-orquestador` |

Desde la UI: abre el DAG y usa **Backfill** (no *Trigger*), con las fechas del periodo.

Orden y dependencias, para que no te sorprenda: `ingesta_validacion` es la base (cada día exige que el anterior haya terminado,
`depends_on_past`); `preparacion_features` y `seguimiento_desempeno` esperan a su validación con sensores; `pronostico_pedido` corre solo los lunes y
espera los features de ese día. Si lanzas a mano un DAG de más abajo antes que los de arriba, simplemente espera (sensores, hasta 6 h) en lugar de fallar.

Detener y reiniciar:

```bash
docker compose down        # detiene; conserva datos y la base de Airflow
docker compose down -v     # además borra la base de Airflow
rm -rf data/bronze data/silver data/cuarentena data/inbox data/features data/monitoring forecasts/pronosticos forecasts/pedidos   # borra los resultados
./iniciar.sh               # repetirlo es seguro: lo ya procesado no se vuelve a escribir
```

## Cómo está armado

```
bandeja ──► ingesta ──► validación ──► features as-of ──► pronóstico (LightGBM por cuantiles) ──► pedido ──► manifiesto
 (D+1..D+3)  bronze        silver          (cada corte)        (lunes, 7 días)                  (costs.yaml)   (trazabilidad)
                 └── cuarentena                                                                       │
                                                      seguimiento diario del desempeño ◄──────────────┘
```

Cuatro DAGs (`dags/`): `ingesta_validacion` (diario), `preparacion_features` (diario), `pronostico_pedido` (semanal, lunes) y
`seguimiento_desempeno` (diario). Los DAGs solo orquestan; la lógica está en `src/laboratorio/`.
Las decisiones de diseño, con alternativas y justificaciones, están en [`DECISIONES.md`](DECISIONES.md).

Dónde queda cada resultado:

```
forecasts/pronosticos/fecha=<lunes>/rev=<N>.parquet             # pronóstico por tienda, familia y día (mediana y cuantiles p10/p90)
forecasts/pedidos/fecha=<lunes>/rev=<N>.parquet                 # pedido sugerido por tienda y familia
forecasts/pronosticos/fecha=<lunes>/rev=<N>.manifiesto.json     # trazabilidad: SHA de Git, hash de datos, modelo, corte, config
data/monitoring/desempeno/fecha=<lunes>/rev=<N>.parquet         # WAPE, sesgo, costo y fill rate contra lo realmente vendido
data/cuarentena/<fuente>/fecha=<D>/rev=<N>.parquet              # archivos rechazados (+ rev=<N>.motivos.json)
models/campeon/                                                  # modelo vigente (+ metricas.json con la decisión de reemplazo)
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
