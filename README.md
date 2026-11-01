# laboratorio-1

Proyecto de reproducibilidad y orquestación de un producto de Machine Learning.

## Estructura de datos

```
data/raw/kaggle/   # datos crudos de Kaggle (aquí se carga el zip)
models/            # modelos entrenados
forecasts/         # pronósticos generados
```

Estas carpetas las versiona **DVC** y Git las ignora: en Git solo se commitean los punteros `*.dvc`.

## Carga de datos

1. Descarga el zip de la competencia de Kaggle y cópialo **sin descomprimir** en:

   ```
   data/raw/kaggle/store-sales-time-series-forecasting.zip
   ```

   El zip debe contener: `train.csv`, `stores.csv`, `holidays_events.csv`, `oil.csv` y `transactions.csv`.
   Si el archivo tiene otro nombre, renómbralo o cambia `datos.zip_kaggle` en `config/pipeline.yaml`.

2. Versiónalo con DVC (una sola vez, y cada vez que cambien los datos) y commitea el puntero en un PR:

   ```bash
   source .venv/bin/activate
   dvc add data/raw/kaggle/store-sales-time-series-forecasting.zip
   git add data/raw/kaggle/store-sales-time-series-forecasting.zip.dvc
   ```

El flujo lee los CSV directamente desde el zip (ruta en `config/pipeline.yaml`); no necesita ejecutar comandos de DVC.

### Remoto de DVC

El remoto es una carpeta local de esta máquina, configurada en `.dvc/config.local` (no se versiona):

```bash
dvc remote add --local -d local ~/dvc-remote/laboratorio-1
dvc push   # respaldar los datos
dvc pull   # recuperarlos
```

En otra máquina, los datos se reproducen descargando el zip de Kaggle al mismo lugar: su md5 debe coincidir con el `.dvc`.

## Pruebas

```bash
PYTHONPATH=src pytest
```

Las pruebas y el CI usan datos pequeños de ejemplo en `tests/data/kaggle/`, nunca `dvc pull`.

## Simulador de la bandeja de entrada

```bash
PYTHONPATH=src python -m laboratorio.inbox                     # entrega todo
PYTHONPATH=src python -m laboratorio.inbox --hasta 2017-07-10  # solo lo que llegó hasta ese día
```

- **Carga histórica inicial:** datos con fecha <= `2017-06-30` (`bandeja.fecha_corte`), leídos del zip.
- **Bandeja** (`data/inbox/`): un archivo por fecha y fuente, `<fuente>/<fuente>_<fecha>.csv`. Cada fuente tiene su retraso (ventas D+1, transacciones D+2, petróleo D+3).
- **Revisiones:** en algunas fechas el archivo original trae errores y luego llega `<fuente>_<fecha>_rev2.csv` con los valores corregidos.
- La ingesta (`laboratorio.ingest.bandeja`) lee la bandeja usando la última revisión de cada fecha y **nunca escribe en ella**.

Todos los parámetros están en `config/pipeline.yaml`. Las decisiones están en `DECISIONES.md`.

## Ingesta, validación y capas as-of

```
data/bronze/<fuente>/fecha=<D>/rev=<N>.parquet      # ingesta: lo recibido, tal cual
data/silver/<fuente>/fecha=<D>/rev=<N>.parquet      # validación: revisiones válidas
data/cuarentena/<fuente>/fecha=<D>/rev=<N>.parquet  # revisiones rechazadas (+ _motivos)
```

Un día del flujo (ese día llega a la bandeja lo que toca, se ingesta y se valida):

```bash
export PYTHONPATH=src
python -m laboratorio.inbox    --hasta         2017-07-01
python -m laboratorio.ingest   --fecha-proceso 2017-07-01
python -m laboratorio.validate --fecha-proceso 2017-07-01
```

- El primer día (`2017-06-30`, la fecha de corte) se hace la carga histórica.
- Todo es idempotente: repetir un día no cambia nada.
- Cada archivo se escribe con reemplazo atómico.
- Cada fila lleva `revision`, `available_at` (el día en que el sistema la conoció) y `origen`.
- Silver solo recibe revisiones que pasan los esquemas de pandera definidos en `config/pipeline.yaml` (tipos, rangos, nulos, duplicados y fechas esperadas). Los motivos de cada rechazo quedan en `cuarentena/.../rev=N.motivos.json`.
- Para leer lo que se sabía al cierre del día X:

  ```python
  from laboratorio.capas import load_as_of
  ventas = load_as_of("ventas", "2017-07-20")  # nunca devuelve date ni available_at > corte
  ```

Para que el as-of sea fiel, la ingesta debe correr **día por día** con la bandeja de ese mismo día simulado: si recibe la bandeja completa, no tiene forma de saber cuándo llegó cada archivo.
