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
