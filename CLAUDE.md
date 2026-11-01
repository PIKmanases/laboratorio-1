# Reglas para el agente

## Código
- **Lógica solo en `src/laboratorio/`.** Los DAGs en `dags/` solo orquestan: importan y llaman funciones de `src/`, sin lógica propia.
- Los parámetros y costos se leen de `config/` (`costs.yaml`, `pipeline.yaml`), nunca se escriben a mano en el código.
- **Tareas idempotentes.** Ejecutar una tarea dos veces con la misma entrada produce el mismo resultado y no duplica efectos (sobrescribir, no agregar).
- **Principio as-of.** Todo cálculo para una fecha de corte usa solo información disponible hasta esa fecha; nada de datos futuros (sin fuga de información).
- **Capas particionadas y atómicas.** Toda escritura en `data/bronze|silver|cuarentena` usa `laboratorio.capas` (`<fuente>/fecha=<D>/rev=<N>.parquet`, reemplazo atómico). Nunca se modifica una revisión ya escrita: una corrección es una revisión nueva.
- **Lecturas as-of.** Para calcular algo para el día X, leer con `leer_asof(..., asof=X)`; nunca leer las capas sin filtrar por `_fecha_ingesta`.
- **La ingesta nunca escribe en la bandeja** (`data/inbox/`): solo la lee. Solo el simulador (`laboratorio.inbox`) escribe ahí.

## Datos
- **Nada de datos en Git.** Datos y modelos se versionan con DVC (`dvc.yaml`); en Git solo van los archivos `.dvc` y `dvc.lock`.
- El remoto de DVC es local (solo esta máquina, en `.dvc/config.local`); el dato crudo se reproduce descargándolo de Kaggle.
- **El CI nunca usa `dvc pull`.** Las pruebas usan datos pequeños de ejemplo en `tests/data/` (única excepción a la regla anterior).

## Flujo de trabajo
- **PRs obligatorios.** Nunca hacer commit ni push directo a `main`; todo cambio entra por una rama y un Pull Request.
- El PR solo se mergea con el CI en verde (pytest + ruff).
- Cada cambio en `src/` lleva sus pruebas en `tests/` (pytest).
- Las decisiones de diseño relevantes se registran en `DECISIONES.md`.
