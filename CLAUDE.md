# Reglas para el agente

- La lógica de negocio vive en `src/laboratorio/`; los DAGs en `dags/` solo orquestan, sin lógica.
- Los parámetros y costos se leen de `config/` (`costs.yaml`, `pipeline.yaml`), nunca se escriben a mano en el código.
- Los datos y modelos se versionan con DVC (`dvc.yaml`), nunca con git.
- Cada cambio en `src/` lleva sus pruebas en `tests/` (pytest).
- Las decisiones de diseño relevantes se registran en `DECISIONES.md`.
