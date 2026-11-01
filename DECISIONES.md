# Registro de decisiones

| Fecha | Decisión | Motivo |
|-------|----------|--------|
| 2026-11-01 | Los datos de Kaggle se cargan como un único zip sin descomprimir en `data/raw/kaggle/` y el flujo lee los CSV desde el zip. | El dato crudo queda inmutable e idéntico a la descarga; DVC rastrea un solo archivo (un hash) y el flujo no depende de comandos de DVC para leerlo. |
