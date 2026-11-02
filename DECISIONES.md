# Registro de decisiones

| Fecha | Decisión | Motivo |
|-------|----------|--------|
| 2026-11-01 | Los datos de Kaggle se cargan como un único zip sin descomprimir en `data/raw/kaggle/` y el flujo lee los CSV desde el zip. | El dato crudo queda inmutable e idéntico a la descarga; DVC rastrea un solo archivo (un hash) y el flujo no depende de comandos de DVC para leerlo. |
| 2026-11-01 | Fecha de corte de la carga histórica inicial: **2017-06-30**. Lo posterior (2017-07-01 a 2017-08-15, 46 días) llega por la bandeja simulada. | Deja 4.5 años de historia (4 diciembres completos) y corta en fin de mes. Después del corte, train y transactions no tienen huecos. La ventana simulada incluye un feriado nacional trasladado (10→11 ago) y la tienda 52, que abrió el 2017-04-20 y llega con poca historia (arranque en frío). |
| 2026-11-01 | La bandeja trae tres fuentes diarias con retrasos distintos: ventas D+1, transacciones D+2 y petróleo D+3. `holidays_events.csv` y `stores.csv` son datos de referencia y entran completos en la carga inicial. | El calendario de feriados se publica por adelantado y las tiendas son un catálogo estático, así que tenerlos completos no rompe el principio as-of. Petróleo no publica fines de semana y por eso tiene menos archivos. |
| 2026-11-01 | Revisiones: el 15 % de las fechas de cada fuente llega primero con el 5 % de las filas alteradas (factor de 0.5–0.8 o 1.2–1.5); 3 días después llega `_rev2` con el valor real de Kaggle. Semilla fija (42). | Simula correcciones de la fuente. Como el `_rev2` coincide con Kaggle, se puede validar que la ingesta use siempre la última revisión. |
| 2026-11-01 | La bandeja vive en `data/inbox/`, no se versiona con DVC y se regenera con `python -m laboratorio.inbox`. | Se deriva del zip de forma determinista, así que versionarla solo duplicaría datos. |
| 2026-11-01 | Las capas (`bronze`, `silver`, `cuarentena`) se guardan como `<capa>/<fuente>/fecha=<D>/rev=<N>.parquet`: una partición por fecha y un archivo por revisión, escrito con reemplazo atómico (temporal + `os.replace`). | Cada revisión conserva su esquema exacto, incluso una revisión vacía o con una columna faltante, que en un solo archivo por fecha se perderían. Saber qué revisiones existen basta con listar archivos, sin leer Parquet. Un lector nunca ve un archivo a medias. |
| 2026-11-01 | El as-of se basa en `available_at` = el día de proceso en que el sistema conoció cada revisión; la carga histórica usa la fecha de corte. Cada fila lleva `revision` y `available_at`. `load_as_of(source, cutoff)` devuelve, por fecha, la última revisión con `available_at <= cutoff`; además verifica que ninguna fila tenga `date` ni `available_at` posteriores al corte y, si alguna los tiene, lanza `FugaDeInformacion`. | Permite reconstruir lo que se sabía cualquier día (backtesting sin fuga). Requiere correr la ingesta día por día con la bandeja en ese mismo día simulado; la ingesta no puede saber cuándo llegó un archivo si se le entrega la bandeja completa. |
| 2026-11-01 | Idempotencia: la ingesta y la validación procesan cada revisión una sola vez (si el archivo ya existe en la capa, no se toca) y la carga histórica deja una marca `_carga_historica.json` al terminar. | Repetir un día no cambia nada (ni contenido ni fecha de modificación). Si un proceso se corta, la siguiente corrida retoma sin duplicar. |
| 2026-11-01 | La validación manda a `cuarentena` (con `_motivos`) las revisiones que no cumplen las reglas de `config/pipeline.yaml`: columnas, fecha de la partición, nulos, negativos, claves duplicadas y tiendas conocidas. Silver conserva la última revisión válida. | Un archivo malo no reemplaza un dato bueno ya publicado, y queda trazado el motivo del rechazo. |
| 2026-11-01 | La validación usa **pandera** 0.34.1, con esquemas armados desde `config/pipeline.yaml`: tipos (con conversión), nulos, rangos, columnas exactas, claves únicas, `date` igual a la partición y `date <= available_at`. Silver guarda los datos con los tipos ya convertidos. | Es declarativo, reporta todos los errores de una vez (`lazy=True`) y deja las reglas en la configuración, no en el código. No se usó la 0.35.0 porque se publicó hace menos de dos semanas. |
| 2026-11-01 | Orquestación con Airflow 3 (LocalExecutor, Docker): 4 DAGs (`ingesta_validacion`, `preparacion_features`, `pronostico_pedido` semanal, `seguimiento_desempeno`) que solo llaman a `laboratorio.tareas`; schedule, fechas, reintentos y sensores salen de `config/pipeline.yaml` (`orquestacion`) vía `laboratorio.orquestacion`. | Los DAGs quedan sin lógica (lo verifica `tests/test_dags.py` con `ast`) y las tareas se pueden probar sin Airflow. |
| 2026-11-01 | Backfill: `catchup=False`, `start_date`/`end_date` fijos (2017-06-30 a 2017-08-21), `max_active_runs=1`, `depends_on_past` donde el orden importa y sensores en modo `reschedule`. Cada tarea recibe `fecha={{ ds }}` y es idempotente. | La ingesta as-of exige procesar los días en orden; el backfill reproduce la operación día a día sin fuga de información y se puede repetir sin duplicar. |
| 2026-11-01 | Modelo: **LightGBM por cuantiles** (p10, p50, p90; un modelo por cuantil sobre `log1p(ventas)`), 150 árboles, 4 hilos. La mediana es el pronóstico y `(p90 - p10)/2.56` es el sigma diario del pedido. Línea base y campeón inicial: media del mismo día de la semana. | Corre en ~2 min en una laptop de 4 GB (sin GPU ni red neuronal), da incertidumbre calibrada (cobertura de [p10, p90] = 80 % en el backtest) y la incertidumbre alimenta directo la regla del vendedor de periódicos. Los cuantiles se conservan bajo `log1p`, así que se entrena sobre la escala logarítmica sin sesgo en el cuantil. |
| 2026-11-01 | Esquema temporal (rolling origin): 4 folds de prueba (los últimos 4 lunes con horizonte completo observado). Para el fold F se entrena solo con los últimos 52 cortes semanales `c <= F - 11 días` (7 de horizonte + 1 de anticipación + 1 de retraso de ventas + 3 del `_rev2`). Las ventas se leen as-of. | Garantiza que toda etiqueta de entrenamiento, con su revisión, ya se conocía el día F (sin fuga); `tests/test_modelado.py` lo verifica. Lunes porque el pronóstico es semanal. |
| 2026-11-01 | Métricas técnicas: WAPE, sesgo, RMSLE, pinball por cuantil y cobertura de [p10, p90]. De negocio: costo (faltante × costo_faltante + sobrante × costo_sobrante), faltante, sobrante y fill rate. El backtest reutiliza `monitoring.desempeno.evaluar` y `orders.pedido`. | Es la misma medición del seguimiento en producción: lo que se aprueba en el backtest es lo que luego se vigila. Los folds se suman antes de dividir (no se promedian cocientes). |
| 2026-11-01 | Regla de reemplazo (`forecast/reemplazo.py`, umbrales en `modelado.reemplazo`): el candidato reemplaza al campeón solo si cumple TODAS: costo ≥ 2 % menor, WAPE no peor, gana en costo en ≥ 3 de 4 folds, fill rate ≥ 0.85, \|sesgo\| ≤ 10 % y cobertura de [p10, p90] entre 0.70 y 0.90. Métricas NaN fallan la condición. | Mejorar el costo total no basta: se exige estabilidad entre folds y servicio mínimo para no cambiar de modelo por ruido. Ante la duda se conserva el campeón. Los umbrales son punto de partida; ajustarlos con el negocio. |
| 2026-11-01 | Campeón y candidato se comparan re-corriendo su *receta* (hiperparámetros, cuantiles, ventana) en los mismos folds, no con el artefacto guardado. Si la receta es idéntica, se reentrena con datos más recientes sin comparar. | Un artefacto entrenado antes ya vio parte de los folds de hoy; re-ejecutar la receta con la misma purga hace la comparación justa. |
| 2026-11-01 | El campeón vive en `models/campeon/` (boosters en texto + `meta.json` + `metricas.json`) y lo versiona DVC con el stage `entrenar` (`dvc.yaml`, `persist: true` para conservar el campeón anterior). El intercambio de carpeta es atómico. Cada versión queda en `dvc.lock` + Git. Resultado inicial (backtest 2017-07-17 a 2017-08-07): WAPE 0.156 → 0.145, costo −6.1 %, fill rate 0.973, cobertura 0.80; ganó 3 de 4 folds. | Sin pickle: los modelos de LightGBM en texto se pueden inspeccionar y no ejecutan código al cargarse. `persist` hace que el stage no sea puramente funcional de sus deps; es inherente a comparar con el campeón vigente. |
| 2026-11-02 | Manifiesto por pronóstico (`forecasts/pronosticos/fecha=<D>/rev=<N>.manifiesto.json`, tarea `publicar` del DAG semanal): SHA de Git (y si había cambios sin commitear), md5 del dato crudo (el de DVC), versión del modelo + hash DVC y de contenido de `models/campeon`, fecha de corte, hash de `pipeline.yaml` y del archivo de costos (sobre el YAML ya leído, sin comentarios ni formato) y hash del contenido del pronóstico y del pedido. Sin rutas ni horas de reloj. | Mismo código, datos y configuración => manifiesto idéntico, así que se puede comparar tal cual. El hash de contenido (no el de un Parquet) evita que cambie por metadatos. Un manifiesto ya escrito conserva su SHA original mientras el pronóstico y el pedido sean los mismos; si el pedido se recalcula con otros costos, se reescribe para que siempre describa el pedido vigente. En contenedores sin `.git` el SHA viene de `LABORATORIO_GIT_SHA`. |
| 2026-11-02 | Los costos del pedido salen del archivo que indica `pedido.costos` en `pipeline.yaml` (por defecto `costs.yaml`); el código no tiene el nombre escrito. Los costos entran a la firma de insumos del pedido, así que cambiarlos genera un pedido nuevo. | Probado en `tests/test_publicacion.py`: otro archivo de costos => otro pedido (más faltante caro => más pedido, más sobrante caro => menos) sin tocar código, y el mismo contenido en otro formato no cambia nada. |
| 2026-11-02 | Reproducibilidad: `python -m laboratorio.publicacion.reproducir <fecha>` crea un `git worktree` en el SHA del manifiesto, hace `dvc checkout` (dato crudo y modelo de esa versión, desde la caché de DVC), regenera capas, features, pronóstico, pedido y manifiesto desde cero y exige que el manifiesto sea idéntico. Se niega si el original se generó con el árbol sucio. | El repositorio actual no se toca. Comparar el manifiesto completo cubre código, datos, modelo, configuración y el contenido de los resultados. El CI prueba lo mismo con datos de ejemplo (dos entornos independientes dan resultados idénticos); la corrida con DVC es local porque el CI no usa `dvc pull`. |
| 2026-11-02 | `pronostico.modelo` pasa a `lightgbm_cuantiles` (campeón). El stage `entrenar` depende solo del código que afecta al modelo y de los parámetros que usa (`pronostico.horizonte_dias`, `anticipacion_dias`); publicar o cambiar `pronostico.modelo` no reentrena. Las pruebas fuerzan la línea base porque `models/` no está en Git. | Evita reentrenar (4 min) por cambios ajenos al modelo. |

| 2026-11-02 | **El modelo se entrena as-of la carga histórica** (`modelado.fecha_entrenamiento: 2017-06-30`) y `pronostico.modelo: campeon` usa lo que diga `models/campeon`. Si no existe, la primera tarea `pronosticar` entrena el inicial. Antes se entrenaba con datos hasta 2017-08-21 y se usaba para pronosticar julio-agosto: fuga de información al pronosticar. | Con la fecha en la carga histórica, todo pronóstico del flujo es posterior al entrenamiento y no hay fuga; además el modelo depende solo de (código, zip, config), así que se puede entrenar antes del flujo (`dvc repro`) o dentro de él con el mismo resultado. Se corrigió al documentar la disponibilidad de la información. Backtest nuevo (lunes 2017-05-29 a 06-19): WAPE 0.146 → 0.126, costo −16.5 %, fill rate 0.974, cobertura 0.81; versión `907b92c85a`. Alternativa descartada: reentrenar cada semana as-of el pronóstico (mejor frescura, ~2 min por semana y el campeón dejaría de ser una sola versión de DVC); queda como evolución. |
| 2026-11-02 | Un solo comando (`./iniciar.sh`) levanta Airflow y el servicio `airflow-orquestador` activa los DAGs y crea un backfill por DAG para todo el periodo, con `--reprocess-behavior failed` (repetirlo solo reintenta lo fallido). | Los DAGs nacen en pausa (para no correr nada al azar) y `catchup=False`, así que sin esto habría que activar y lanzar cuatro backfills a mano. Probado: sintaxis de los scripts y `docker compose config`; **no** se levantó el stack completo (memoria de la máquina de desarrollo). |

## Disponibilidad de la información

**Decisión.** Cada fuente llega con su retraso (ventas D+1, transacciones D+2, petróleo D+3; el petróleo no publica fines de semana).
Cada fila lleva `available_at` (el día en que el sistema la conoció) y todo cálculo de un corte X lee con `load_as_of(fuente, X)`.
El pronóstico del lunes X cubre X+1 … X+7 con ventas conocidas hasta X−1, transacciones hasta X−2 y el último precio de petróleo
conocido. El calendario de feriados y las tiendas son de referencia y se conocen completos (los feriados se publican por adelantado).
La carga histórica usa `available_at = fecha + retraso`, como si hubiera llegado a tiempo. El entrenamiento purga 11 días
(7 de horizonte + 1 de anticipación + 1 de retraso de ventas + 3 de la corrección `_rev2`) para que toda etiqueta se conociera el día de la prueba.
El modelo se entrena con lo conocido al inicio del flujo (ver fila del 2026-11-02).

| Alternativa | Por qué no |
|---|---|
| Leer los CSV completos y filtrar por la columna `date` | Ignora cuándo se supo cada dato: una corrección que llegó 3 días después aparecería como si siempre hubiera estado. Es la fuga más común. |
| Esperar a que las tres fuentes estén completas antes de pronosticar | Retrasaría el pedido hasta D+3 por el petróleo, la fuente que menos pesa. Se pronostica con lo último conocido y `dias_desde_ultimo_dato` queda como variable. |
| Entrenar con todo el periodo y probar en el final | Fuga: el modelo vería días posteriores al corte. Se usa rolling origin con purga. |

**Riesgos conocidos.** (1) Si una fuente no llega o queda en cuarentena, el flujo sigue con el dato anterior y no se bloquea ni se avisa más
que por la variable de antigüedad. Alternativa: una compuerta de frescura que falle el DAG si ventas tiene más de N días de atraso.
(2) `anticipacion_dias: 1` supone que se pide el lunes para cubrir desde el martes: no hay plazo de entrega real ni horario de corte.
(3) Los retrasos son simulados con los de `config/pipeline.yaml`; en producción deben medirse y ajustarse.

## Validación

**Decisión.** Cada revisión de bronze se valida con pandera antes de entrar a silver, con esquemas armados desde `config/pipeline.yaml`
(no en el código): columnas exactas, tipos, mínimos y máximos, nulos solo donde se permiten, claves únicas, `date` igual a la
partición y no posterior a `available_at`, tiendas existentes, archivo no vacío y una sola revisión por archivo. Lo que falla va a
`cuarentena` con los motivos y **silver conserva la última revisión válida**: un archivo malo no pisa un dato bueno. Se reportan todos
los errores a la vez (`lazy=True`).

| Alternativa | Por qué no |
|---|---|
| Fallar el DAG ante cualquier archivo inválido | Un solo archivo malo detendría todo el periodo; con `depends_on_past` bloquearía todos los días siguientes. |
| Corregir automáticamente (recortar negativos, rellenar nulos) | Inventa datos sin que nadie lo sepa. Se prefiere aislar y que la fuente corrija. |
| Great Expectations / Soda | Más herramientas y otro lugar donde definir reglas; pandera cubre los casos con poca infraestructura. |

**Lo que la validación NO detecta** (se prefiere decirlo): un archivo que pasa los rangos pero viene incompleto (hoy basta con que
tenga al menos una fila, no se exigen las 1 782 combinaciones tienda-familia de un día), valores plausibles pero erróneos (la
simulación inyecta justo eso: factores de 0.5–0.8 o 1.2–1.5 en el 5 % de las filas de ciertas fechas, que pasan los rangos y solo
se corrigen cuando llega el `_rev2`), valores atípicos frente a la historia, ni la consistencia entre fuentes (ventas vs. transacciones).
Alternativas, de menor a mayor costo: verificar completitud por día contra el calendario de tiendas y familias; comparar cada día con
su media móvil y mandar a revisión si se desvía mucho; conciliar ventas con transacciones.

## Manejo de correcciones

**Decisión.** Una corrección es una revisión nueva (`rev=2`) con su propio `available_at`; nunca se modifica una revisión escrita.
`load_as_of` devuelve, por fecha, la última revisión conocida al corte, así que se puede reconstruir lo que se sabía en cualquier día.
Las capas derivadas guardan un linaje (qué revisión de cada fecha usaron) y solo escriben una revisión nueva si cambió esa firma.
Consecuencias: (a) los **features y el pronóstico ya publicados no se recalculan** cuando llega una corrección posterior, porque
representan lo que se sabía ese día y sobre eso se tomó la decisión; (b) el **seguimiento sí se recalcula** con el dato corregido,
porque mide contra la mejor verdad disponible; (c) los datos corregidos entran en los cortes posteriores y en el reentrenamiento.

| Alternativa | Por qué no |
|---|---|
| Sobrescribir el dato en su lugar | Se pierde qué se sabía cuándo; el backtest y la auditoría dejan de ser fieles y se rompe la reproducibilidad. |
| Recalcular y reemitir los pronósticos y pedidos pasados al llegar cada corrección | Cambia pedidos que ya se ejecutaron y mezcla la decisión con información que no existía. Solo tendría sentido para un pedido todavía no emitido. |
| Ignorar las correcciones | El seguimiento y el reentrenamiento medirían contra datos erróneos. |

**Límites.** La purga de 11 días supone que una corrección llega como máximo 3 días después (`revisiones.retraso_extra_dias`); una
corrección más tardía se ingesta igual, pero el entrenamiento podría haber usado antes el valor original. Recalcular un día pasado con
otra configuración cuando ya hay revisiones posteriores falla a propósito (`RevisionFueraDeOrden`): hay que borrar la capa derivada y
repetir el backfill desde el inicio, para no dejar un histórico mezclado. Cambiar `costs.yaml` afecta al siguiente pedido; los ya emitidos conservan los costos con que se calcularon (y su manifiesto).

## Supuesto sobre inventario

**Supuesto.** Los datos de Kaggle **no incluyen inventario**, ni pedidos en tránsito, plazos de entrega, vida útil o capacidad.
Por eso el pedido es un problema de una sola decisión por tienda-familia (vendedor de periódicos): se supone **inventario inicial cero**,
reposición instantánea al inicio del horizonte (7 días), sin pedidos pendientes y sin arrastre de sobrantes a la semana siguiente. La
cantidad es el cuantil `costo_faltante / (costo_faltante + costo_sobrante)` de la demanda de la semana (normal con media = suma de
medianas y sigma = raíz de la suma de varianzas diarias del modelo). Los costos de `config/costs.yaml` (perecibles con sobrante más
caro) son **valores de referencia**, no del negocio.

Implicaciones para quien use la recomendación:
- `cantidad_pedido` es una **necesidad bruta**: la orden de compra es `max(0, cantidad_pedido − inventario disponible − en tránsito)`. Esa resta no la hace el sistema porque no tiene el dato.
- Se usa **venta como demanda**. Si hubo quiebres de stock, la venta es menor que la demanda (demanda censurada) y el modelo la subestima; los ceros de ventas pueden ser falta de producto y no falta de demanda.
- El fill rate y el costo del backtest se calculan con el mismo supuesto (pedido semanal, inventario cero), no representan un nivel de servicio diario real.

| Alternativa | Qué exigiría | Por qué no se hizo |
|---|---|---|
| Política (s, S) o de revisión periódica con inventario simulado | Inventario real, plazos de entrega, frecuencia de pedido | Sin datos habría que inventarlos; los resultados parecerían más reales de lo que son. |
| Estimar demanda censurada (p. ej. tratar días de ceros en series con ventas altas como faltantes, o un modelo tobit) | Saber cuándo hubo quiebre | Sin dato de stock no se distingue "sin demanda" de "sin producto". |
| Aceptar una fuente `inventario` en la bandeja y descontarla | Esquema, validación, retraso y revisiones propios | Es el siguiente paso natural y encaja en la arquitectura (otra fuente as-of); se deja como evolución. |

## Supuesto sobre promociones

**Supuesto.** `onpromotion` (cantidad de productos en promoción de esa tienda y familia ese día) solo se conoce para días ya vendidos:
viene en las filas de ventas. El zip del flujo no incluye `test.csv` (donde Kaggle publica las promociones de los días a pronosticar) y
el simulador solo entrega los campos de `train.csv`. Por eso el modelo usa únicamente `promo_28`, la proporción histórica de promoción de
los últimos 28 días, y **no** la promoción del día objetivo.

Implicación: el modelo no reacciona a una promoción planificada para la semana que se pronostica; en semanas con promociones fuertes se
espera subestimación, y en semanas sin promociones, una ligera sobreestimación si la ventana tuvo muchas. No se midió cuánto.

| Alternativa | Por qué no / cuándo sí |
|---|---|
| Usar `onpromotion` del día objetivo como variable | En el backtest sería fuga: ese valor no se conocía al corte en este flujo. Solo es válido si el plan de promociones se **publica por adelantado**. |
| Fuente nueva `promociones_planeadas`, ingerida como los feriados (con su `available_at` = fecha de publicación) | Es lo correcto si el negocio planifica promociones con anticipación; requiere ese dato, que hoy no existe en el flujo. Es la evolución recomendada. |
| Multiplicador de uplift manual sobre el pedido | Introduce intervención manual sin base en datos; mejor modelar el efecto con la fuente anterior. |
| Escenarios (con y sin promoción) | Duplica salidas sin saber cuál ocurre; útil solo cuando el plan existe. |

## Qué queda fuera, a propósito

Reentrenamiento programado en Airflow (hoy: `dvc repro` bajo demanda, con la regla de reemplazo ya implementada y probada);
compuerta de frescura y de completitud de datos; inventario real; plan de promociones; costos reales. Todo lo anterior está
descrito arriba con su alternativa y deja la arquitectura lista para agregarlo como una fuente o una tarea más.
