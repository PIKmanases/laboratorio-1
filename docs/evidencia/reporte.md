# Reporte de funcionamiento y desempeño

Generado con el código `fc6b929dc5`; datos al 2017-07-11.

## 1. Ejecuciones en Airflow

| DAG | Corridas | Exitosas | Fallidas | Fechas lógicas | Duración media |
|---|---|---|---|---|---|
| `ingesta_validacion` | 12 | 12 | 0 | 2017-06-30 a 2017-07-11 | 192 s |
| `preparacion_features` | 12 | 12 | 0 | 2017-06-30 a 2017-07-11 | 115 s |
| `pronostico_pedido` | 2 | 2 | 0 | 2017-07-03 a 2017-07-10 | 522 s |
| `seguimiento_desempeno` | 12 | 12 | 0 | 2017-06-30 a 2017-07-11 | 106 s |

## 2. Comparación de modelos

### 2.1 Backtest temporal (decisión de reemplazo)

Backtest temporal (rolling origin), cortes de prueba: 2017-05-29, 2017-06-05, 2017-06-12, 2017-06-19.

| Modelo | WAPE | Sesgo | RMSLE | Fill rate | Costo | Costo/unidad | Cobertura p10–p90 |
|---|---|---|---|---|---|---|---|
| baseline (campeón anterior) | 0.1459 | +0.0038 | 0.4545 | 0.9804 | 1,204,755 | 0.0493 | – |
| lightgbm_cuantiles (candidato) | 0.1264 | -0.0194 | 0.3986 | 0.9742 | 1,005,785 | 0.0412 | 0.811 |

Costo por corte de prueba (menor es mejor):

| Corte | Costo campeón anterior | Costo candidato | Gana |
|---|---|---|---|
| 2017-05-29 | 391,457 | 309,701 | candidato |
| 2017-06-05 | 239,494 | 279,918 | anterior |
| 2017-06-12 | 284,469 | 201,487 | candidato |
| 2017-06-19 | 289,335 | 214,679 | candidato |

Regla de reemplazo (`forecast/reemplazo.py`):

| Condición | Cumple | Detalle |
|---|---|---|
| costo | sí | mejora 16.52% (mínimo 2.00%) |
| wape | sí | candidato 0.1264 vs máximo 0.1459 |
| estabilidad | sí | gana 3 de 4 folds (mínimo 3) |
| fill_rate | sí | 0.9742 (mínimo 0.85) |
| sesgo | sí | -0.0194 (máximo ±0.1) |
| cobertura | sí | 0.811 (rango 0.7-0.9) |

**Decisión:** reemplazar — el candidato cumple todas las condiciones.

**Campeón vigente:** `lightgbm_cuantiles`, versión `907b92c85a`, entrenado con datos hasta 2017-06-30, cortes de entrenamiento ['2016-06-27', '2017-06-19'], 646,866 filas.

### 2.2 En vivo: campeón vs. línea base en el flujo

Los mismos pronósticos del flujo (semana completa observada), con el modelo vigente y con la línea base, ambos con la misma regla de pedido y los mismos costos.

| Corte | Modelo | WAPE | Sesgo | Fill rate | Costo |
|---|---|---|---|---|---|
| 2017-07-03 | lightgbm_cuantiles | 0.1053 | -0.0144 | 0.9744 | 256,083 |
| 2017-07-03 | media_dia_semana (línea base) | 0.1214 | -0.0162 | 0.9801 | 263,670 |

**Agregado**

| Modelo | WAPE | Sesgo | Fill rate | Costo |
|---|---|---|---|---|
| **lightgbm_cuantiles** | 0.1053 | -0.0144 | 0.9744 | 256,083 |
| **media_dia_semana (línea base)** | 0.1214 | -0.0162 | 0.9801 | 263,670 |

## 3. Seguimiento del desempeño

Desempeño vigente al 2017-07-11 (umbral de alerta: WAPE > 0.35). `Días obs.` = días del horizonte ya vendidos; el costo y el fill rate solo existen con el horizonte completo. `Rev.` = veces que se recalculó el desempeño de ese pronóstico: una por cada día nuevo observado y otra por cada corrección de datos (`_rev2`).

| Corte | Días obs. | WAPE | Sesgo | RMSLE | Fill rate | Costo | Estado | Rev. |
|---|---|---|---|---|---|---|---|---|
| 2017-07-03 | 7/7 | 0.1053 | -0.0144 | 0.3783 | 0.9744 | 256,083 | ok | 7 |

Pronósticos con la semana completa (1): WAPE agregado **0.1053**, sesgo agregado **-0.0144**.

Pronóstico del 2017-07-03, por familia: las 5 mejores y las 5 peores por WAPE.

**Mejores**

| Familia | Venta real | WAPE | Sesgo | Fill rate | Costo |
|---|---|---|---|---|---|
| DAIRY | 348,547 | 0.072 | +0.009 | 0.990 | 9,811 |
| GROCERY I | 1,772,488 | 0.077 | +0.014 | 0.996 | 38,027 |
| PRODUCE | 923,867 | 0.081 | +0.001 | 0.981 | 38,175 |
| BREAD/BAKERY | 213,726 | 0.093 | -0.013 | 0.971 | 10,101 |
| POULTRY | 153,051 | 0.095 | -0.019 | 0.978 | 5,221 |

**Peores**

| Familia | Venta real | WAPE | Sesgo | Fill rate | Costo |
|---|---|---|---|---|---|
| BOOKS | 7 | 1.128 | -0.850 | 0.644 | 4 |
| HOME APPLIANCES | 192 | 1.032 | -0.388 | 0.604 | 96 |
| BABY CARE | 73 | 1.030 | -0.667 | 0.522 | 39 |
| HARDWARE | 574 | 0.650 | -0.287 | 0.837 | 114 |
| SCHOOL AND OFFICE SUPPLIES | 557 | 0.564 | -0.249 | 0.867 | 95 |

## 4. Trazabilidad de cada pronóstico

| Corte | SHA Git | Modelo | Versión | md5 datos | Hash config | Hash pronóstico |
|---|---|---|---|---|---|---|
| 2017-07-03 | f4612460da | lightgbm_cuantiles | 907b92c85a | b5cf65e315 | ed0201570c | 1d00130978 |
| 2017-07-10 | f4612460da | lightgbm_cuantiles | 907b92c85a | b5cf65e315 | ed0201570c | 6c2ce7311c |
