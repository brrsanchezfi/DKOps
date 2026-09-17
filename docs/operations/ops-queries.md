# Consultas de monitoreo

Consultas SQL para sacar partido a la tabla de control. En los ejemplos la tabla se llama
`ops`; sustitúyelo por el nombre o la ruta que uses.

Todas filtran por `status`, porque cada ejecución genera una fila de inicio y otra de
cierre.

## Tasa de éxito por dataset

```sql
SELECT dataset,
       count_if(status = 'SUCCESS')                    AS ok,
       count_if(status = 'FAILED')                     AS fallidas,
       round(100.0 * count_if(status = 'SUCCESS')
             / nullif(count_if(status <> 'STARTED'), 0), 1) AS pct_exito
FROM   ops
GROUP  BY dataset
ORDER  BY pct_exito
```

## Duración media y volumen

La fila de cierre lleva su propio `started_at`, así que no hace falta un join:

```sql
SELECT dataset,
       round(avg(unix_timestamp(finished_at)
                 - unix_timestamp(started_at)), 1) AS segundos,
       sum(rows_written)                           AS filas
FROM   ops
WHERE  status = 'SUCCESS'
GROUP  BY dataset
```

## Últimos fallos y su causa

```sql
SELECT finished_at, dataset, notes
FROM   ops
WHERE  status = 'FAILED'
ORDER  BY finished_at DESC
LIMIT  10
```

## Ejecuciones que nunca se cerraron

Filas `STARTED` sin su `SUCCESS` o `FAILED` correspondiente. Suelen indicar que el proceso
murió a mitad de camino:

```sql
SELECT s.run_id, s.dataset, s.started_at
FROM        ops s
LEFT JOIN   ops c
       ON   c.run_id = s.run_id AND c.status <> 'STARTED'
WHERE       s.status = 'STARTED' AND c.run_id IS NULL
```

## Desde PySpark

Si prefieres no escribir SQL, puedes partir del DataFrame que devuelve el engine:

```python
from pyspark.sql import functions as F

(engine.ops.read()
    .filter(F.col("status") == "FAILED")
    .orderBy(F.col("finished_at").desc())
    .select("finished_at", "dataset", "notes")
    .show(10, truncate=False))
```
