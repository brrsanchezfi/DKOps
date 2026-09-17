# Demos

<p class="dk-lead">
El repositorio incluye cinco proyectos completos. Cada uno genera sus propios datos,
recorre las capas de Landing a Gold y muestra un conjunto distinto de funcionalidades.
</p>

## Cómo ejecutarlos

```bash
python demos/demo_5/pipeline.py
```

Los demos escriben en `/tmp/dkops_demoN/`. Si quieres empezar desde cero, borra esa
carpeta antes de ejecutar. La primera ejecución en local tarda algo más porque Spark
descarga los JAR de Delta.

## Qué muestra cada uno

| Demo | Dominio | Estrategias en Silver | Lo más destacado |
|---|---|---|---|
| [Aeronáutica](demo_1.md) | Operaciones aéreas | No usa ingesta | Los cinco writers y `SafeMigrator` en modo simulación |
| [Manufactura](demo_2.md) | Artículos de aseo | `incremental_replace`, `cdc_merge`, `full_merge` | Reglas de calidad declarativas y transformaciones con tests |
| [E-commerce](demo_3.md) | Tienda en línea | `full_merge`, `cdc_merge`, `append_dedup` | `merge_schema`, máscaras de columna y streaming |
| [Retail e inventario](demo_4.md) | Inventario | `full_merge`, `append_dedup` | `read_cdf()`, `read_stream()` y `SafeMigrator` |
| [Marketplace](demo_5.md) | Marketplace | `cdc_merge`, `full_merge` | Capa Gold con revenue y engagement, tabla de control |

## Por dónde empezar

<div class="grid cards" markdown>

-   **Si te interesa el gobierno de tablas**

    ---

    Empieza por Aeronáutica. No usa el motor de ingesta y se centra en los writers.

    [:octicons-arrow-right-24: Demo de Aeronáutica](demo_1.md)

-   **Si quieres ver el flujo completo**

    ---

    Marketplace recorre todas las capas con CDC, snapshots y streaming.

    [:octicons-arrow-right-24: Demo de Marketplace](demo_5.md)

</div>

Cada demo sigue la misma estructura: una carpeta `datagen/` que genera los datos, los
contratos en `tables/` e `ingestion/`, y un `pipeline.py` que lo orquesta todo.
