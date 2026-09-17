# Arquitectura

<p class="dk-lead">
DKOps implementa la arquitectura Medallion con un motor que separa la configuración del
comportamiento: los contratos dicen qué hacer y el framework decide cómo hacerlo en el
runtime donde esté corriendo.
</p>

## Vista general

```mermaid
flowchart TB
    subgraph src["Fuentes externas"]
        F1["JSON / CSV\nParquet / Avro"]
        F2["Kafka / Event Hubs"]
    end

    subgraph landing["Landing"]
        L["Archivos crudos depositados\npor Data Factory, Kafka Connect, FTP"]
    end

    subgraph bronze["Bronze: datos crudos con trazabilidad"]
        B1["ventas_raw\n_ingested_at, _ingested_date\n_source_file"]
        B2["clientes_raw\nop_type I/U/D"]
        B3["eventos_raw\nstreaming"]
    end

    subgraph silver["Silver: estado actual"]
        S1["ventas_current\ncdc_merge"]
        S2["clientes_current\nfull_merge"]
        S3["eventos_current\nappend_dedup"]
    end

    subgraph gold["Gold: KPIs y agregados"]
        G1["revenue_diario"]
        G2["engagement_clientes"]
    end

    F1 --> L
    F2 --> L
    L -->|"ingest_bronze()"| B1
    L --> B2
    L -->|"run_streaming()"| B3
    B1 -->|cdc_merge| S1
    B2 -->|full_merge| S2
    B3 -->|append_dedup| S3
    S1 & S2 & S3 -->|"SQL + TableWriter"| G1 & G2
```

## Las cuatro capas

| Capa | Qué contiene | Quién escribe | Cómo se mantiene idempotente |
|---|---|---|---|
| Landing | Archivos tal como los deja el sistema de origen | Procesos externos | No aplica |
| Bronze | Los mismos datos más columnas técnicas de ingesta | `BronzeIngestor` | Sobrescribe la partición `_ingested_date` del día |
| Silver | Última versión de cada entidad, deduplicada por clave | `SilverPromoter` | Upsert por `merge_keys` |
| Gold | Métricas y agregados para consumo | Tu SQL con `TableWriter` | Normalmente `overwrite` |

## Tres ideas que atraviesan el framework

**Los contratos son la fuente de verdad.** Ni el nombre de una tabla ni su schema se
escriben en el código Python. Todo sale de un JSON que se revisa en un pull request como
cualquier otro cambio.

**El pipeline no sabe dónde corre.** `Launcher` detecta el runtime una sola vez y el
resto de componentes lo consulta con `Launcher.current()`. Por eso los writers y readers
solo reciben el contrato.

**Repetir una ejecución no rompe nada.** Cada capa tiene un mecanismo de escritura que
produce el mismo resultado si se lanza dos veces con los mismos datos.

## En esta sección

- [Módulos](modules.md): qué hace cada paquete y cómo se relacionan.
- [Contratos](contracts.md): la diferencia entre contratos de tabla y de ingesta.
- [Flujos internos](flows.md): qué ocurre paso a paso en una escritura y en una promoción.
- [Local y Databricks](runtime.md): cómo se resuelven catálogos, rutas y readers.
