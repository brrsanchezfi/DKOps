---
hide:
  - navigation
  - toc
---

<div class="dk-hero" markdown>

<span class="dk-hero__eyebrow">Spark · Delta Lake · Databricks</span>

# Pipelines de datos gobernados por contratos

<p class="dk-hero__text">
DKOps es un framework en Python para construir lakehouses Delta con la arquitectura
Medallion. Describes tus tablas y tus cargas en JSON, y el framework se encarga de
crearlas, validarlas y mantenerlas. El mismo código corre en tu equipo y en Databricks.
</p>

[Empezar](getting-started/index.md){ .md-button .md-button--primary }
[Ver en GitHub](https://github.com/brrsanchezfi/DKOps){ .md-button }

</div>

<div class="dk-layers">
  <div class="dk-layer">
    <div class="dk-layer__step">Capa 1</div>
    <div class="dk-layer__name">Landing</div>
    <div class="dk-layer__desc">Archivos crudos en JSON, CSV o Parquet, o mensajes de Kafka.</div>
  </div>
  <div class="dk-layer">
    <div class="dk-layer__step">Capa 2</div>
    <div class="dk-layer__name">Bronze</div>
    <div class="dk-layer__desc">Datos tal como llegaron, con columnas de trazabilidad.</div>
  </div>
  <div class="dk-layer">
    <div class="dk-layer__step">Capa 3</div>
    <div class="dk-layer__name">Silver</div>
    <div class="dk-layer__desc">Estado actual, limpio y sin duplicados por clave de negocio.</div>
  </div>
  <div class="dk-layer">
    <div class="dk-layer__step">Capa 4</div>
    <div class="dk-layer__name">Gold</div>
    <div class="dk-layer__desc">Agregados y métricas listos para BI.</div>
  </div>
</div>

## Qué resuelve

<div class="grid cards" markdown>

-   :material-file-document-outline:{ .lg .middle } **Contratos versionados**

    ---

    El schema, las particiones, los permisos y las propiedades de cada tabla viven
    en JSON junto al código, no repartidos entre notebooks.

    [:octicons-arrow-right-24: Contratos](concepts/contracts.md)

-   :material-transit-connection-variant:{ .lg .middle } **Ingesta declarativa**

    ---

    Cuatro estrategias de promoción a Silver que eliges desde el contrato:
    `full_merge`, `cdc_merge`, `incremental_replace` y `append_dedup`.

    [:octicons-arrow-right-24: Ingesta](ingestion/index.md)

-   :material-table-edit:{ .lg .middle } **Escritura y lectura gobernadas**

    ---

    `TableWriter` y `TableReader` validan contra el contrato antes de tocar la tabla,
    y aplican comentarios, máscaras y permisos por ti.

    [:octicons-arrow-right-24: Gobierno de tablas](governance/index.md)

-   :material-laptop:{ .lg .middle } **Local y Databricks**

    ---

    El framework detecta dónde corre y resuelve catálogos y rutas desde
    `config.json`. No hay ramas `if databricks` en tu pipeline.

    [:octicons-arrow-right-24: Runtime](concepts/runtime.md)

-   :material-source-branch-sync:{ .lg .middle } **Migraciones seguras**

    ---

    `SafeMigrator` compara el contrato con la tabla real y genera solo los cambios
    que no pierden datos.

    [:octicons-arrow-right-24: Migraciones](governance/migrations.md)

-   :material-chart-timeline-variant:{ .lg .middle } **Trazabilidad operativa**

    ---

    Cada ejecución queda registrada en una tabla Delta que puedes consultar con SQL
    para auditoría y monitoreo.

    [:octicons-arrow-right-24: Operación](operations/index.md)

</div>

## Un pipeline completo en pocas líneas

```python
from DKOps.launcher import Launcher
from DKOps.ingestion.engine import IngestionEngine

Launcher("config/config.json")

engine = IngestionEngine.from_launcher(
    bronze_contracts_dir = "ingestion/batch",
    silver_contracts_dir = "ingestion/silver",
    tables_base_dir      = ".",
    ops_path             = "/tmp/ops/control",
)

engine.ingest_bronze()     # de Landing a Bronze
engine.promote_silver()    # de Bronze a Silver
engine.status()
```

Toda la lógica de qué leer, dónde escribir y cómo fusionar está en los contratos JSON.
Si quieres ver cómo se arma un proyecto desde cero, sigue la guía de
[primeros pasos](getting-started/index.md) o abre alguno de los [demos](demos/index.md).
