# Arquitectura

## Diagrama de paquetes

![Paquetes DKOps](../assets/packages_DKOps.png)

## Diagrama de clases

![Clases DKOps](../assets/classes_DKOps.png)

## Descripción de componentes

### Core

| Módulo | Responsabilidad |
|---|---|
| `Launcher` | Punto de entrada único. Detecta el runtime (local / Databricks), crea la `SparkSession` y se registra como singleton. |
| `EnvironmentConfig` | Resuelve placeholders `{catalog.bronze}`, `{path.raw}` según el workspace activo. |
| `LoggerConfig` | Logging estructurado con `loguru`. Mixin `LoggableMixin` para inyectar `self.log` en cualquier clase. |

### table_governance

```
table_governance/
├── contracts/
│   ├── loader.py     # JSON → TableContract / ColumnContract (inmutable, frozen dataclass)
│   └── validator.py  # valida tipos y nullabilidad de un DataFrame contra el contrato
├── writers/
│   ├── table_writer.py     # ★ fachada pública de escritura
│   ├── base_writer.py      # bridge local ↔ Databricks + merge_schema + masks + tblproperties
│   ├── create_writer.py    # CREATE OR REPLACE TABLE + SET MASK + TBLPROPERTIES
│   ├── append_writer.py    # INSERT INTO (soporta mergeSchema)
│   ├── upsert_writer.py    # MERGE INTO (SCD1)
│   ├── partition_writer.py # overwrite_partition (soporta mergeSchema)
│   └── delete_writer.py    # DELETE WHERE
├── readers/
│   └── table_reader.py     # ★ fachada pública de lectura (read/partition/stream/cdf)
└── migrations/
    └── safe_migrator.py    # compara contrato vs estado real → plan de ALTER TABLE
```

### Flujo de una escritura

```
TableWriter.overwrite(df)
    └── CreateWriter(contract).write(df)
            ├── BaseWriter.__init__()           → Launcher.current() para spark/env
            ├── SchemaValidator.validate(df)    → verifica tipos y nulls
            ├── BaseWriter._write_df(df, "overwrite", overwrite_schema=True)
            │       ├── DataFrameWriter.format("delta").mode("overwrite")
            │       └── .option("overwriteSchema","true")
            └── post-write
                    ├── _apply_tblproperties()   → ALTER TABLE SET TBLPROPERTIES
                    ├── _apply_table_comment()   → COMMENT ON TABLE
                    ├── _apply_column_comments() → ALTER COLUMN COMMENT
                    ├── _apply_column_masks()    → SET MASK (solo Databricks)
                    └── _apply_permissions()     → GRANT (solo Databricks)
```

### Flujo de una lectura

```
TableReader.read(filter=..., columns=..., limit=...)
    └── TableReader.__init__()         → Launcher.current() para spark/env
            ├── spark.read.table(name) → DataFrame nativo PySpark
            ├── df.filter(predicate)   → si filter != None
            ├── df.select(*columns)    → si columns != None
            └── df.limit(n)            → si limit != None

TableReader.read_cdf(starting_version=N)
    └── spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", N)
            .table(name)               → DataFrame con columnas _change_type, _commit_version, _commit_timestamp
```

### Singleton Launcher

Todos los writers, readers y el `SafeMigrator` llaman `Launcher.current()` en lugar de recibir `spark` / `env` como parámetros. Esto mantiene la API mínima:

```python
launcher = Launcher("config/config.json")   # una sola vez al inicio del pipeline

TableWriter(contract).overwrite(df)         # ← sin spark, sin env
TableReader(contract).read()                # ← sin spark, sin env
SafeMigrator(contract).apply()             # ← sin spark, sin env
```

El Launcher se instancia **una vez** al inicio del pipeline y queda disponible para el resto del proceso via `Launcher.current()`.
