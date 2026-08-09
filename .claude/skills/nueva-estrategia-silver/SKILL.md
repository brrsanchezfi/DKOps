---
name: nueva-estrategia-silver
description: Añade una estrategia de promoción Bronze → Silver al módulo de ingesta de DKOps (SCD2, upsert particionado, etc.). Usar cuando las cuatro estrategias existentes (full_merge, cdc_merge, incremental_replace, append_dedup) no cubren un patrón de carga, o al pedir "nueva estrategia de promoción", "estrategia de merge personalizada" o "cómo extender SilverPromoter".
---

# Nueva estrategia de promoción Silver

## 0. Descartar primero

Una estrategia nueva es la última opción. Antes de crearla, comprueba que el
caso no se resuelve con las existentes en `src/DKOps/ingestion/strategies/`:

| Estrategia | Patrón que cubre |
|---|---|
| `full_merge` | SCD1 — dedup por clave con watermark, upsert del más reciente |
| `cdc_merge` | eventos I/U/D con soft delete (`is_deleted`) |
| `incremental_replace` | snapshot que reemplaza una partición completa |
| `append_dedup` | solo inserciones, anti-join contra el destino |

Muchos casos "nuevos" son en realidad una de estas cuatro con distinto
`merge_keys`, `watermark_col` o `filter` en el contrato. Ese ajuste es
preferible: es configuración, no código.

## 1. Los cinco puntos de cambio

Una estrategia nueva toca exactamente estos archivos. Omitir cualquiera de los
tres primeros hace que la estrategia sea inalcanzable en runtime.

1. **`src/DKOps/ingestion/contracts/ingestion_contract.py`** — añadir el miembro
   al enum `SilverStrategy`. El valor del enum es el string que va en el JSON.
2. **`src/DKOps/ingestion/strategies/<nombre>.py`** — la implementación.
3. **`src/DKOps/ingestion/silver_promoter.py`** — registrar la clase en el
   `strategy_map` de `_build_strategy()`.
4. **`src/DKOps/ingestion/strategies/__init__.py`** — exportar la clase en
   `__all__`.
5. **`schema/ingestion_contract.schema.json`** — añadir el valor al enum
   `strategy` y documentar qué campos exige (si requiere `merge_keys` o
   `watermark_col`, añadir la condición al bloque `allOf`).

## 2. Escribir la estrategia

Hereda de `BasePromotionStrategy` (`strategies/base.py`) e implementa
`execute() -> int`, que devuelve **filas escritas en Silver**.

La base te da ya construidos:

- `self._contract` — el `IngestionContract` (`merge_keys`, `watermark_col`,
  `data_filter`, `metadata`).
- `self._src_contract` / `self._dst_contract` — los `TableContract` de Bronze y Silver.
- `self._reader` / `self._dst_reader` — `TableReader` de origen y destino.
- `self._writer` — `TableWriter` del destino (`overwrite`, `append`, `upsert`,
  `overwrite_partition`, `delete`).
- `self._read_bronze()` — lee la fuente aplicando `contract.data_filter`.
- `self._select_for_silver(df)` — recorta el DF a las columnas del contrato Silver.
- `self.log` — logger (vía `LoggableMixin`).

Usa `append_dedup.py` como plantilla: es la estrategia más corta y muestra el
esqueleto completo.

## 3. Invariantes que la estrategia debe cumplir

- **Idempotencia.** Correr `execute()` dos veces sobre los mismos datos de Bronze
  no puede duplicar filas en Silver. Es la propiedad que hace re-ejecutable todo
  el pipeline y no es negociable.
- **Primer run sin destino.** En la primera ejecución la tabla Silver no existe.
  Leer `self._dst_reader` lanza excepción — captúrala y trata todo Bronze como
  registros nuevos (ver `append_dedup.py`).
- **Timestamps antes del select.** Si `self._contract.metadata.add_silver_timestamps`
  está activo, añade la columna **antes** de llamar a `_select_for_silver()`, o
  el select la descarta.
- **Siempre `_select_for_silver()` antes de escribir.** Evita que los metadatos
  de Bronze (`_ingested_at`, `_source_file`) se filtren a Silver, donde no están
  declarados y romperían la validación de schema.
- **Devolver el conteo real**, no una estimación: alimenta el `IngestionOpsLogger`.
- **Salida temprana en 0 filas** — log y `return 0` sin escribir.

## 4. Documentar

El docstring del módulo sigue un formato fijo en las cuatro estrategias
existentes: algoritmo numerado paso a paso, y una sección "Cuándo usarlo" con
casos concretos. Mantenlo — es lo que se lee para elegir estrategia.

Añade también la fila a la tabla de estrategias en
`.claude/skills/nuevo-contrato-ingesta/SKILL.md` y en `README.md`.

## 5. Probar

Los tests de ingesta viven en `tests/ingestion/` y `conftest.py` ya provee los
fixtures de Spark. Cubre como mínimo:

- caso normal con datos en Bronze;
- **segunda ejecución idempotente** (mismo input → mismo estado final en Silver);
- primer run sin tabla Silver;
- Bronze vacío → devuelve 0 sin escribir.

```bash
python -m pytest tests/ingestion -q
```

```bash
python scripts/validate_contracts.py
```
