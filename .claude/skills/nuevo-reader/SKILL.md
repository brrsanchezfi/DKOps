---
name: nuevo-reader
description: Añade un lector de fuentes al módulo de ingesta de DKOps (JDBC, API REST, Event Hubs, formato de archivo nuevo). Usar al pedir "leer desde X", "soportar una fuente nueva", "conectar una base de datos al pipeline", o al tocar SourceReaderFactory y los readers de ingestion/readers/.
---

# Nuevo reader de fuente

## 0. Descartar primero

Los readers existentes cubren más de lo que parece. `LocalBatchReader`,
`FileStreamReader` y `AutoLoaderReader` delegan en `spark.read.format(...)`, así
que **un formato de archivo nuevo (avro, orc, xml…) normalmente no necesita
reader**: basta poner `source.format` en el contrato y las opciones de Spark en
`source.options`.

Un reader nuevo se justifica cuando la fuente **no es un path que Spark lea
directamente** (JDBC, una API REST, un SDK propietario) o cuando requiere
autenticación y configuración propia, como hace `KafkaReader`.

## 1. Entender el factory

`SourceReaderFactory.create()` (`readers/factory.py`) decide qué reader se
instancia. Su orden de decisión es:

1. `source.format == "kafka"` → `KafkaReader`, en cualquier entorno.
2. streaming → `AutoLoaderReader` (Databricks) o `FileStreamReader` (local).
3. batch → `AutoLoaderReader` (Databricks, formato ≠ delta) o `LocalBatchReader`.

La matriz completa entorno × modo está documentada en el docstring del módulo.
**Mantenla actualizada** al añadir un reader: es la referencia que se lee para
entender qué corre dónde.

Los readers de Databricks se importan **dentro** de cada rama, no arriba del
módulo. Eso permite importar el factory en un PC local sin Databricks instalado.
Respeta ese patrón: import diferido dentro de la rama que instancia tu reader.

## 2. Implementar

Hereda de `BaseSourceReader` (`readers/base.py`) e implementa `read()`.

- Recibe el `IngestionContract` en `__init__` y guárdalo con
  `super().__init__(contract)` — la base expone `self.contract`,
  `self.is_streaming` y `self.source_format`.
- Devuelve un DataFrame batch o de streaming según `contract.is_streaming()`. Si
  tu fuente solo soporta uno de los dos modos, falla con un mensaje explícito
  cuando el contrato pide el otro.
- Toda configuración sale del contrato: `contract.source.path`,
  `contract.source.options`, `contract.source.schema`. **No leer variables de
  entorno ni hardcodear rutas** — rompe la promesa de que el contrato describe la
  ingesta por completo.
- Credenciales: no van en el contrato. Se inyectan por constructor, como hace
  `KafkaReader` con `kafka_creds`. Si tu reader las necesita, añade el parámetro
  a `SourceReaderFactory.create()` y pásalo desde ahí.
- Logging con `self.log` (viene de `LoggableMixin`), no `print()`.
- El reader **solo lee**. No enriquece metadatos (eso es `MetadataEnricher`) ni
  escribe (eso es `BronzeIngestor`).

## 3. Registrar

1. `src/DKOps/ingestion/readers/<nombre>.py` — la implementación.
2. `readers/factory.py` — la rama de selección, con import diferido, y la matriz
   de compatibilidad del docstring actualizada.
3. `readers/__init__.py` — exportar la clase.
4. `schema/ingestion_contract.schema.json` — si el reader se activa por un valor
   nuevo de `source.format`, añádelo a los `examples` de ese campo y documenta en
   la descripción las opciones que espera.

## 4. Probar

`tests/ingestion/test_reader_factory.py` verifica la selección de reader por
entorno y modo. Añade ahí los casos de tu reader: batch y streaming, local y
Databricks (el entorno se simula con `env._is_databricks`).

Para el reader en sí, prueba `read()` contra una fuente pequeña de fixture. Si
depende de un servicio externo, mockea el cliente — los tests no deben requerir
red.

```bash
python -m pytest tests/ingestion -q
```
