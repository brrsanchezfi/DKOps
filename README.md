<div align="center">

# DKOps

**Framework de gobierno de tablas Delta y orquestación de pipelines Spark para entornos híbridos local ↔ Databricks.**

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/pyspark-3.5+-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/delta--lake-3.2+-00ADD4.svg)](https://delta.io/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](#-contribuir)

*El mismo código corre en tu PC y en Databricks — sin cambios.*

</div>

---

## ¿Qué es DKOps?

DKOps es un framework Python que **profesionaliza la construcción de pipelines de datos** sobre Spark + Delta Lake. Resuelve los problemas que aparecen cuando un equipo crece más allá de "scripts sueltos":

- **Contratos de tabla** — el schema, los permisos, el particionado y los metadatos viven en JSON versionado, no enterrados en código.
- **Writers gobernados** — `CreateWriter`, `AppendWriter`, `UpsertWriter`, `PartitionWriter`, `DeleteWriter`. Cada uno valida contra el contrato antes de escribir.
- **Migraciones seguras** — `SafeMigrator` compara contrato vs estado real y genera un plan de cambios sin pérdida de datos.
- **Runtime-agnóstico** — el mismo pipeline corre en local PC (Spark + Delta) y en Databricks (Connect o cluster nativo). El framework detecta el entorno y se adapta.
- **Configuración por entorno** — placeholders `{catalog.bronze}`, `{path.silver}` se resuelven contra `dev`/`prod` desde un único `config.json`.

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, CreateWriter, UpsertWriter

launcher = Launcher("config/config.json")
contract = load_contract("tables/fact_ventas.json")

CreateWriter(contract).write(df)                    # full load
UpsertWriter(contract).write(df_nuevo, merge_keys=["venta_id"])
```

---

## Tabla de contenidos

- [Arquitectura](#-arquitectura)
- [Instalación](#-instalación)
  - [Requisitos](#requisitos)
  - [Entorno local PC (`.venv-local`)](#entorno-local-pc-venv-local)
  - [Entorno Databricks Connect (`.venv-databricks`)](#entorno-databricks-connect-venv-databricks)
- [Configuración](#-configuración)
- [Quickstart](#-quickstart)
- [Demos](#-demos)
- [Build](#-build)
- [Estado del proyecto](#-estado-del-proyecto)
- [Contribuir](#-contribuir)
- [Licencia](#-licencia)

---

## 🏗️ Arquitectura

```
DKOps/
├── launcher.py                  # punto de entrada — detecta runtime y crea SparkSession
├── environment_config.py        # resuelve catalogs/paths/secrets según workspace activo
├── logger_config.py             # logging estructurado (loguru) con contexto
└── table_governance/
    ├── contracts/
    │   ├── loader.py            # carga JSON → TableContract tipado
    │   └── validator.py         # valida DataFrame contra contrato (tipos, nulls)
    ├── writers/
    │   ├── base_writer.py       # bridge local PC ↔ Databricks
    │   ├── create_writer.py     # CREATE OR REPLACE TABLE
    │   ├── append_writer.py     # INSERT INTO
    │   ├── upsert_writer.py     # MERGE INTO (SCD1)
    │   ├── partition_writer.py  # overwrite de partición específica
    │   └── delete_writer.py     # DELETE WHERE
    └── safe_migrator.py         # compara contrato vs tabla real → plan de migración
```

**Filosofía:** pasar `spark` y `env` a cada componente es ruido. El `Launcher` se auto-registra como singleton del proceso; los writers, loaders y migrator obtienen lo que necesitan vía `Launcher.current()`. La API queda mínima: `CreateWriter(contract).write(df)`.

---

## 📦 Instalación

### Requisitos

- **Python 3.10+** (3.11 recomendado)
- **Java 11 o 17** (requerido por Spark)
- **Git**

DKOps se distribuye con `pyproject.toml`. Recomendamos dos virtual environments separados — uno para correr localmente con Spark, otro para Databricks Connect — porque tienen dependencias incompatibles entre sí (PySpark vanilla vs `databricks-connect`).

### Entorno local PC (`.venv-local`)

Para desarrollo y tests en tu máquina con Spark + Delta Lake configurados desde cero.

```bash
# 1. Clonar el repo
git clone https://github.com/<TU_USER>/<NOMBRE_REPO>.git
cd <NOMBRE_REPO>

# 2. Crear el venv local
python3 -m venv .venv-local
source .venv-local/bin/activate          # Linux/Mac/WSL
# .venv-local\Scripts\activate           # Windows PowerShell

# 3. Instalar el framework + dependencias locales
pip install --upgrade pip
pip install -e ".[local]"
```

Esto instala:
- `pyspark` 3.5.x (con Delta Lake configurado vía JARs en runtime)
- `loguru` para logging estructurado
- `pytest` para tests
- DKOps en modo editable (`-e`) — los cambios al código se reflejan al instante

**Verificación:**

```bash
python -c "from DKOps.launcher import Launcher; print('OK')"
```

### Entorno Databricks Connect (`.venv-databricks`)

Para conectarte desde tu máquina a un cluster Databricks remoto. **No mezcles este venv con el local** — las versiones de PySpark son incompatibles.

```bash
# 1. Crear el venv (asegúrate de NO tener el local activo)
deactivate 2>/dev/null
python3 -m venv .venv-databricks
source .venv-databricks/bin/activate

# 2. Instalar el framework + extras de Databricks
pip install --upgrade pip
pip install -e ".[databricks]"
```

Esto instala:
- `databricks-connect` (versión que coincida con el runtime de tu cluster)
- `databricks-sdk`
- `loguru`, `pytest`
- DKOps en modo editable

**Configurar credenciales** (PAT o OAuth):

```bash
# Opción A: Personal Access Token (rápido para desarrollo)
export DATABRICKS_HOST="https://<workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="<tu-pat>"

# Opción B: OAuth via Databricks CLI (recomendado para uso prolongado)
databricks auth login
```

Luego edita tu `config.json`:

```json
{
  "EXECUTION_ENVIRONMENT": "databricks",
  "CLUSTER_ID": "<tu-cluster-id>"
}
```

**Verificación:**

```bash
python -c "from databricks.connect import DatabricksSession; \
           DatabricksSession.builder.getOrCreate().sql('SELECT 1').show()"
```

### Cuál venv activar

| Estás haciendo... | Activa |
|---|---|
| Desarrollo del framework, tests unitarios, demos en local | `.venv-local` |
| Ejecutar contra un cluster Databricks remoto desde la PC | `.venv-databricks` |
| Notebook dentro del workspace Databricks | Ninguno — usa el del cluster |

---

## ⚙️ Configuración

DKOps lee un `config.json` que define:
- El runtime (`local` o `databricks`).
- Los **environments** del proyecto (`dev`, `prod`) con sus catálogos, paths y secrets scopes.
- Configuración de logging.

Estructura mínima:

```json
{
  "EXECUTION_ENVIRONMENT": "local",
  "SPARK_APP_NAME": "miPipeline",
  "SPARK_WAREHOUSE_DIR": "/tmp/spark-warehouse",
  "DELTA_VERSION": "3.2.0",

  "environments": {
    "<workspace_id>": {
      "env": "dev",
      "env_short": "d",
      "catalogs": {
        "bronze": "bronze_dev",
        "silver": "silver_dev",
        "gold":   "gold_dev"
      },
      "paths": {
        "bronze": "abfss://bronze@<storage>.dfs.core.windows.net",
        "silver": "abfss://silver@<storage>.dfs.core.windows.net"
      }
    }
  }
}
```

DKOps busca el config en este orden:
1. Argumento explícito: `Launcher("ruta/config.json")`
2. Variable de entorno: `PATH_CONFIG_LAUNCHER=ruta/config.json`

---

## 🚀 Quickstart

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, CreateWriter, UpsertWriter

# 1. Inicializa el Launcher (auto-detecta runtime, crea SparkSession)
launcher = Launcher("config/config.json")

# 2. Carga un contrato JSON — los placeholders {catalog.silver} se resuelven solos
contract = load_contract("tables/fact_ventas.json")

# 3. Construye tu DataFrame (de un source, una transformación, lo que sea)
df = launcher.spark.read.parquet("source/ventas.parquet")

# 4. Escribe usando el writer apropiado
CreateWriter(contract).write(df)

# 5. Día siguiente — solo añadir lo nuevo
UpsertWriter(contract).write(
    df_delta,
    merge_keys=["venta_id", "fecha"],
)
```

Para ejemplos completos con varias capas y tests, ver la carpeta [`demos/`](demos/).

---

## 📚 Demos

Cada demo es **independiente y autocontenido**, pensado como referencia de uso.

| Demo | Tema | Qué demuestra |
|---|---|---|
| [`demos/demo_1`](demos/demo_1) | Contratos y writers gobernados | Bootstrap, append, upsert, partition overwrite, delete y migración con `SafeMigrator`. Dominio: aeronáutica. |
| [`demos/demo_2`](demos/demo_2) | Transformaciones testeables y Data Quality | Pipeline bronze → silver → gold con funciones puras de transformación, tests `pytest` y motor de DQ declarativo. Dominio: manufactura de aseo. |
| `demos/demo_3` | *(próximamente)* | — |

Para correr un demo:

```bash
source .venv-local/bin/activate
cd demos/demo_1
python pipeline_aeronautica.py
```

---

## 🔨 Build

DKOps usa `pyproject.toml` (PEP 517/621). Para construir el wheel distribuible:

```bash
source .venv-local/bin/activate
pip install --upgrade build
python -m build
```

Esto genera en `dist/`:
- `dkops-X.Y.Z-py3-none-any.whl` — wheel para instalar en Databricks o cualquier entorno
- `dkops-X.Y.Z.tar.gz` — sdist

**Subir a Databricks** como librería del cluster:

```bash
databricks libraries install --cluster-id <id> --whl dist/dkops-X.Y.Z-py3-none-any.whl
```

**Versionado:** DKOps sigue [Semantic Versioning](https://semver.org/). La versión vive en `pyproject.toml`.

---

## 📊 Estado del proyecto

| Componente | Estado |
|---|---|
| `Launcher` (multi-runtime) | ✅ Estable |
| Contratos + `ContractLoader` | ✅ Estable |
| Writers (`Create`, `Append`, `Upsert`, `Partition`, `Delete`) | ✅ Estables |
| `SafeMigrator` (esquema seguro) | ✅ Estable |
| Demos (1, 2) | ✅ Disponibles |
| Tests del framework | 🚧 En desarrollo |
| Documentación de API | 🚧 En desarrollo |
| Soporte SCD2 | 📋 Backlog |
| Módulo de Data Quality nativo | 📋 Backlog (existe prototipo en `demo_2`) |

---

<div align="center">

## 🤝 Contribuir

**¿Te interesa lo que estamos construyendo? Las contribuciones son bienvenidas y muy apreciadas.**

[![Issues abiertos](https://img.shields.io/github/issues/<TU_USER>/<NOMBRE_REPO>)](https://github.com/<TU_USER>/<NOMBRE_REPO>/issues)
[![PRs abiertos](https://img.shields.io/github/issues-pr/<TU_USER>/<NOMBRE_REPO>)](https://github.com/<TU_USER>/<NOMBRE_REPO>/pulls)
[![Last commit](https://img.shields.io/github/last-commit/<TU_USER>/<NOMBRE_REPO>)](https://github.com/<TU_USER>/<NOMBRE_REPO>/commits)

</div>

Áreas donde nos vendría especialmente bien ayuda:

- 🧪 **Tests del framework** — todavía no hay suite de tests para DKOps mismo (los demos sí están testeados).
- 📖 **Documentación** — guías de uso, referencia de API, casos reales.
- 🎨 **Más demos** — dominios distintos, patrones distintos.
- 🐛 **Reportar bugs** — abre un issue con un caso reproducible.
- 💡 **Discutir ideas** — el módulo de Data Quality, soporte SCD2, integración con Great Expectations son temas abiertos.

### Cómo contribuir

1. **Haz fork** del repo y crea una rama: `git checkout -b feature/mi-mejora`
2. Activa el venv local: `source .venv-local/bin/activate`
3. Haz tus cambios siguiendo el estilo del código existente.
4. Si añades funcionalidad, **añade un test o un demo** que la demuestre.
5. Verifica que los demos siguen pasando: `cd demos/demo_2 && pytest`
6. Abre un Pull Request describiendo el cambio y por qué es útil.

¿Primera vez contribuyendo a un proyecto open source? Consulta [esta guía de GitHub](https://docs.github.com/es/get-started/quickstart/contributing-to-projects).

---

## 📄 Licencia

DKOps se distribuye bajo licencia MIT. Ver [`LICENSE`](LICENSE) para los términos completos.

---

<div align="center">

**Hecho con ☕ y ❤️ por el equipo de Data Engineering.**

Si DKOps te resulta útil, considera darle una ⭐ al repo — ayuda a que otros lo encuentren.

</div>