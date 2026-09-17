# Instalación

DKOps requiere Python 3.9 o superior. El paquete base solo depende de `loguru`;
PySpark y Delta se instalan aparte porque en Databricks ya vienen con el runtime y
añadirlos de nuevo rompe el cluster.

## Elige el extra según tu entorno

=== "Desarrollo local"

    Incluye PySpark 3.5 y Delta Lake 3.2 para correr todo en tu equipo.

    ```bash
    pip install "DKOps[local]"
    ```

=== "Databricks Connect"

    Para ejecutar desde tu equipo contra un cluster remoto. La versión de
    `databricks-connect` tiene que coincidir con la del runtime del cluster.

    ```bash
    pip install "DKOps[databricks-connect]"
    ```

=== "Dentro de Databricks"

    En un notebook o un job, el paquete base es suficiente.

    ```bash
    %pip install DKOps
    ```

!!! warning "No mezcles entornos"

    `pyspark` y `databricks-connect` no pueden convivir en el mismo entorno virtual.
    Si trabajas con ambos, usa dos entornos separados (en el repositorio se llaman
    `.venv-local` y `.venv-databricks`).

## Desde el código fuente

```bash
git clone https://github.com/brrsanchezfi/DKOps
cd DKOps
pip install -e ".[local]"
```

También puedes instalar una versión concreta directamente desde un tag:

```bash
pip install "DKOps @ git+https://github.com/brrsanchezfi/DKOps.git@v0.3.6"
```

## Comprueba la instalación

```python
import DKOps
from DKOps.launcher import Launcher
```

!!! note "El import distingue mayúsculas"

    La distribución y el módulo se llaman `DKOps`. `pip install dkops` funciona porque
    pip normaliza el nombre, pero `import dkops` falla. Usa siempre `import DKOps`.

## Versiones que conviene evitar desde git

Estas notas solo afectan a la instalación desde un tag de git. Los paquetes publicados
en PyPI están bien construidos.

| Tag | Problema | Qué hacer |
|---|---|---|
| `v0.3.0` | El tag apunta a un commit cuyo `pyproject.toml` todavía dice `0.2.4` | Usa `v0.3.2` o posterior |
| `v0.3.1` | Declara la licencia con una sintaxis que exige `setuptools >= 77`; en Databricks falla con `ERROR_WHEEL_BUILD` | Usa `v0.3.2` o posterior |

Desde la `v0.3.2`, el workflow de publicación comprueba que el tag y la versión del
`pyproject.toml` coincidan.
