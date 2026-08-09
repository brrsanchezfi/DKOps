---
description: Valida todos los contratos JSON del repo contra los JSON Schema
---

Valida los contratos de tabla e ingesta del repo.

Ejecuta:

```bash
python scripts/validate_contracts.py $ARGUMENTS
```

Sin argumentos valida `demos/`. Con un argumento valida ese directorio.

Si `jsonschema` no está instalado el script avisa y omite la capa de schema —
en ese caso instálalo (`python -m pip install jsonschema`) y vuelve a ejecutar,
porque la validación parcial no sirve como garantía.

Para cada error reportado:

1. Abre el contrato señalado.
2. Consulta `schema/table_contract.schema.json` o
   `schema/ingestion_contract.schema.json` para ver el campo correcto — las
   descripciones del schema explican el porqué de cada regla.
3. Corrige el JSON y vuelve a validar hasta que salga 0 errores.

No relajes el schema para que un contrato pase. Si crees que el schema está
equivocado, dilo explícitamente y explica por qué antes de tocarlo.
