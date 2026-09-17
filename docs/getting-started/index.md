# Primeros pasos

<p class="dk-lead">
Esta sección te lleva desde la instalación hasta un pipeline que mueve datos de Landing
a Gold. Son tres páginas cortas y conviene leerlas en orden.
</p>

## Para quién es DKOps

DKOps tiene sentido cuando un equipo de datos pasa de tener algunos scripts sueltos a
mantener decenas de tablas. En ese punto aparecen problemas conocidos:

| Situación | Cómo la aborda DKOps |
|---|---|
| El schema de cada tabla está escondido en el código | Contratos JSON versionados y validados antes de escribir |
| Un cambio de columnas rompe la carga | `SafeMigrator` y la opción `merge_schema` |
| Cada dataset repite la misma lógica de ingesta | `IngestionEngine` con estrategias que se eligen en el contrato |
| El código de local y el de Databricks divergen | Detección de runtime y placeholders por entorno |
| Nadie sabe qué corrió anoche ni cuántas filas movió | Tabla de control operativo consultable con SQL |

## Recorrido

<div class="grid cards" markdown>

-   **1. Instalación**

    ---

    Instala el paquete con el extra adecuado para tu entorno.

    [:octicons-arrow-right-24: Instalar](installation.md)

-   **2. Configuración**

    ---

    Define catálogos, rutas y logging en `config.json`.

    [:octicons-arrow-right-24: Configurar](configuration.md)

-   **3. Tu primer pipeline**

    ---

    Crea los contratos y ejecuta la carga completa hasta Gold.

    [:octicons-arrow-right-24: Construir](first-pipeline.md)

</div>
