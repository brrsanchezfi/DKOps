# Operación

<p class="dk-lead">
DKOps tiene dos sistemas de registro que responden preguntas diferentes. Confundirlos es
la causa más habitual de no encontrar la información que se busca.
</p>

| | Logger de aplicación | Registro operativo |
|---|---|---|
| Clase | `LoggableMixin` y `AppLogger` | `IngestionOpsLogger` |
| Responde a | ¿Qué está pasando ahora? | ¿Qué se ejecutó y cómo terminó? |
| Dónde escribe | Consola y archivo `.log` | Tabla Delta de control |
| Cuánto dura | Lo que dure la retención del log | Permanente |
| Cómo se consulta | Leyendo el log | Con SQL |
| Para qué sirve | Depurar y seguir una ejecución | Auditoría, monitoreo y SLA |

Una regla sencilla: si necesitas la respuesta **mientras corre** el pipeline, mira el log
de aplicación. Si la necesitas **una semana después y agregada**, consulta el registro
operativo.

## Contenido de la sección

<div class="grid cards" markdown>

-   **Logger de aplicación**

    ---

    `self.log`, helpers semánticos, el decorador `log_operation` y la configuración.

    [:octicons-arrow-right-24: Leer](app-logger.md)

-   **Logs en la nube**

    ---

    Cómo se escriben los logs en ADLS, S3 o GCS y cómo leerlos después.

    [:octicons-arrow-right-24: Leer](cloud-logs.md)

-   **Registro operativo**

    ---

    La tabla de control: cómo activarla, su esquema y su uso fuera del engine.

    [:octicons-arrow-right-24: Leer](ops-table.md)

-   **Consultas de monitoreo**

    ---

    SQL listo para calcular tasas de éxito, duraciones y ejecuciones sin cerrar.

    [:octicons-arrow-right-24: Leer](ops-queries.md)

</div>
