# Análisis SQL
Análisis SQL de Logs de Servidor

Ejercicio de analisis SQL de logs de un servidor para:

- Detectar problemas: Errores, timeouts, patrones anómalos
- Optimizar performance: Identificar endpoints lentos
- Entender usuarios: Qué usan, cuándo, cómo
- Debugging: Reconstruir qué pasó cuando algo falló

Situacion: Tenía acceso a logs de un servidor web con millones de requests. El equipo de desarrollo sabía que había problemas de performance pero no podía identificar exactamente dónde.

Tarea: Mi objetivo era analizar los logs para encontrar patrones de errores, identificar endpoints lentos, y generar insights accionables para el equipo.

Proceso: 

- Cargué los logs en DuckDB para poder hacer análisis SQL rápido sin necesidad de un servidor
- Usé Window Functions para calcular percentiles de response time (p50, p95, p99)
- Implementé análisis temporal con LAG() para detectar degradación gradual
- Creé CTEs para organizar queries complejas de forma legible
- Identifiqué que el endpoint /api/search tenía p99 de 5 segundos vs 200ms del resto

Herramientas usadas:

- DuckDB para SQL, aplicando Windows Functions y CTEs
- Python en VBC. pandas y duckdb como librerias principales.
- Aplicacion de analisis de datos
