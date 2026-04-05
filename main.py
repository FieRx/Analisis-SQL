import duckdb
import pandas as pd

con = duckdb.connect()

con.execute("""
    CREATE TABLE logs AS 
    SELECT * FROM read_json_auto('data/logs_access_logs.json')
""")

"""-- analysis.sql - Análisis Completo de Logs
-- ================================================
-- Dataset: Web server logs de aplicación e-commerce
-- Autor: Reinaldo Franco
-- Fecha: 5/4/2026
-- ================================================"""

"""-- ========================================
-- 1. EXPLORACIÓN INICIAL
-- ========================================
-- ¿Cuántos registros? ¿Qué período cubren?"""


df_kpis = con.execute("""SELECT
                  COUNT(*) as total_requests,
                  MIN(timestamp) as primera_request,
                  MAX(timestamp) as ultima_request,
                  COUNT(DISTINCT user_id) as usuarios_unicos,
                  COUNT(DISTINCT endpoint) as endpoints_unicos
                  FROM logs;""").fetchdf()

print(df_kpis)

"""-- ========================================
-- 2. ENDPOINTS MÁS USADOS
-- ========================================
-- ¿Qué endpoints reciben más tráfico?"""

df_endpoints = con.execute("""SELECT 
                            endpoint,
                            COUNT(*) as hits,
                            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM logs), 2) as porcentaje
                            FROM logs
                            GROUP BY endpoint
                            ORDER BY hits DESC
                            LIMIT 10;""").fetchdf()

print(df_endpoints)

"""-- ========================================
-- 3. ANÁLISIS DE ERRORES
-- ========================================
-- ¿Qué endpoints tienen más errores 500?"""

df_errors = con.execute("""SELECT
                        endpoint,
                        COUNT(*) as total_errors,
                        COUNT(DISTINCT user_id) as usuarios_afectados,
                        ROUND(AVG(response_time_ms), 2) as avg_response_time
                        FROM logs
                        WHERE status_code >= 500
                        GROUP BY endpoint
                        ORDER BY total_errors DESC
                        LIMIT 10;""").fetchdf()

print(df_errors)

"""-- ========================================
-- 4. PERFORMANCE POR ENDPOINT
-- ========================================
-- ¿Qué endpoints son más lentos?"""

df_ep_time = con.execute("""SELECT 
                                endpoint,
                                COUNT(*) as requests,
                                ROUND(AVG(response_time_ms), 2) as avg_time,
                                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_time_ms), 2) as p50,
                                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms), 2) as p95,
                                MAX(response_time_ms) as max_time
                            FROM logs
                            WHERE status_code < 500  -- Solo requests exitosas
                            GROUP BY endpoint
                            HAVING COUNT(*) > 100  -- Solo endpoints con suficiente tráfico
                            ORDER BY p95 DESC
                            LIMIT 10;""").fetchdf()

print(df_ep_time)

"""-- ========================================
-- 5. TENDENCIA HORARIA
-- ========================================
-- ¿A qué hora hay más tráfico?"""

df_peak_hours = con.execute("""SELECT 
                                    EXTRACT(HOUR FROM timestamp) as hora,
                                    COUNT(*) as requests,
                                    ROUND(AVG(response_time_ms), 2) as avg_response_time,
                                    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) as errors
                                FROM logs
                                GROUP BY EXTRACT(HOUR FROM timestamp)
                                ORDER BY hora;""").fetchdf()

print(df_peak_hours)

"""-- ========================================
-- 6. WINDOW FUNCTIONS - RANKING
-- ========================================
-- Top 3 requests más lentas por endpoint"""

df_ranking = con.execute("""WITH ranked AS (
    SELECT 
        endpoint,
        timestamp,
        response_time_ms,
        user_id,
        ROW_NUMBER() OVER (
            PARTITION BY endpoint 
            ORDER BY response_time_ms DESC
        ) as rank
    FROM logs
    WHERE status_code < 500
)
SELECT * FROM ranked 
WHERE rank <= 3
ORDER BY endpoint, rank;""").fetchdf()

print(df_ranking)


"""-- ========================================
-- 7. COMPARACIÓN CON PERÍODO ANTERIOR
-- ========================================
-- ¿Cómo cambia el tráfico día a día?"""

df_comp = con.execute("""WITH daily_stats AS (
                                SELECT 
                                    DATE(timestamp) as fecha,
                                    COUNT(*) as requests,
                                    ROUND(AVG(response_time_ms), 2) as avg_time
                                FROM logs
                                GROUP BY DATE(timestamp)
                            )
                            SELECT 
                                fecha,
                                requests,
                                LAG(requests) OVER (ORDER BY fecha) as requests_dia_anterior,
                                requests - LAG(requests) OVER (ORDER BY fecha) as diferencia,
                                ROUND(
                                    (requests - LAG(requests) OVER (ORDER BY fecha)) * 100.0 / 
                                    LAG(requests) OVER (ORDER BY fecha), 
                                    2
                                ) as cambio_porcentual
                            FROM daily_stats
                            ORDER BY fecha;""").fetchdf()

print(df_comp)

