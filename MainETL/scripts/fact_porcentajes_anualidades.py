import psycopg2
from psycopg2.extras import execute_values

def insertar_porcentajes_anualidades(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        print("Ejecutando consulta de porcentajes de anualidades en cur_destino...")

        cur_destino.execute("""
            WITH socios_2025 AS (
              SELECT 
                id_socio AS socio,
                pago_anualidad AS fecha_venc
              FROM dim_membresias_2025
            ),
            pagos_2025 AS (
              SELECT
                c.id_socio AS socio,
                c.fecha_pago::date AS fecha_pago
              FROM fact_anualidades c
              WHERE c.fecha_pago::date BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
            ),
            ult_pago AS (
              SELECT 
                socio, 
                MAX(fecha_pago) AS fecha_pago_2025
              FROM pagos_2025
              GROUP BY socio
            ),
            base AS (
              SELECT
                s.socio,
                s.fecha_venc,
                u.fecha_pago_2025,
                DATE_TRUNC('month', s.fecha_venc)::date AS mes_venc,
                CASE
                  WHEN u.fecha_pago_2025 IS NULL         THEN 'abandono'
                  WHEN u.fecha_pago_2025 <= s.fecha_venc THEN 'retenido'
                  ELSE 'recuperado'
                END AS estado_2025
              FROM socios_2025 s
              LEFT JOIN ult_pago u USING (socio)
            ),
            agg AS (
              SELECT
                mes_venc,
                COUNT(DISTINCT socio) FILTER (WHERE estado_2025 = 'retenido')   AS retenido,
                COUNT(DISTINCT socio) FILTER (WHERE estado_2025 = 'abandono')   AS abandono,
                COUNT(DISTINCT socio) FILTER (WHERE estado_2025 = 'recuperado') AS recuperado,
                COUNT(DISTINCT socio)                                            AS total_mes
              FROM base
              GROUP BY mes_venc
            )
            SELECT
              mes_venc,
              retenido,
              abandono,
              recuperado,
              total_mes,
              ROUND(100.0 * retenido   / NULLIF(total_mes, 0), 2) AS pct_retencion,
              ROUND(100.0 * abandono   / NULLIF(total_mes, 0), 2) AS pct_abandono,
              ROUND(100.0 * recuperado / NULLIF(total_mes, 0), 2) AS pct_recuperacion
            FROM agg
            ORDER BY mes_venc;
        """)

        resultados = cur_destino.fetchall()
        print(f"Registros obtenidos: {len(resultados)}")

        if not resultados:
            print("ℹ No hay registros para insertar o actualizar en fact_porcentajes_anualidades.")
            return {
                "estatus": "success",
                "tabla": "fact_porcentajes_anualidades",
                "proceso": "insertar_porcentajes_anualidades",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- UPSERT ----------
        print("Insertando o actualizando registros en fact_porcentajes_anualidades...")

        insert_sql = """
            INSERT INTO fact_porcentajes_anualidades (
                mes_venc,
                retenido,
                abandono,
                recuperado,
                total_mes,
                pct_retencion,
                pct_abandono,
                pct_recuperacion
            ) VALUES %s
            ON CONFLICT (mes_venc)
            DO UPDATE SET
                retenido = EXCLUDED.retenido,
                abandono = EXCLUDED.abandono,
                recuperado = EXCLUDED.recuperado,
                total_mes = EXCLUDED.total_mes,
                pct_retencion = EXCLUDED.pct_retencion,
                pct_abandono = EXCLUDED.pct_abandono,
                pct_recuperacion = EXCLUDED.pct_recuperacion,
                fecha_insert = NOW();
        """

        execute_values(cur_destino, insert_sql, resultados)
        conn_destino.commit()

        registros_insertados = len(resultados)
        print(f"✅ Se han insertado o actualizado {registros_insertados} registros en fact_porcentajes_anualidades.")

        return {
            "estatus": "success",
            "tabla": "fact_porcentajes_anualidades",
            "proceso": "insertar_porcentajes_anualidades",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        print(f"❌ Error: {e}")
        return {
            "estatus": "failed",
            "tabla": "fact_porcentajes_anualidades",
            "proceso": "insertar_porcentajes_anualidades",
            "registros_insertados": 0,
            "error_text": str(e)
        }
