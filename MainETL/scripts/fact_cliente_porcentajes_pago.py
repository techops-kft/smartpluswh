import psycopg2
from psycopg2.extras import execute_values

def upsert_cliente_porcentajes_pago(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            WITH pendientes AS (
                SELECT
                    socio,
                    COALESCE(SUM(monto)::numeric, 0) AS pasivo_pagos_pendientes
                FROM pagos
                WHERE estatus = 0
                GROUP BY socio
            ),
            realizados AS (
                SELECT
                    socio,
                    COALESCE(SUM(monto)::numeric, 0) AS pasivo_pagos_realizados
                FROM pagos
                WHERE estatus = 1
                GROUP BY socio
            )
            SELECT
                p.socio AS id_socio,
                p.pasivo_pagos_pendientes,
                COALESCE(r.pasivo_pagos_realizados, 0) AS pasivo_pagos_realizados,
                CASE
                    WHEN p.pasivo_pagos_pendientes = 0 THEN NULL
                    ELSE (COALESCE(r.pasivo_pagos_realizados, 0) / p.pasivo_pagos_pendientes) * 100
                END AS porcentajes_pago
            FROM pendientes p
            LEFT JOIN realizados r ON r.socio = p.socio
            WHERE p.socio <> -1 AND p.socio <> 1624
            ORDER BY p.socio;
        """)

        resultados_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(resultados_origen)}")

        if not resultados_origen:
            print("ℹ No hay registros para insertar/actualizar.")
            return {
                "estatus": "success",
                "tabla": "fact_cliente_porcentajes_pago",
                "proceso": "upsert_cliente_porcentajes_pago",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- UPSERT DATA
        print("Insertando/actualizando registros en fact_cliente_porcentajes_pago")

        insert_sql = """
            INSERT INTO fact_cliente_porcentajes_pago (
                id_socio,
                pasivo_pagos_pendientes,
                pasivo_pagos_realizados,
                porcentajes_pago
            ) VALUES %s
            ON CONFLICT (id_socio) DO UPDATE
            SET pasivo_pagos_pendientes = EXCLUDED.pasivo_pagos_pendientes,
                pasivo_pagos_realizados = EXCLUDED.pasivo_pagos_realizados,
                porcentajes_pago = EXCLUDED.porcentajes_pago,
                fecha_insert = NOW()
        """

        execute_values(cur_destino, insert_sql, resultados_origen)
        conn_destino.commit()

        registros_insertados = len(resultados_origen)
        print(f"✅ Se han insertado/actualizado {registros_insertados} registros correctamente.")

        return {
            "estatus": "success",
            "tabla": "fact_cliente_porcentajes_pago",
            "proceso": "upsert_cliente_porcentajes_pago",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_cliente_porcentajes_pago",
            "proceso": "upsert_cliente_porcentajes_pago",
            "registros_insertados": 0,
            "error_text": str(e)
        }
