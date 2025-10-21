import psycopg2
from psycopg2.extras import execute_values

def insertar_cliente_liabilities(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            WITH aum AS (
                SELECT socio, SUM(saldo)::numeric AS pasivo_productos
                FROM socio_productos sp
                WHERE sp.cerrado IS FALSE
                    AND sp.producto NOT IN (280,281,282)
                    AND sp.socio IN (SELECT socio FROM socios WHERE estatus IN (1,2))
                GROUP BY socio
            ),
            pendientes AS (
                SELECT socio, COALESCE(SUM(monto)::numeric, 0) AS pasivo_pagos_pendientes
                FROM pagos
                WHERE estatus = 0
                    AND tipo = 0
                GROUP BY socio
            )
            SELECT
                a.socio AS id_socio,
                a.pasivo_productos,
                COALESCE(p.pasivo_pagos_pendientes, 0) AS pasivo_pagos_pendientes,
                a.pasivo_productos - COALESCE(p.pasivo_pagos_pendientes, 0) AS liabilities
            FROM aum a
            LEFT JOIN pendientes p ON p.socio = a.socio
            ORDER BY a.socio;
        """)
        
        resultados_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(resultados_origen)}")

        if not resultados_origen:
            print("ℹ No hay registros para insertar/actualizar.")
            return {
                "estatus": "success",
                "tabla": "fact_cliente_liabilities",
                "proceso": "insertar_cliente_liabilities",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- UPSERT DATA
        print("Insertando/actualizando registros en fact_cliente_liabilities")

        insert_sql = """
            INSERT INTO fact_cliente_liabilities (
                id_socio,
                pasivo_productos,
                pasivo_pagos_pendientes,
                liabilities
            ) VALUES %s
            ON CONFLICT (id_socio) DO UPDATE
            SET pasivo_productos = EXCLUDED.pasivo_productos,
                pasivo_pagos_pendientes = EXCLUDED.pasivo_pagos_pendientes,
                liabilities = EXCLUDED.liabilities,
                fecha_insert = NOW()
        """

        execute_values(cur_destino, insert_sql, resultados_origen)
        conn_destino.commit()

        registros_insertados = len(resultados_origen)
        print(f"✅ Se han insertado/actualizado {registros_insertados} registros correctamente.")

        return {
            "estatus": "success",
            "tabla": "fact_cliente_liabilities",
            "proceso": "insertar_cliente_liabilities",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_cliente_liabilities",
            "proceso": "insertar_cliente_liabilities",
            "registros_insertados": 0,
            "error_text": str(e)
        }
