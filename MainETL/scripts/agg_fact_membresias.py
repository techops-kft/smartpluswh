from psycopg2.extras import execute_values

def actualizar_agg_fact_membresias(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        print("Ejecutando: agg_fact_membresias -> actualizar_agg_fact_membresias")

        # Consulta de agregación por mes
        cur_destino.execute("""
            SELECT 
                date_trunc('month', fecha_pago)::date AS mes,
                COUNT(*) AS total_pagos,
                SUM(precio::numeric) AS total_ingresos
            FROM fact_membresias
            WHERE fecha_pago::date BETWEEN '2022-01-01' AND now()::date
              AND estatus = 'Pagado'
            GROUP BY date_trunc('month', fecha_pago)
            ORDER BY mes;
        """)

        resultados = cur_destino.fetchall()

        if not resultados:
            print("No hay datos agregados para insertar en agg_fact_membresias.")
            return {
                "estatus": "success",
                "tabla": "agg_fact_membresias",
                "proceso": "actualizar_agg_fact_membresias",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # Insertar con UPSERT
        insert_sql = """
            INSERT INTO agg_fact_membresias (mes, total_pagos, total_ingresos)
            VALUES %s
            ON CONFLICT (mes) DO UPDATE
            SET total_pagos = EXCLUDED.total_pagos,
                total_ingresos = EXCLUDED.total_ingresos;
        """

        execute_values(cur_destino, insert_sql, resultados)
        conn_destino.commit()
        registros_insertados = len(resultados)
        print(f"Se han insertado/actualizado {registros_insertados} registros en agg_fact_membresias.")

        return {
            "estatus": "success",
            "tabla": "agg_fact_membresias",
            "proceso": "actualizar_agg_fact_membresias",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        print(f"Error al actualizar agg_fact_membresias: {e}")
        return {
            "estatus": "failed",
            "tabla": "agg_fact_membresias",
            "proceso": "actualizar_agg_fact_membresias",
            "registros_insertados": 0,
            "error_text": str(e)
        }
