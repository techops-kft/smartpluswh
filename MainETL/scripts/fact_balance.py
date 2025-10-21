import psycopg2
from psycopg2.extras import execute_values

def insertar_fact_balance(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        print("Ejecutando consulta de balance diario en cur_origen...")

        # --- Consulta principal (se ejecuta en la base de origen) ---
        cur_origen.execute("""
            WITH params AS (
              SELECT DATE '2025-01-01' AS d_ini,
                     NOW()::date       AS d_fin
            ),
            calendario AS (
              SELECT gs::date AS fecha
              FROM params p, LATERAL generate_series(p.d_ini, p.d_fin, interval '1 day') gs
            ),
            ingresos AS (
              SELECT
                c.fecha_pago::date AS fecha,
                SUM(CASE WHEN c.tipo_item = 5 THEN c.monto_pagado::numeric ELSE 0 END)  AS anualidades,
                SUM(CASE WHEN c.tipo_item = 1 THEN c.monto_pagado::numeric ELSE 0 END)  AS membresias,
                SUM(CASE WHEN c.tipo_item NOT IN (1,5) THEN c.monto_pagado::numeric ELSE 0 END) AS venta_planes
              FROM compras c, params p
              WHERE c.estatus = 6
                AND c.fecha_pago::date BETWEEN p.d_ini AND p.d_fin
              GROUP BY c.fecha_pago::date
            ),
            egresos AS (
              SELECT
                p.fecha_pagado::date AS fecha,
                SUM(CASE WHEN p.tipo NOT IN (0,4) THEN p.monto::numeric ELSE 0 END) AS bonos_y_comisiones,
                SUM(CASE WHEN p.tipo = 4 THEN p.monto::numeric ELSE 0 END) AS masterbonos,
                SUM(CASE WHEN p.tipo = 0 THEN p.monto::numeric ELSE 0 END) AS retiros
              FROM pagos p, params pr
              WHERE p.estatus = 1
                AND p.fecha_pagado::date BETWEEN pr.d_ini AND pr.d_fin
              GROUP BY p.fecha_pagado::date
            )
            SELECT
              c.fecha,
              COALESCE(i.anualidades, 0)::numeric AS anualidades,
              COALESCE(i.membresias, 0)::numeric AS membresias,
              COALESCE(i.venta_planes, 0)::numeric AS venta_planes,
              COALESCE(e.bonos_y_comisiones, 0)::numeric AS bonos_y_comisiones,
              COALESCE(e.masterbonos, 0)::numeric AS masterbonos,
              COALESCE(e.retiros, 0)::numeric AS retiros,
              (
                COALESCE(i.anualidades,0) + COALESCE(i.membresias,0) + COALESCE(i.venta_planes,0)
                - COALESCE(e.bonos_y_comisiones,0) - COALESCE(e.masterbonos,0) - COALESCE(e.retiros,0)
              )::numeric AS total
            FROM calendario c
            LEFT JOIN ingresos i ON i.fecha = c.fecha
            LEFT JOIN egresos e  ON e.fecha = c.fecha
            ORDER BY c.fecha;
        """)

        resultados = cur_origen.fetchall()
        print(f"Registros obtenidos desde origen: {len(resultados)}")

        # --- Validación: si no hay resultados, salir ---
        if not resultados:
            print("ℹ No hay registros para insertar en fact_balance.")
            return {
                "estatus": "success",
                "tabla": "fact_balance",
                "proceso": "insertar_fact_balance",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # --- Inserción o actualización (UPSERT) en la base destino ---
        print("Insertando o actualizando registros en fact_balance (destino)...")

        insert_sql = """
            INSERT INTO fact_balance (
                fecha,
                anualidades,
                membresias,
                venta_planes,
                bonos_y_comisiones,
                masterbonos,
                retiros,
                total
            ) VALUES %s
            ON CONFLICT (fecha)
            DO UPDATE SET
                anualidades = EXCLUDED.anualidades,
                membresias = EXCLUDED.membresias,
                venta_planes = EXCLUDED.venta_planes,
                bonos_y_comisiones = EXCLUDED.bonos_y_comisiones,
                masterbonos = EXCLUDED.masterbonos,
                retiros = EXCLUDED.retiros,
                total = EXCLUDED.total,
                fecha_insert = NOW();
        """

        execute_values(cur_destino, insert_sql, resultados)
        conn_destino.commit()

        registros_insertados = len(resultados)
        print(f"✅ Se han insertado o actualizado {registros_insertados} registros en fact_balance.")

        return {
            "estatus": "success",
            "tabla": "fact_balance",
            "proceso": "insertar_fact_balance",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        print(f"❌ Error en insertar_fact_balance: {e}")
        return {
            "estatus": "failed",
            "tabla": "fact_balance",
            "proceso": "insertar_fact_balance",
            "registros_insertados": 0,
            "error_text": str(e)
        }
