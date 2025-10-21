import psycopg2
from psycopg2.extras import execute_values

def insertar_recompra_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA ----------
        print("Ejecutando consulta de recompras...")

        cur_origen.execute("""
            WITH ventas AS (
              SELECT
                 c.compra,
                 c.socio,
                 c.fecha_pago::date AS fecha_pago,
                 DATE_TRUNC('month', c.fecha_pago)::date AS mes,
                 ROW_NUMBER() OVER (
                   PARTITION BY c.socio
                   ORDER BY c.fecha_pago
                 ) AS orden_compra
              FROM compras c
              WHERE c.estatus = 6
                AND c.tipo_item NOT IN (1,3,5)
                AND c.fecha_pago::date >= DATE '2025-01-01'
            )
            SELECT
               mes,
               COUNT(*) AS total_compras,
               COUNT(*) FILTER (WHERE orden_compra = 1) AS primeras_compras,
               COUNT(*) FILTER (WHERE orden_compra > 1) AS recompras,
               ROUND(100.0 * COUNT(*) FILTER (WHERE orden_compra > 1) / COUNT(*), 2) AS pct_recompra
            FROM ventas
            GROUP BY mes
            ORDER BY mes;
        """)

        resultados = cur_origen.fetchall()
        print(f"Registros origen: {len(resultados)}")

        if not resultados:
            print("ℹ No hay registros para insertar o actualizar en fact_recompra_planes.")
            return {
                "estatus": "success",
                "tabla": "fact_recompra_planes",
                "proceso": "insertar_recompra_planes",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD DATA ----------
        print("Insertando o actualizando registros en fact_recompra_planes...")

        insert_sql = """
            INSERT INTO fact_recompra_planes (
                mes,
                total_compras,
                primeras_compras,
                recompras,
                pct_recompra
            ) VALUES %s
            ON CONFLICT (mes)
            DO UPDATE SET
                total_compras = EXCLUDED.total_compras,
                primeras_compras = EXCLUDED.primeras_compras,
                recompras = EXCLUDED.recompras,
                pct_recompra = EXCLUDED.pct_recompra,
                fecha_insert = NOW();
        """

        execute_values(cur_destino, insert_sql, resultados)
        conn_destino.commit()

        registros_insertados = len(resultados)
        print(f"✅ Se han insertado o actualizado {registros_insertados} registros correctamente en fact_recompra_planes.")

        return {
            "estatus": "success",
            "tabla": "fact_recompra_planes",
            "proceso": "insertar_recompra_planes",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        print(f"❌ Error: {e}")
        return {
            "estatus": "failed",
            "tabla": "fact_recompra_planes",
            "proceso": "insertar_recompra_planes",
            "registros_insertados": 0,
            "error_text": str(e)
        }
