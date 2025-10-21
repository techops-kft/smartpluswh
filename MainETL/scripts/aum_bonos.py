import psycopg2
from psycopg2.extras import execute_values

def insertar_aum_bonos(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            SELECT
                max(fecha_pago::date) AS fecha_ultimo_bono,
                sum(monto)::numeric AS bonos_a_pagar
            FROM pagos
            WHERE estatus = 0
              AND tipo NOT IN (18,19,20)
        """)

        resultados_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(resultados_origen)}")

        if not resultados_origen or resultados_origen[0][0] is None:
            print("ℹ No hay registros para insertar.")
            return {
                "estatus": "success",
                "tabla": "aum_bonos",
                "proceso": "insertar_aum_bonos",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LIMPIAR TABLA
        print("Limpiando tabla aum_bonos con TRUNCATE...")
        cur_destino.execute("TRUNCATE TABLE aum_bonos;")
        conn_destino.commit()

        # ---------- INSERT NUEVO REGISTRO
        print("Insertando nuevo registro en aum_bonos")
        insert_sql = """
            INSERT INTO aum_bonos (
                fecha_ultimo_bono,
                bonos_a_pagar
            ) VALUES %s
        """

        execute_values(cur_destino, insert_sql, resultados_origen)
        conn_destino.commit()

        registros_insertados = len(resultados_origen)
        print(f"✅ Se ha insertado {registros_insertados} registro correctamente.")

        return {
            "estatus": "success",
            "tabla": "aum_bonos",
            "proceso": "insertar_aum_bonos",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "aum_bonos",
            "proceso": "insertar_aum_bonos",
            "registros_insertados": 0,
            "error_text": str(e)
        }
