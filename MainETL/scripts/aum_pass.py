import psycopg2
from psycopg2.extras import execute_values

def insertar_aum_pass(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            SELECT
                max(fecha_compra::date) AS fecha_ultimo_plan,
                sum(saldo)::numeric AS saldo_total_productos,
                sum(capital)::numeric AS capital_inicial_total_productos
            FROM socio_productos sp
            WHERE
                cerrado IS FALSE
                AND producto NOT IN (280,281,282)
                AND socio IN (SELECT socio FROM socios WHERE estatus IN (1,2))
        """)

        resultados_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(resultados_origen)}")

        if not resultados_origen or resultados_origen[0][0] is None:
            print("ℹ No hay registros para insertar.")
            return {
                "estatus": "success",
                "tabla": "aum_pass",
                "proceso": "insertar_aum_pass",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- TRUNCATE TABLA
        print("Limpiando tabla aum_pass con TRUNCATE...")
        cur_destino.execute("TRUNCATE TABLE aum_pass;")
        conn_destino.commit()

        # ---------- INSERT NUEVO REGISTRO
        print("Insertando nuevo registro en aum_pass")
        insert_sql = """
            INSERT INTO aum_pass (
                fecha_ultimo_plan,
                saldo_total_productos,
                capital_inicial_total_productos
            ) VALUES %s
        """

        execute_values(cur_destino, insert_sql, resultados_origen)
        conn_destino.commit()

        registros_insertados = len(resultados_origen)
        print(f"✅ Se ha insertado {registros_insertados} registro correctamente.")

        return {
            "estatus": "success",
            "tabla": "aum_pass",
            "proceso": "insertar_aum_pass",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "aum_pass",
            "proceso": "insertar_aum_pass",
            "registros_insertados": 0,
            "error_text": str(e)
        }
