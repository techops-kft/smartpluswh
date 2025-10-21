import psycopg2
from psycopg2.extras import execute_values

def insertar_aum_bonos_select_actual(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            SELECT
                CASE
                    WHEN tipo = 18 THEN 'Bono Select 3x'
                    WHEN tipo = 19 THEN 'Bono Select 4x'
                    WHEN tipo = 20 THEN 'Bono Select 5x'
                    WHEN tipo = 28 THEN 'Bono Select 8x'
                    WHEN tipo = 29 THEN 'Bono Select 9x'
                    WHEN tipo = 30 THEN 'Bono Select 10x'
                END AS tipo_bono,
                tipo,
                COALESCE(SUM(monto)::NUMERIC(18,2), 0) AS total
            FROM pagos
            WHERE tipo IN (18,19,20,28,29,30)
              AND estatus = 1
              AND socio IN (
                  SELECT socio
                  FROM socio_productos
                  WHERE producto IN (280,281,282,283,284,285)
              )
            GROUP BY tipo_bono, tipo
            ORDER BY tipo;
        """)

        resultados_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(resultados_origen)}")

        if not resultados_origen:
            print("ℹ No hay registros para insertar.")
            return {
                "estatus": "success",
                "tabla": "aum_bonos_select_actual",
                "proceso": "insertar_aum_bonos_select_actual",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LIMPIAR TABLA
        print("Limpiando tabla aum_bonos_select_actual con TRUNCATE...")
        cur_destino.execute("TRUNCATE TABLE aum_bonos_select_actual;")
        conn_destino.commit()

        # ---------- INSERTAR NUEVOS REGISTROS
        print("Insertando registros en aum_bonos_select_actual")
        insert_sql = """
            INSERT INTO aum_bonos_select_actual (
                tipo_bono,
                total
            ) VALUES %s
        """

        # Ajustar los datos: quitar la columna 'tipo' para insertarla en la tabla
        datos_a_insertar = [(row[0], row[2]) for row in resultados_origen]

        execute_values(cur_destino, insert_sql, datos_a_insertar)
        conn_destino.commit()

        registros_insertados = len(datos_a_insertar)
        print(f"✅ Se han insertado {registros_insertados} registros correctamente.")

        return {
            "estatus": "success",
            "tabla": "aum_bonos_select_actual",
            "proceso": "insertar_aum_bonos_select_actual",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "aum_bonos_select_actual",
            "proceso": "insertar_aum_bonos_select_actual",
            "registros_insertados": 0,
            "error_text": str(e)
        }
