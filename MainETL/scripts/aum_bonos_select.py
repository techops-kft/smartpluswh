import psycopg2
from psycopg2.extras import execute_values

def insertar_aum_bonos_select(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            SELECT
                CASE
                    WHEN nombre = 'Select 3x' THEN (fecha_compra::date + INTERVAL '1 year')::date
                    WHEN nombre = 'Select 4x' THEN (fecha_compra::date + INTERVAL '1 year')::date
                    WHEN nombre = 'Select 5x' THEN (fecha_compra::date + INTERVAL '1 year')::date
                    WHEN nombre = 'Select 8x' THEN (fecha_compra::date + INTERVAL '2 year')::date
                    WHEN nombre = 'Select 9x' THEN (fecha_compra::date + INTERVAL '2 year')::date
                    WHEN nombre = 'Select 10x' THEN (fecha_compra::date + INTERVAL '2 year')::date
                END AS fecha_ultimo_pago,
                nombre AS tipo_select,
                CAST(REPLACE(REPLACE(valor_contrato, '$', ''), ',', '') AS NUMERIC(18,2)) AS valor_contrato,
                cantidad_de_select,
                CAST(REPLACE(REPLACE(valor_contrato, '$', ''), ',', '') AS NUMERIC(18,2)) * cantidad_de_select AS valor_total
            FROM (
                SELECT
                    max(fecha_compra) AS fecha_compra,
                    CASE
                        WHEN producto = 280 THEN 'Select 3x'
                        WHEN producto = 281 THEN 'Select 4x'
                        WHEN producto = 282 THEN 'Select 5x'
                        WHEN producto = 283 THEN 'Select 8x'
                        WHEN producto = 284 THEN 'Select 9x'
                        WHEN producto = 285 THEN 'Select 10x'
                    END AS nombre,
                    CASE
                        WHEN producto = 280 THEN '$60,000.00'
                        WHEN producto = 281 THEN '$80,000.00'
                        WHEN producto = 282 THEN '$100,000.00'
                        WHEN producto = 283 THEN '$160,000.00'
                        WHEN producto = 284 THEN '$180,000.00'
                        WHEN producto = 285 THEN '$200,000.00'
                    END AS valor_contrato,
                    COUNT(*) AS cantidad_de_select
                FROM socio_productos sp2  
                WHERE
                    cerrado IS FALSE
                    AND producto IN (280, 281, 282, 283, 284, 285)
                    AND socio IN (SELECT socio FROM socios WHERE estatus IN (1, 2))
                GROUP BY nombre, valor_contrato
            ) AS resultados
        """)

        resultados_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(resultados_origen)}")

        if not resultados_origen:
            print("ℹ No hay registros para insertar.")
            return {
                "estatus": "success",
                "tabla": "aum_bonos_select",
                "proceso": "insertar_aum_bonos_select",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LIMPIAR TABLA
        print("Limpiando tabla aum_bonos_select con TRUNCATE...")
        cur_destino.execute("TRUNCATE TABLE aum_bonos_select;")
        conn_destino.commit()

        # ---------- INSERTAR NUEVOS REGISTROS
        print("Insertando registros en aum_bonos_select")
        insert_sql = """
            INSERT INTO aum_bonos_select (
                fecha_ultimo_pago,
                tipo_select,
                valor_contrato,
                cantidad_de_select,
                valor_total
            ) VALUES %s
        """

        execute_values(cur_destino, insert_sql, resultados_origen)
        conn_destino.commit()

        registros_insertados = len(resultados_origen)
        print(f"✅ Se han insertado {registros_insertados} registros correctamente.")

        return {
            "estatus": "success",
            "tabla": "aum_bonos_select",
            "proceso": "insertar_aum_bonos_select",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "aum_bonos_select",
            "proceso": "insertar_aum_bonos_select",
            "registros_insertados": 0,
            "error_text": str(e)
        }
