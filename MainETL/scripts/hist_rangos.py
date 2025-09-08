import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

def insertar_hist_rangos(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ------- EXTRACT DATA
        # 1. Obtener todos los registros actuales de dim_socios
        cur_destino.execute("""
            SELECT 
                id_socio,
                id_rango_actual
            FROM dim_socios
            WHERE id_rango_actual != 0
        """)
        socios_actuales = cur_destino.fetchall()
        print(f"Registros actuales de dim_socios {len(socios_actuales)}")

        # 2. Obtener el último registro de hist_rangos para cada socio
        cur_destino.execute("""
            SELECT DISTINCT ON (id_socio)
                id_socio,
                id_rango
            FROM hist_rangos
            ORDER BY id_socio, fecha_registro DESC
        """)
        historial_ultimo = cur_destino.fetchall()
        print(f"Últimos registros de hist_rangos: {len(historial_ultimo)}")

        # -------- TRANSFORM DATA
        # Convertir en diccionario para acceso rápido
        hist_dict = {row[0]: row[1] for row in historial_ultimo}

        nuevos_registros = []
        fecha_actual = datetime.now()

        # 3. Comparar rangos actuales con históricos
        for socio in socios_actuales:
            id_socio = socio[0]
            id_rango_actual = socio[1]

            id_rango_anterior = hist_dict.get(id_socio)
            insertar = False

            if id_rango_anterior is None or id_rango_anterior != id_rango_actual:
                insertar = True

            if insertar:
                nuevos_registros.append((
                    id_socio,
                    id_rango_actual,
                    fecha_actual
                ))
                #print(f"    ➤ id_socio '{id_socio}' → rango '{id_rango_actual}'")

        print(f"Registros nuevos para insertar: {len(nuevos_registros)}")

        # ------- LOAD DATA
        # 4. Insertar nuevos registros si los hay
        if nuevos_registros:
            execute_values(cur_destino, """
                INSERT INTO hist_rangos (
                    id_socio,
                    id_rango,
                    fecha_registro
                ) VALUES %s
            """, nuevos_registros)
            conn_destino.commit()
            registros_insertados = len(nuevos_registros)
            print(f"Insertados correctamente {registros_insertados} registros en hist_rangos.")
        else:
            registros_insertados = 0
            print("No hay cambios nuevos que registrar.")

        return {
            "estatus": "success",
            "tabla": "hist_rangos",
            "proceso": "insertar_hist_rangos",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "hist_rangos",
            "proceso": "insertar_hist_rangos",
            "registros_insertados": 0,
            "error_text": str(e)
        }