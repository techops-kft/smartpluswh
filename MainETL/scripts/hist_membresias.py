import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

def insertar_hist_membresias(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ------- EXTARCT DATA
        # 1. Obtener todos los registros actuales de dim_socios
        cur_destino.execute("""
            SELECT 
                id_socio,
                estatus,
                fecha_ingreso,
                fecha_vigencia_membresia
            FROM dim_socios
        """)
        socios_actuales = cur_destino.fetchall()
        print(f"Registros actuales de dim_socios: {len(socios_actuales)}")

        # 2. Obtener el último registro en hist_membresias para todos los socios
        cur_destino.execute("""
            SELECT DISTINCT ON (id_socio)
                id_socio,
                estatus,
                fecha_vigencia_membresia
            FROM hist_membresias
            ORDER BY id_socio, fecha_registro DESC
        """)
        historial_ultimo = cur_destino.fetchall()
        print(f"Últimos registros de hist_membresias: {len(historial_ultimo)}")

        # Crear diccionario para búsqueda rápida
        hist_dict = {row[0]: (row[1], row[2]) for row in historial_ultimo}


        # ------- TRANFORM DATA
        # 3. Comparar y preparar nuevos registros
        nuevos_registros = []
        fecha_actual = datetime.now()

        for socio in socios_actuales:
            id_socio = socio[0]
            estatus_actual = socio[1]
            fecha_ingreso = socio[2]
            fecha_vigencia = socio[3]

            ultimo = hist_dict.get(id_socio)
            insertar = False

            if not ultimo:
                insertar = True
            else:
                estatus_anterior, fecha_vigencia_anterior = ultimo
                if estatus_anterior != estatus_actual or fecha_vigencia_anterior != fecha_vigencia:
                    insertar = True

            if insertar:
                nuevos_registros.append((
                    id_socio,
                    estatus_actual,
                    fecha_ingreso,
                    fecha_vigencia,
                    fecha_actual
                ))
                #print(f"    ➤ id_socio '{id_socio}' → estatus '{estatus_actual}'")

        print(f"Registros nuevos para insertar: {len(nuevos_registros)}")


        # -------- LOAD DATA
        # 4. Insertar los nuevos registros si hay
        if nuevos_registros:
            execute_values(cur_destino, """
                INSERT INTO hist_membresias (
                    id_socio,
                    estatus,
                    fecha_ingreso,
                    fecha_vigencia_membresia,
                    fecha_registro
                ) VALUES %s
            """, nuevos_registros)
            conn_destino.commit()
            registros_insertados = len(nuevos_registros)
            print(f"Insertados correctamente {len(nuevos_registros)} registros en hist_membresias.")
        else:
            registros_insertados = 0
            print("No hay cambios nuevos que registrar.")

        return {
            "estatus": "success",
            "tabla": "hist_membresias",
            "proceso": "insertar_hist_membresias",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "hist_membresias",
            "proceso": "insertar_hist_membresias",
            "registros_insertados": 0,
            "error_text": str(e)
        }