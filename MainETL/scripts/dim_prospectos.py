import psycopg2
from psycopg2.extras import execute_values

def insertar_prospectos(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
         #----------EXTRACT DATA
        # 1. Obtener todos los registros del origen
        cur_origen.execute("""
            SELECT
                p.prospecto,
                p.tipo,
                p.nombre,
                p.apellido_pat,
                p.apellido_mat,
                p.padre,
                p.cultura,
                p.nuevo_socio,
                pi.int_formacion,
                pi.int_ahorro,
                pi.int_networking,
                pi.int_emprendimiento,
                pi.pais,
                pi.genero,
                ps.nombre,
                p.fecha_insert
            FROM prospectos p 
            LEFT JOIN prospecto_info pi ON pi.prospecto = p.prospecto 
            LEFT JOIN paises ps ON pi.pais = ps.pais  
            ORDER BY p.fecha_insert ASC;
        """)
        prospectos_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(prospectos_origen)}")


        # 2. Obtener todos los registros del origen
        cur_destino.execute("SELECT id_prospecto FROM dim_prospectos")
        prospectos_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(prospectos_existentes)}")


        # ----------TRANSFORM DATA
        # 3. Filtrar solo los prospectos que no existen aún
        nuevos_prospectos = [
            row for row in prospectos_origen if row[0] not in prospectos_existentes
        ]

        if not nuevos_prospectos:
            print("ℹNo hay nuevos prospectos para insertar.")
            return {
            "estatus": "success",
            "tabla": "dim_prospectos",
            "proceso": "insertar_prospectos",
            "registros_insertados": 0,
            "error_text": "No error"
            }

        # ----------LOAD DATA
        # 4. SQL de inserción optimizada
        insert_sql = """
            INSERT INTO dim_prospectos (
                id_prospecto, tipo, nombre, apellido_pat, apellido_mat,
                id_socio_padre, lenguaje, id_socio, interes_formacion, interes_ahorro,
                interes_networking, interes_emprendimiento, pais, genero, nombre_pais, fecha_registro
            ) VALUES %s
        """
        execute_values(cur_destino, insert_sql, nuevos_prospectos)
        conn_destino.commit()

        registros_insertados = len(nuevos_prospectos)
        print(f"Se han insertado {registros_insertados} nuevos prospectos correctamente.")

        return {
            "estatus": "success",
            "tabla": "dim_prospectos",
            "proceso": "insertar_prospectos",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_prospectos",
            "proceso": "insertar_prospectos",
            "registros_insertados": 0,
            "error_text": str(e)
        }
    
def actualizar_prospectos(cur_origen, conn_origen, cur_destino, conn_destino):
    try:

        # ----------EXTRACT DATA
        # 1. Obtener prospectos desde origen
        cur_origen.execute("""
            SELECT
                p.prospecto, p.tipo, p.nombre, p.apellido_pat, p.apellido_mat,
                p.email, p.padre, p.cultura, p.nuevo_socio, pi.int_formacion,
                pi.int_ahorro, pi.int_networking, pi.int_emprendimiento, pi.pais, pi.genero
            FROM prospectos p
            JOIN prospecto_info pi ON pi.prospecto = p.prospecto
            ORDER BY p.fecha_insert ASC;
        """)
        prospectos_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(prospectos_origen)}")

        # 2. Obtener prospectos desde destino
        cur_destino.execute("""
            SELECT 
                id_prospecto, id_socio, tipo, id_socio_padre
            FROM dim_prospectos;
        """)
        prospectos_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(prospectos_destino)}")

        # ----------EXTRACT DATA
        # 3. Mapeo rápido para comparar
        mapa_destino = {s[0]: (s[1], s[2], s[3]) for s in prospectos_destino}
        actualizaciones = []

        for prospecto in prospectos_origen:
            id_prospecto = prospecto[0]
            tipo_origen = prospecto[1]
            padre_origen = prospecto[6]
            nuevo_socio = prospecto[8]

            datos_destino = mapa_destino.get(id_prospecto)

            if datos_destino:
                id_socio_destino, tipo_destino, padre_destino = datos_destino

                if (
                    id_socio_destino != nuevo_socio or
                    tipo_destino != tipo_origen or
                    padre_destino != padre_origen
                ):
                    actualizaciones.append((id_prospecto, nuevo_socio, tipo_origen, padre_origen))
                    #print(f'Prospecto: {id_prospecto} | ' f'id_socio-old: {id_socio_destino} → new: {nuevo_socio}, ' f'tipo-old: {tipo_destino} → new: {tipo_origen}, ' f'padre-old: {padre_destino} → new: {padre_origen}')

        # 4. Ejecutar actualización en bloque si hay datos
        if actualizaciones:
            query = """
                WITH datos (id_prospecto, id_socio, tipo, id_socio_padre) AS (
                    VALUES %s
                )
                UPDATE dim_prospectos dp
                SET 
                    id_socio = datos.id_socio,
                    tipo = datos.tipo,
                    id_socio_padre = datos.id_socio_padre
                FROM datos
                WHERE dp.id_prospecto = datos.id_prospecto;
            """
            execute_values(cur_destino, query, actualizaciones)
            conn_destino.commit()
            registros_insertados = len(actualizaciones)
            print(f"Se actualizaron {registros_insertados} prospectos.")
        else:
            print("No hubo prospectos por actualizar.")
            registros_insertados = 0

        return {
            "estatus": "success",
            "tabla": "dim_prospectos",
            "proceso": "actualizar_prospectos",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_prospectos",
            "proceso": "actualizar_prospectos",
            "registros_insertados": 0,
            "error_text": str(e)
        }