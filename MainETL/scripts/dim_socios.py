import psycopg2
from psycopg2.extras import execute_values

def insertar_socios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        #----------EXTRACT DATA
        # 1. Obtener todos los registros del origen
        cur_origen.execute("""
            SELECT 
                s.socio, s.nombre, s.apellido_pat, s.apellido_mat, s.email, 
                s.estatus, s.padre, s.fecha_ingreso, s.fecha_vigencia_membresia,
                vs.rango, si.pais, p.nombre, 
                (fn_check_validity_membership(s.socio)).estatus_membresia,
                vs.saldo_organizacion, vs.saldo_personal, vs.directos, vs.directos_plan
            FROM socios s 
            LEFT JOIN v_socio_rangos_defi vs ON s.socio = vs.socio 
            LEFT JOIN socio_info si ON s.socio = si.socio 
            LEFT JOIN paises p ON si.pais = p.pais 
            ORDER BY s.fecha_ingreso ASC;
        """)
        socios_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(socios_origen)}")

        # 2. Obtener todos los id_socio del destino
        cur_destino.execute("SELECT id_socio FROM dim_socios;")
        socios_destino = {row[0] for row in cur_destino.fetchall()}
        print(f"Registros en destino: {len(socios_destino)}")

        # declaracion de variables
        estatus_activo_array = [1, 2, 3, 7]
        nuevos_socios = []

        # ----------TRANSFORM DATA
        # 3. Filtrar los que no existen en destino
        print('Transformando datos ...')
        for socio in socios_origen:
            id_socio = socio[0]
            if id_socio in socios_destino:
                continue

            estatus_membresia_funcion = socio[12]
            estatus_final = 'Activo' if estatus_membresia_funcion in estatus_activo_array else 'Inactivo'

            valores = (
                socio[0],  # id_socio
                socio[1],  # nombre
                socio[2],  # apellido_paterno
                socio[3],  # apellido_materno
                socio[4],  # email
                socio[5],  # estatus
                socio[6],  # padre
                socio[9],  # rango_actual
                socio[7],  # fecha_ingreso
                socio[8],  # fecha_vigencia_membresia
                socio[10], # id_pais
                socio[11], # nombre_pais
                estatus_final,
                socio[13], # saldo_organizacion
                socio[14], # saldo_personal
                socio[15], # directos
                socio[16], # directos_plan
            )
            nuevos_socios.append(valores)

        registros_insertados = len(nuevos_socios)

        # ----------LOAD DATA
        # 4. Inserción en lote usando execute_values
        print(f"Cargando {registros_insertados} datos ...")
        if nuevos_socios:
            sql = """
                INSERT INTO dim_socios(
                    id_socio, nombre, apellido_paterno, apellido_materno, email, estatus,
                    id_padre, id_rango_actual, fecha_ingreso, fecha_vigencia_membresia,
                    id_pais, nombre_pais, estatus_membresia_funcion,
                    saldo_organizacion, saldo_personal, socios_directos, socios_directos_plan
                ) VALUES %s
            """
            
            execute_values(cur_destino, sql, nuevos_socios, page_size=1000) #el page size es un iterador automatico
            conn_destino.commit()

            print(f"¡Se han insertado {registros_insertados} socios exitosamente!.")
        
        else:
            print("No hay nuevos socios por insertar.")

        return {
            "estatus": "success",
            "tabla": "dim_socios",
            "proceso": "insertar_socios",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_socios",
            "proceso": "insertar_socios",
            "registros_insertados": 0,
            "error_text": str(e)
        }

def actualizar_socios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        #----------EXTRACT DATA
        # 1. Obtener datos de origen
        cur_origen.execute("""
            SELECT 
                s.socio as id_socio, 
                s.fecha_ingreso, 
                (fn_check_validity_membership(s.socio)).estatus_membresia as estatus_membresia_funcion, 
                s.estatus, 
                s.fecha_vigencia_membresia, 
                vs.rango AS id_rango_actual,
                vs.saldo_organizacion,
                vs.saldo_personal,
                vs.directos,
                vs.directos_plan
            FROM socios s 
            LEFT JOIN v_socio_rangos_defi vs ON s.socio = vs.socio
            ORDER BY s.fecha_ingreso ASC;
        """)
        socios_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(socios_origen)}")
        
        # 2. Obtener datos de destino
        cur_destino.execute("""
            SELECT 
                id_socio, 
                fecha_vigencia_membresia,
                saldo_organizacion,
                saldo_personal,
                socios_directos,
                socios_directos_plan,
                id_rango_actual
            FROM dim_socios;
        """)
        socios_destino = cur_destino.fetchall()
        print(f"Registros en destino: {len(socios_destino)}")

        #----------TRANSFORM DATA
        # 3. Convertir resultados de destino en diccionario para comparación rápida
        print('Transformando datos ...')
        mapa_destino = {
            s[0]: {
                'fecha_vigencia_membresia': s[1],
                'saldo_organizacion': s[2],
                'saldo_personal': s[3],
                'socios_directos': s[4],
                'socios_directos_plan': s[5],
                'id_rango_actual': s[6]
            }
            for s in socios_destino
        }

        socios_diferentes = []
        for socio in socios_origen:
            id_socio = socio[0]
            fecha_vigencia_origen = socio[4]
            id_rango_actual = socio[5]
            saldo_org_origen = socio[6]
            saldo_per_origen = socio[7]
            directos_origen = socio[8]
            directos_plan_origen = socio[9]

            datos_destino = mapa_destino.get(id_socio)

            if not datos_destino:
                continue

            # 4. Comprobar diferencias entre registros
            if (
                fecha_vigencia_origen != datos_destino['fecha_vigencia_membresia'] or
                saldo_org_origen != datos_destino['saldo_organizacion'] or
                saldo_per_origen != datos_destino['saldo_personal'] or
                directos_origen != datos_destino['socios_directos'] or
                directos_plan_origen != datos_destino['socios_directos_plan'] or
                id_rango_actual != datos_destino['id_rango_actual']
            ):
                socios_diferentes.append(socio)
                #print(f'Socio diferente: {id_socio}')

        registros_insertados = len(socios_diferentes)
        print(f"Se actualizarán {registros_insertados} socios.")

        estatus_activo_array = [1, 2, 3, 7]

        #----------LOAD DATA
        if socios_diferentes:
            print("Iniciando actualización con tabla temporal...")

            # 1. Preparar los datos para la tabla temporal
            datos_update = []
            estatus_activo_array = [1, 2, 3, 7]
            for socio in socios_diferentes:
                id_socio = socio[0]
                fecha_ingreso = socio[1]
                estatus_membresia_funcion = socio[2]
                estatus = socio[3]
                fecha_vigencia_membresia = socio[4]
                rango_actual = socio[5]
                saldo_organizacion = socio[6]
                saldo_personal = socio[7]
                directos = socio[8]
                directos_plan = socio[9]

                estatus_membresia_str = 'Activo' if estatus_membresia_funcion in estatus_activo_array else 'Inactivo'

                datos_update.append((
                    id_socio,
                    estatus_membresia_str,
                    estatus,
                    fecha_vigencia_membresia,
                    rango_actual,
                    saldo_organizacion,
                    saldo_personal,
                    directos,
                    directos_plan
                ))

            # 2. Crear tabla temporal
            cur_destino.execute("""
                CREATE TEMP TABLE tmp_socios_update (
                    id_socio INTEGER,
                    estatus_membresia_funcion TEXT,
                    estatus INTEGER,
                    fecha_vigencia_membresia DATE,
                    id_rango_actual INTEGER,
                    saldo_organizacion MONEY,
                    saldo_personal MONEY,
                    socios_directos INTEGER,
                    socios_directos_plan INTEGER
                ) ON COMMIT DROP;
            """)

            # 3. Insertar datos en la tabla temporal con execute_values
            execute_values(
                cur_destino,
                """
                INSERT INTO tmp_socios_update (
                    id_socio, estatus_membresia_funcion, estatus,
                    fecha_vigencia_membresia, id_rango_actual,
                    saldo_organizacion, saldo_personal,
                    socios_directos, socios_directos_plan
                ) VALUES %s
                """,
                datos_update
            )

            # 4. Ejecutar UPDATE ... FROM con la tabla temporal
            cur_destino.execute("""
                UPDATE dim_socios d
                SET 
                    estatus_membresia_funcion = t.estatus_membresia_funcion,
                    estatus = t.estatus,
                    fecha_vigencia_membresia = t.fecha_vigencia_membresia,
                    id_rango_actual = t.id_rango_actual,
                    saldo_organizacion = t.saldo_organizacion,
                    saldo_personal = t.saldo_personal,
                    socios_directos = t.socios_directos,
                    socios_directos_plan = t.socios_directos_plan
                FROM tmp_socios_update t
                WHERE d.id_socio = t.id_socio;
            """)

            conn_destino.commit()
            print("\n\nSe han actualizado los socios con tabla temporal.")
        else:
            print("No hay socios para actualizar.")

        return {
            "estatus": "success",
            "tabla": "dim_socios",
            "proceso": "actualizar_socios",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_socios",
            "proceso": "actualizar_socios",
            "registros_insertados": 0,
            "error_text": str(e)
        }        