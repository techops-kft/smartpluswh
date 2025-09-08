import psycopg2
from psycopg2.extras import execute_values
from collections import OrderedDict

def insertar_primerplatinum(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        #----------EXTRACT DATA
        # 1. Obtener todos los registros del origen
        cur_origen.execute("""
            SELECT
                s.socio                                 AS cliente_id,
                x.socio                                 AS id_primer_platinum,
                x.nombre                                AS nombre_primer_platinum,
                CASE
                    WHEN x.socio IN (106196,105573,105582,106482,106538,107120,107237,107247,
                                     107381,107401,108506,244574,245436,252306,254348,283776,318784) THEN 'Norte'
                    WHEN x.socio IN (101000,107166,252612,255342)                                     THEN 'CDMX'
                    WHEN x.socio IN (107432,107434,107435,108149,110780,111128,112300,112448)         THEN 'Centro'
                    WHEN x.socio IN (252848)                                                         THEN 'Europa'
                    WHEN x.socio IN (107870,107922,107959,109114)                                   THEN 'Perú'
                    WHEN x.socio IN (107142)                                                        THEN 'Sur'
                    WHEN x.socio IN (105574,105613,111796,112228,256458,291770,295786,309442)        THEN 'USA'
                    ELSE 'Sin Region'
                END                                           AS region_de_socio
            FROM public.socios AS s
            LEFT JOIN LATERAL (
            	SELECT sr.rango,net.socio,net.padre,net.nombre
            	FROM public.fn_get_invert_partner_network(s.socio) AS net
            	JOIN public.v_socio_rangos sr ON sr.socio = net.socio
            	JOIN process.rangos r ON (r.rango = sr.rango AND r."version" = sr."version")
            	JOIN socio_info si on si.socio = net.socio and si.lista_negra is false
            	where ((sr.rango = 18 AND sr.version = 2021) OR (sr.rango = 19 AND sr.version = 2024)) AND net.socio != s.socio
            	ORDER BY net.nivel ASC
            	LIMIT 1
            ) x ON TRUE
            where s.estatus in (1,2)
            ORDER BY cliente_id DESC;
        """)
        socios_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(socios_origen)}")


        # 2. Obtener todos los registros del origen
        cur_destino.execute("SELECT id_socio FROM dim_primerplatinum")
        socios_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(socios_existentes)}")

        # ----------TRANSFORM DATA
        #3.  Filtrar y deduplicar socios
        socios_dict = OrderedDict()
        for row in socios_origen:
            id_socio = row[0]
            if id_socio not in socios_existentes and id_socio not in socios_dict:
                socios_dict[id_socio] = row  # Solo la primera ocurrencia

        nuevos_socios = list(socios_dict.values())

        if not nuevos_socios:
            print("ℹNo hay nuevos socios para insertar.")
            return {
                "estatus": "success",
                "tabla": "dim_primerplatinum",
                "proceso": "insertar_primerplatinum",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ----------LOAD DATA
        # 4. SQL de inserción
        print(f"Insertando registros a la BD dim_primerplatinum")
        insert_sql = """
            INSERT INTO dim_primerplatinum (
                id_socio, id_platinum, nombre_platinum, region
            ) VALUES %s
        """
        execute_values(cur_destino, insert_sql, nuevos_socios)

        conn_destino.commit()
        registros_insertados = len(nuevos_socios)
        print(f"Se han insertado {registros_insertados} nuevos socios correctamente.")

        return {
            "estatus": "success",
            "tabla": "dim_primerplatinum",
            "proceso": "insertar_primerplatinum",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_primerplatinum",
            "proceso": "insertar_primerplatinum",
            "registros_insertados": 0,
            "error_text": str(e)
        }
    
def actualizar_primerplatinum(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ----------EXTRACT DATA
        # 1. Obtener datos de origen
        cur_origen.execute("""
            SELECT
                s.socio                                 AS cliente_id,
                x.socio                                 AS id_primer_platinum,
                x.nombre                                AS nombre_primer_platinum,
                CASE
                    WHEN x.socio IN (106196,105573,105582,106482,106538,107120,107237,107247,
                                     107381,107401,108506,244574,245436,252306,254348,283776,318784) THEN 'Norte'
                    WHEN x.socio IN (101000,107166,252612,255342)                                     THEN 'CDMX'
                    WHEN x.socio IN (107432,107434,107435,108149,110780,111128,112300,112448)         THEN 'Centro'
                    WHEN x.socio IN (252848)                                                         THEN 'Europa'
                    WHEN x.socio IN (107870,107922,107959,109114)                                   THEN 'Perú'
                    WHEN x.socio IN (107142)                                                        THEN 'Sur'
                    WHEN x.socio IN (105574,105613,111796,112228,256458,291770,295786,309442)        THEN 'USA'
                    ELSE 'Sin Region'
                END                                           AS region_de_socio
            FROM public.socios AS s
            LEFT JOIN LATERAL (
            	SELECT sr.rango,net.socio,net.padre,net.nombre
            	FROM public.fn_get_invert_partner_network(s.socio) AS net
            	JOIN public.v_socio_rangos sr ON sr.socio = net.socio
            	JOIN process.rangos r ON (r.rango = sr.rango AND r."version" = sr."version")
            	JOIN socio_info si on si.socio = net.socio and si.lista_negra is false
            	where ((sr.rango = 18 AND sr.version = 2021) OR (sr.rango = 19 AND sr.version = 2024)) AND net.socio != s.socio
            	ORDER BY net.nivel ASC
            	LIMIT 1
            ) x ON TRUE
            where s.estatus in (1,2)
            ORDER BY cliente_id DESC;
        """)
        socios_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(socios_origen)}")

        # 2. Obtener datos de destino
        cur_destino.execute("""
            SELECT 
                id_socio,
                id_platinum,
                nombre_platinum,
                region
            FROM dim_primerplatinum;
        """)
        socios_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(socios_destino)}")

        # ----------TRANSFORM DATA
        # 3. Mapeo destino para comparación
        mapa_destino = {s[0]: (s[1], s[2], s[3]) for s in socios_destino}

        actualizaciones = []

        for socio in socios_origen:
            id_socio = socio[0]
            id_platinum = socio[1]
            nombre_platinum = socio[2]
            region = socio[3]

            datos_destino = mapa_destino.get(id_socio)

            if datos_destino:
                id_platinum_dest, nombre_dest, region_dest = datos_destino
                if (
                    id_platinum != id_platinum_dest or
                    nombre_platinum != nombre_dest or
                    region != region_dest
                ):
                    actualizaciones.append((id_socio, id_platinum, nombre_platinum, region))
                    #print(f'Socio: {id_socio} | ' f'id_platinum-old: {id_platinum_dest} → new: {id_platinum}, ' f'nombre-old: {nombre_dest} → new: {nombre_platinum}, ' f'region-old: {region_dest} → new: {region}')

        # ----------LOAD DATA
        # 4. Ejecutar actualización en bloque
        if actualizaciones:
            update_sql = """
                WITH datos (id_socio, id_platinum, nombre_platinum, region) AS (
                    VALUES %s
                )
                UPDATE dim_primerplatinum dpp
                SET 
                    id_platinum = datos.id_platinum,
                    nombre_platinum = datos.nombre_platinum,
                    region = datos.region
                FROM datos
                WHERE dpp.id_socio = datos.id_socio;
            """
            execute_values(cur_destino, update_sql, actualizaciones)
            conn_destino.commit()
            registros_insertados = len(actualizaciones)
            print(f"Se han actualizado {registros_insertados} socios correctamente.")
        else:
            registros_insertados = 0
            print("No hubo socios por actualizar.")

        return  {
            "estatus": "success",
            "tabla": "dim_primerplatinum",
            "proceso": "actualizar_primerplatinum",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_primerplatinum",
            "proceso": "actualizar_primerplatinum",
            "registros_insertados": 0,
            "error_text": str(e)
        }
