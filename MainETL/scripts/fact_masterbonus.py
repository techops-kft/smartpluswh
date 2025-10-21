import psycopg2
from psycopg2.extras import execute_values

def insertar_masterbonus(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        print('Obteniendo registros de origen...')
        cur_origen.execute("""
            SELECT 
                pago, 
                socio, 
                monto, 
                monto_pagado_btc, 
                cotiza_btc, 
                estatus,
                fecha, 
                fecha_pago, 
                fecha_pagado, 
                fecha_cancelacion, 
                defi,
                CASE WHEN mtd_dsp = 2 THEN 'CNKT' ELSE 'BTC' END AS moneda,
                CASE WHEN pago_tx IS NOT NULL THEN pago_tx::integer ELSE NULL END AS pago_tx
            FROM pagos 
            WHERE tipo = 4;
        """)
        registros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(registros_origen)}")

        # ---------- DESTINO
        print('Obteniendo registros de destino...')
        cur_destino.execute("""
            SELECT id_masterbonus FROM fact_masterbonus;
        """)
        ids_destino = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(ids_destino)}")

        # ---------- TRANSFORM
        print('Transformando datos para la inserción SQL...')
        registros_nuevos = [r for r in registros_origen if r[0] not in ids_destino]

        valores_a_insertar = [
            (
                r[0],  # id_masterbonus (pago)
                r[1],  # id_socio
                r[2],  # monto
                r[3],  # monto_pagado_cripto
                r[4],  # cotiza_cripto
                r[5],  # estatus
                r[6].date() if r[6] else None, r[6].date() if r[6] else None,  # fecha_solicitud, fk_fecha_solicitud
                r[7].date() if r[7] else None, r[7].date() if r[7] else None,  # fecha_pago_estimada, fk_fecha_pago_estimada
                r[8].date() if r[8] else None, r[8].date() if r[8] else None,  # fecha_pagado, fk_fecha_pagado
                r[9].date() if r[9] else None, r[9].date() if r[9] else None,  # fecha_cancelacion, fk_fecha_cancelacion
                r[10], # defi
                r[11], # moneda
                r[12]  # pago_tx
            )
            for r in registros_nuevos
        ]

        # ---------- LOAD
        print('Iniciando inserción SQL..')
        sql = """
            INSERT INTO fact_masterbonus (
                id_masterbonus, id_socio, monto, monto_pagado_cripto, cotiza_cripto, estatus,
                fecha_solicitud, fk_fecha_solicitud, fecha_pago_estimada, fk_fecha_pago_estimada,
                fecha_pagado, fk_fecha_pagado, fecha_cancelacion, fk_fecha_cancelacion, defi,
                moneda, pago_tx
            ) VALUES %s
        """

        registros_insertados = len(valores_a_insertar)
        if valores_a_insertar:
            execute_values(cur_destino, sql, valores_a_insertar)
            conn_destino.commit()
            print(f'¡Se insertaron {registros_insertados} nuevos masterbonus!')
        else:
            print('No hay masterbonus nuevos para insertar.')

        return {
            "estatus": "success",
            "tabla": "fact_masterbonus",
            "proceso": "insertar_masterbonus",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_masterbonus",
            "proceso": "insertar_masterbonus",
            "registros_insertados": 0,
            "error_text": str(e)
        }

def actualizar_masterbonus(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
                
        #----------EXTRACT DATA
        # 1. Obtener registros origen
        print('Obteniendo registros de origen...')
        cur_origen.execute("""
            SELECT pago, socio, monto, monto_pagado_btc, cotiza_btc, estatus,
                fecha, fecha_pago, fecha_pagado, fecha_cancelacion, defi
            FROM pagos 
            WHERE tipo = 4;
        """)
        registros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(registros_origen)}")

        # 2. Obtener registros destino para comparación
        print('Obteniendo registros de destino...')
        cur_destino.execute("""
            SELECT id_masterbonus, estatus, fecha_solicitud, fecha_pago_estimada, fecha_pagado, fecha_cancelacion
            FROM fact_masterbonus;
        """)
        registros_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(registros_destino)}")

        # ----------TRANSFORM DATA
        print('Transformando datos para la inserción SQL...')
        # 3. Crear diccionario destino con id_masterbonus como clave
        dict_destino = {
            row[0]: (
                row[1],  # estatus
                row[2].date() if row[2] else None,  # fecha_solicitud
                row[3].date() if row[3] else None,  # fecha_pago_estimada
                row[4].date() if row[4] else None,  # fecha_pagado
                row[5].date() if row[5] else None,  # fecha_cancelacion
            )
            for row in registros_destino
        }

        # 4. Identificar registros que necesitan actualización
        registros_para_update = []

        for r in registros_origen:
            id_masterbonus = r[0]
            clave_origen = (
                r[5],  # estatus
                r[6].date() if r[6] else None,  # fecha
                r[7].date() if r[7] else None,  # fecha_pago
                r[8].date() if r[8] else None,  # fecha_pagado
                r[9].date() if r[9] else None   # fecha_cancelacion
            )

            clave_destino = dict_destino.get(id_masterbonus)

            if clave_destino and clave_origen != clave_destino:
                registros_para_update.append((
                    id_masterbonus,  # Para la condición del JOIN
                    r[5],  # estatus
                    r[6].date() if r[6] else None,  # fecha_solicitud
                    r[6].date() if r[6] else None,  # fk_fecha_solicitud
                    r[7].date() if r[7] else None,  # fecha_pago_estimada
                    r[7].date() if r[7] else None,  # fk_fecha_pago_estimada
                    r[8].date() if r[8] else None,  # fecha_pagado
                    r[8].date() if r[8] else None,  # fk_fecha_pagado
                    r[9].date() if r[9] else None,  # fecha_cancelacion
                    r[9].date() if r[9] else None,  # fk_fecha_cancelacion
                    r[10]  # defi
                ))

        print(f'Se actualizarán {len(registros_para_update)} registros.')

        if registros_para_update:
            # 5. Crear tabla temporal
            print('Creando tabla temporal...')
            cur_destino.execute("""
                CREATE TEMP TABLE tmp_masterbonus_update (
                    id_masterbonus BIGINT,
                    estatus INTEGER,
                    fecha_solicitud DATE,
                    fk_fecha_solicitud DATE,
                    fecha_pago_estimada DATE,
                    fk_fecha_pago_estimada DATE,
                    fecha_pagado DATE,
                    fk_fecha_pagado DATE,
                    fecha_cancelacion DATE,
                    fk_fecha_cancelacion DATE,
                    defi BOOLEAN
                ) ON COMMIT DROP;
            """)

            # 6. Insertar registros en la tabla temporal usando execute_values
            print('Insertando registros en tabla temporal...')
            insert_sql = """
                INSERT INTO tmp_masterbonus_update (
                    id_masterbonus, estatus, fecha_solicitud, fk_fecha_solicitud,
                    fecha_pago_estimada, fk_fecha_pago_estimada,
                    fecha_pagado, fk_fecha_pagado,
                    fecha_cancelacion, fk_fecha_cancelacion, defi
                ) VALUES %s
            """
            execute_values(cur_destino, insert_sql, registros_para_update, page_size=1000)

            # 7. Actualizar registros en bloque usando JOIN
            print('Actualizando registros en fact_masterbonus...')
            update_sql = """
                UPDATE fact_masterbonus AS f
                SET estatus = t.estatus,
                    fecha_solicitud = t.fecha_solicitud,
                    fk_fecha_solicitud = t.fk_fecha_solicitud,
                    fecha_pago_estimada = t.fecha_pago_estimada,
                    fk_fecha_pago_estimada = t.fk_fecha_pago_estimada,
                    fecha_pagado = t.fecha_pagado,
                    fk_fecha_pagado = t.fk_fecha_pagado,
                    fecha_cancelacion = t.fecha_cancelacion,
                    fk_fecha_cancelacion = t.fk_fecha_cancelacion,
                    defi = t.defi
                FROM tmp_masterbonus_update AS t
                WHERE f.id_masterbonus = t.id_masterbonus;
            """
            cur_destino.execute(update_sql)

            conn_destino.commit()
            reigstros_insertados = len(registros_para_update) 
            print(f'¡Se actualizaron {reigstros_insertados} exitosamente!')
            return {
            "estatus": "success",
            "tabla": "fact_masterbonus",
            "proceso": "actualizar_masterbonus",
            "registros_insertados": reigstros_insertados,
            "error_text": "No error"
            }

        else:
            print('No hay registros para actualizar.')
            return {
                "estatus": "success",
                "tabla": "fact_masterbonus",
                "proceso": "actualizar_masterbonus",
                "registros_insertados": 0,
                "error_text": "No error"
            }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_masterbonus",
            "proceso": "actualizar_masterbonus",
            "registros_insertados": 0,
            "error_text": str(e)
        }
