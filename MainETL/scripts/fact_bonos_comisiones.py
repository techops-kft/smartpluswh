import psycopg2
from psycopg2.extras import execute_values

def insertar_fact_bonos_comisiones(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        print('Obteniendo registros de origen...')
        cur_origen.execute("""
            SELECT 
                p.pago, 
                p.socio, 
                p.monto, 
                p.monto_pagado_btc, 
                p.cotiza_btc, 
                p.estatus,
                p.fecha, 
                p.fecha_pago, 
                p.fecha_pagado, 
                p.fecha_cancelacion, 
                p.defi,
                CASE WHEN p.mtd_dsp = 2 THEN 'CNKT' ELSE 'BTC' END AS moneda,
                CASE WHEN p.pago_tx IS NOT NULL THEN p.pago_tx::integer ELSE NULL END AS pago_tx
            FROM pagos p
            LEFT JOIN "enum" e ON e.codigo = p.tipo
            WHERE
                e.categoria = 'TipoPago'
                AND p.estatus = 1;
        """)
        registros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(registros_origen)}")

        # ---------- DESTINO
        print('Obteniendo registros de destino...')
        cur_destino.execute("""
            SELECT id_retiro FROM fact_bonos_comisiones;
        """)
        ids_destino = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(ids_destino)}")

        # ---------- TRANSFORM
        print('Transformando datos para la inserción SQL...')
        registros_nuevos = [r for r in registros_origen if r[0] not in ids_destino]

        valores_a_insertar = [
            (
                r[0],  # id_retiro (pago)
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
            INSERT INTO fact_bonos_comisiones (
                id_retiro, id_socio, monto, monto_pagado_cripto, cotiza_cripto, estatus,
                fecha_solicitud, fk_fecha_solicitud, fecha_pago_estimada, fk_fecha_pago_estimada,
                fecha_pagado, fk_fecha_pagado, fecha_cancelacion, fk_fecha_cancelacion, defi,
                moneda, pago_tx
            ) VALUES %s
        """

        registros_insertados = len(valores_a_insertar)
        if valores_a_insertar:
            execute_values(cur_destino, sql, valores_a_insertar)
            conn_destino.commit()
            print(f'¡Se insertaron {registros_insertados} nuevos bonos_comisiones!')
        else:
            print('No hay bonos_comisiones nuevos para insertar.')

        return {
            "estatus": "success",
            "tabla": "fact_bonos_comisiones",
            "proceso": "insertar_fact_bonos_comisiones",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_bonos_comisiones",
            "proceso": "insertar_fact_bonos_comisiones",
            "registros_insertados": 0,
            "error_text": str(e)
        }

def actualizar_fact_bonos_comisiones(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        
        #----------EXTRACT DATA
        # 1. Obtener registros origen
        print('Obteniendo registros de origen...')
        cur_origen.execute("""
            SELECT p.pago, p.socio, p.monto, p.monto_pagado_btc, p.cotiza_btc, p.estatus,
                   p.fecha, p.fecha_pago, p.fecha_pagado, p.fecha_cancelacion, p.defi
            FROM pagos p
            LEFT JOIN "enum" e ON e.codigo = p.tipo
            WHERE
                e.categoria = 'TipoPago'
                AND p.estatus = 1;
        """)
        registros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(registros_origen)}")

        # 2. Obtener registros destino para comparación
        print('Obteniendo registros de destino...')
        cur_destino.execute("""
            SELECT id_retiro, estatus, fecha_solicitud, fecha_pago_estimada, fecha_pagado, fecha_cancelacion
            FROM fact_bonos_comisiones;
        """)
        registros_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(registros_destino)}")

        # ----------TRANSFORM DATA
        print('Transformando datos para la inserción SQL...')
        # 3. Crear diccionario destino con id_retiro como clave
        dict_destino = {
            row[0]: (
                row[1], #estatus
                row[2].date() if row[2] else None, #fecha_solicitud
                row[3].date() if row[3] else None, #fecha_pago_estimada
                row[4].date() if row[4] else None, #fecha_pagado
                row[5].date() if row[5] else None, #fecha_cancelacion
            )
            for row in registros_destino
        }

        # 4. Identificar registros que necesitan actualización
        registros_para_update = []

        for r in registros_origen:
            id_retiro = r[0]
            clave_origen = (
                r[5],  # estatus
                r[6].date() if r[6] else None, #fecha
                r[7].date() if r[7] else None, #fecha_pago
                r[8].date() if r[8] else None, #fecha_pagado
                r[9].date() if r[9] else None #fecha_cancelacion
            )

            clave_destino = dict_destino.get(id_retiro)

            #print(f'clave origen: {clave_origen} - clave destino: {clave_destino}')

            if clave_destino and clave_origen != clave_destino:
                registros_para_update.append((
                    r[5],  # estatus
                    r[6].date() if r[6] else None,  # fecha_solicitud
                    r[6].date() if r[6] else None,  # fk_fecha_solicitud
                    r[7].date() if r[7] else None,  # fecha_pago_estimada
                    r[7].date() if r[7] else None,  # fk_fecha_pago_estimada
                    r[8].date() if r[8] else None,  # fecha_pagado
                    r[8].date() if r[8] else None,  # fk_fecha_pagado
                    r[9].date() if r[9] else None,  # fecha_cancelacion
                    r[9].date() if r[9] else None,  # fk_fecha_cancelacion
                    r[10],  # defi
                    id_retiro  # condición del WHERE
                ))

        print(f'Se actualizarán {len(registros_para_update)} registros.')

        # 5. Ejecutar updates
        sql_update = """
            UPDATE fact_bonos_comisiones
            SET estatus = %s,
                fecha_solicitud = %s,
                fk_fecha_solicitud = %s,
                fecha_pago_estimada = %s,
                fk_fecha_pago_estimada = %s,
                fecha_pagado = %s,
                fk_fecha_pagado = %s,
                fecha_cancelacion = %s,
                fk_fecha_cancelacion = %s,
                defi = %s
            WHERE id_retiro = %s
        """
        registros_insertados = len(registros_para_update)
        print('Iniciando inserción SQL..')
        for row in registros_para_update:
            cur_destino.execute(sql_update, row)
            #print(row)

        conn_destino.commit()
        print(f'¡Se actualizaron {registros_insertados} exitosamente!')

        return {
            "estatus": "success",
            "tabla": "fact_bonos_comisiones",
            "proceso": "actualizar_fact_bonos_comisiones",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_bonos_comisiones",
            "proceso": "actualizar_fact_bonos_comisiones",
            "registros_insertados": 0,
            "error_text": str(e)
        }
