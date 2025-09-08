import psycopg2
from psycopg2.extras import execute_values

def insertar_retiros(cur_origen, conn_origen, cur_destino, conn_destino):
    try:

        #----------EXTRACT DATA
        # 1. Obtener todos los registros del origen
        cur_origen.execute("""
            SELECT
                pago, socio, tipo, monto, monto_pagado_btc, cotiza_btc, estatus, fecha, fecha_pago, fecha_pagado, fecha_cancelacion, defi
            FROM 
                pagos
            WHERE tipo = 0 and socio != 28642
            ORDER BY pago DESC;
        """)
        retiros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(retiros_origen)}")        

        # 2. Obtener todos los registros de destino
        cur_destino.execute("""
            SELECT 
                id_retiro
            FROM fact_retiros
            ORDER BY id_retiro DESC;
        """)
        retiros_destino = {r[0] for r in cur_destino.fetchall()}
        print(f"Registros destino: {len(retiros_destino)}")

        # ----------TRANSFORM DATA
        # 3. Filtrar los que no existen en destino
        print("Transformando datos...")
        nuevos_retiros = []

        for retiro in retiros_origen:
            id_retiro = retiro[0]
            if id_retiro in retiros_destino:
                continue
            
            valores= (
                retiro[0], # id_retiro
                retiro[1], # id_socio
                retiro[2], # tipo
                retiro[3], # monto
                retiro[4], # monto_pagado_btc
                retiro[5], # cotiza_btc
                retiro[6], # estatus
                retiro[7], # fecha
                retiro[8], # fecha_pago
                retiro[9], # fecha_pagado
                retiro[10], # fecha_cancelacion
                retiro[11], # defi
            )
            nuevos_retiros.append(valores)
            #print(f'Retiro:  {id_retiro}')

        registros_insertados = len(nuevos_retiros)
    
        # ----------LOAD DATA
        # 4. Inserción en lote usando execute_values
        print(f"Cargando datos {registros_insertados} ...")

        if nuevos_retiros:
            sql = """
                INSERT INTO fact_retiros(
                    id_retiro, id_socio, tipo, monto, monto_pagado_cripto, cotiza_cripto, estatus, fecha_solicitud, fecha_pago_estimada, fecha_pago, fecha_cancelacion, defi
                ) VALUES %s
            """
            execute_values(cur_destino, sql, nuevos_retiros, page_size=1000) #el page size es un iterador automatico
            conn_destino.commit()

            print(f"¡Se han insertado {registros_insertados} retiros exitosamente!.")
        
        else:
            print("No hay nuevos retiros por insertar.")    

        return {
            "estatus": "success",
            "tabla": "fact_retiros",
            "proceso": "insertar_retiros",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_retiros",
            "proceso": "insertar_retiros",
            "registros_insertados": 0,
            "error_text": str(e)
        }
    
def actualizar_retiros(cur_origen, conn_origen, cur_destino, conn_destino):
    try:

        #----------EXTRACT DATA
        # 1. Obtener todos los registros del origen
        cur_origen.execute("""
            SELECT estatus, fecha, fecha_pago, fecha_pagado, fecha_cancelacion, pago, fecha_insert 
            FROM pagos 
            WHERE tipo = 0
            ORDER BY fecha_insert ASC
        """)
        retiros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(retiros_origen)}")

        # 2. Obtener datos de destino
        cur_destino.execute("""
            SELECT id_retiro, fecha_pago, fecha_cancelacion, fecha_pago_estimada 
            FROM fact_retiros 
        """)
        retiros_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(retiros_destino)}")

        #----------TRANSFORM DATA
        # 3. Transformacion de los datos para la insercion

        mapa_destino = {r[0]: (r[1], r[2], r[3]) for r in retiros_destino}  # id_retiro: (fecha_pago, fecha_cancelacion, fecha_pago_estimada)
        actualizados = []

        for retiro in retiros_origen:
            estatus = retiro[0]
            fecha_solicitud = retiro[1]
            fecha_pago_estimada = retiro[2]
            fecha_pago = retiro[3]
            fecha_cancelacion = retiro[4]
            id_retiro = retiro[5]
            fecha_insert = retiro[6]

            fechas_destino = mapa_destino.get(id_retiro)

            if fechas_destino:
                pago_destino, cancelacion_destino, estimada_destino = fechas_destino

                # Solo actualiza si hay diferencia
                if (
                    fecha_pago != pago_destino or 
                    fecha_cancelacion != cancelacion_destino or 
                    fecha_pago_estimada != estimada_destino
                ):
                    query = """
                        UPDATE fact_retiros 
                        SET estatus = %s, 
                            fecha_solicitud = %s, 
                            fecha_pago = %s, 
                            fecha_pago_estimada = %s, 
                            fecha_cancelacion = %s 
                        WHERE id_retiro = %s
                    """
                    values = (estatus, fecha_solicitud, fecha_pago, fecha_pago_estimada, fecha_cancelacion, id_retiro)
                    cur_destino.execute(query, values)
                    actualizados.append(retiro)
                    #print(f'Actualizar retiro: {id_retiro} Fecha Insert: {fecha_insert}')

        # Commit final
        conn_destino.commit()
        
        registros_insertados = len(actualizados)
        print(f"Se han actualizado {registros_insertados} retiros.\n")

        return {
            "estatus": "success",
            "tabla": "fact_retiros",
            "proceso": "aztualizar_retiros",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_retiros",
            "proceso": "aztualizar_retiros",
            "registros_insertados": 0,
            "error_text": str(e)
        }