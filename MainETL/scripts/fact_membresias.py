import psycopg2
from psycopg2.extras import execute_values

def insertar_membresias(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ------ EXTRACT DATA
        # 1. Obtener todos los IDS de compra del origen
        cur_origen.execute("""
            SELECT
                c.compra,
                c.socio,
                c.prospecto,
                c.fecha_pago,
                CASE
                    when c.forma_pago = 1 then 'BTC'
                    when c.forma_pago = 2 and c.entidad_pago = 1            then 'FIAT - OpenPay'
                    when c.forma_pago = 2 and c.entidad_pago = 2            then 'FIAT - Banorte'
                    when c.forma_pago = 3 and c.referencia ilike 'TRX-%'    then 'FLEXPAY - TRX_CNKT_POL'
                    when c.forma_pago = 3 and c.referencia3 = 'BTC'     then 'FLEXPAY - BTC'
                    when c.forma_pago = 3 and c.referencia3 = 'CNKT_POL'    then 'FLEXPAY - CNKT_POL'
                    when c.forma_pago = 3 and c.referencia3 = 'USDT_TRON'   then 'FLEXPAY - USDT_TRON'
                    when c.forma_pago = 0 then 'Bono único'
                    when c.forma_pago = 6 THEN 'Transferencia'
                    when  c.forma_pago= 4 then 'Polygon - CNKT'
                    when  c.forma_pago= 5 then 'Polygon - BTC'
                    when  c.forma_pago= 7 then 'Polygon - USDT'
                    when  c.forma_pago= 8 then 'FIAT - STRIPE'
                    else 'x'
                END AS forma_pago,
                CASE
                    WHEN c.estatus = -2 THEN 'Estatus manual de cambio'
                    WHEN c.estatus = -1 THEN 'Cancelado en Flexbit'
                    WHEN c.estatus = 0 THEN 'Creado'
                    WHEN c.estatus = 1 THEN 'Pendiente'
                    WHEN c.estatus = 2 THEN 'En proceso'
                    WHEN c.estatus = 3 THEN 'Pago incompleto'
                    WHEN c.estatus = 4 THEN 'Transitorio se dispara un proceso que realiza desarrollo'
                    WHEN c.estatus = 5 THEN 'Expirado'
                    WHEN c.estatus = 6 THEN 'Pagado'
                END AS estatus,
                promo.nombre AS promocion,
                p.nombre AS tipo_plan,
                p.nombre AS tipo_smartpack,
                CASE
                    when c.tipo_item = 1 then 'Membresia'
                    when c.tipo_item = 1 AND c.prospecto IS NULL then 'Membresia Zero'
                END AS tipo_producto,
                c.precio,
                c.moneda,
                c.precio_btc,
                c.cotiza_btc,
                c.cotiza_btc2,
                c.monto_pagado,
                c.monto_pagado_btc,
                c.fue_insuficiente AS pago_insuficiente,
                c.es_excedido AS pago_excedido,
                c.avance_pago,
                c.es_complementario AS complemento,
                c.complemento_de AS id_complemento,
                c.monto_complementario
            FROM compras c
            LEFT JOIN productos p on c.tipo_item = p.producto
            LEFT JOIN promociones promo on c.promocion = promo.promocion
            WHERE c.tipo_item = 1
            ORDER BY c.fecha_insert ASC;
        """)

        compras_origen = cur_origen.fetchall()
        print(f"Registros origen {len(compras_origen)}")

        # 2. Obtener los IDs de compra que ya existen en el destino
        cur_destino.execute("SELECT id_compra FROM fact_membresias")
        compras_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino : {len(compras_existentes)}")


        # ------- TRANSFORM DATA
        # Filtrar solo las compras que no existen aún
        nuevas_compras = [
            row for row in compras_origen if row[0] not in compras_existentes
        ]

        if not nuevas_compras:
            print("ℹNo hay nuevas compras para insertar.")
            registros_insertados = 0
            return {
            "estatus": "success",
            "tabla": "fact_membresias",
            "proceso": "insertar_membresias",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
            }

        # ------- LOAD DATA
        # SQL de inserción optimizada
        print(f"Insertando registros a la BD fact_membresias")
        insert_sql = """
            INSERT INTO fact_membresias (
                id_compra,id_socio,id_prospecto,fecha_pago,forma_pago,estatus,
                promocion,tipo_plan,tipo_smartpack,tipo_producto,precio,moneda,
                precio_btc,cotiza_btc,cotiza_btc2,monto_pagado,monto_pagado_btc,
                pago_insuficiente,pago_excedido,avance_pago,complemento,id_complemento,
                monto_complementario
            ) VALUES %s
        """
        execute_values(cur_destino, insert_sql, nuevas_compras)
        conn_destino.commit()
        registros_insertados = len(nuevas_compras)
        print(f"Se han insertado {registros_insertados} nuevas compras correctamente.")

        return {
            "estatus": "success",
            "tabla": "fact_membresias",
            "proceso": "insertar_membresias",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_membresias",
            "proceso": "insertar_membresias",
            "registros_insertados": 0,
            "error_text": str(e)
        }
    
def actualizar_membresias(cur_origen, conn_origen, cur_destino, conn_destino):
    try:

        # Obtener datos desde la base origen
        cur_origen.execute("""
            SELECT
                c.compra,
                c.socio,
                c.prospecto,
                c.fecha_pago,
                CASE
                    when c.forma_pago = 1 then 'BTC'
                    when c.forma_pago = 2 and c.entidad_pago = 1            then 'FIAT - OpenPay'
                    when c.forma_pago = 2 and c.entidad_pago = 2            then 'FIAT - Banorte'
                    when c.forma_pago = 3 and c.referencia ilike 'TRX-%'    then 'FLEXPAY - TRX_CNKT_POL'
                    when c.forma_pago = 3 and c.referencia3 = 'BTC'     then 'FLEXPAY - BTC'
                    when c.forma_pago = 3 and c.referencia3 = 'CNKT_POL'    then 'FLEXPAY - CNKT_POL'
                    when c.forma_pago = 3 and c.referencia3 = 'USDT_TRON'   then 'FLEXPAY - USDT_TRON'
                    when c.forma_pago = 0 then 'Bono único'
                    when c.forma_pago = 6 THEN 'Transferencia'
                    when  c.forma_pago= 4 then 'Polygon - CNKT'
                    when  c.forma_pago= 5 then 'Polygon - BTC'
                    when  c.forma_pago= 7 then 'Polygon - USDT'
                    when  c.forma_pago= 8 then 'FIAT - STRIPE'
                    else 'x'
                END AS forma_pago,
                CASE
                    WHEN c.estatus = -2 THEN 'Estatus manual de cambio'
                    WHEN c.estatus = -1 THEN 'Cancelado en Flexbit'
                    WHEN c.estatus = 0 THEN 'Creado'
                    WHEN c.estatus = 1 THEN 'Pendiente'
                    WHEN c.estatus = 2 THEN 'En proceso'
                    WHEN c.estatus = 3 THEN 'Pago incompleto'
                    WHEN c.estatus = 4 THEN 'Transitorio se dispara un proceso que realiza desarrollo'
                    WHEN c.estatus = 5 THEN 'Expirado'
                    WHEN c.estatus = 6 THEN 'Pagado'
                END AS estatus,
                promo.nombre AS promocion,
                p.nombre AS tipo_plan,
                p.nombre AS tipo_smartpack,
                CASE
                    WHEN c.tipo_item = 1 THEN 'Membresia'
                    WHEN c.tipo_item = 1 AND c.prospecto IS NULL THEN 'Membresia Zero'
                END AS tipo_producto,
                c.precio,
                c.moneda,
                c.precio_btc,
                c.cotiza_btc,
                c.cotiza_btc2,
                c.monto_pagado,
                c.monto_pagado_btc,
                c.fue_insuficiente AS pago_insuficiente,
                c.es_excedido AS pago_excedido,
                c.avance_pago,
                c.es_complementario AS complemento,
                c.complemento_de AS id_complemento,
                c.monto_complementario
            FROM compras c
            LEFT JOIN productos p on c.tipo_item = p.producto
            LEFT JOIN promociones promo on c.promocion = promo.promocion
            WHERE c.tipo_item = 1
            ORDER BY c.fecha_insert ASC;
        """)
        compras_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(compras_origen)}")

        # Obtener los datos existentes en destino
        cur_destino.execute("""
            SELECT 
                id_compra,
                pago_insuficiente,
                pago_excedido,
                complemento,
                id_complemento,
                monto_complementario,
                estatus
            FROM fact_membresias;
        """)
        compras_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(compras_destino)}")

        # Mapeo destino para comparación
        mapa_destino = {
            row[0]: (row[1], row[2], row[3], row[4], row[5], row[6])
            for row in compras_destino
        }

        actualizaciones = []

        for row in compras_origen:
            id_compra = row[0]
            # Convertir valores booleanos de forma segura
            pago_insuficiente = bool(row[-6]) if row[-6] is not None else False
            pago_excedido = bool(row[-5]) if row[-5] is not None else False
            complemento = bool(row[-3]) if row[-3] is not None else False
            id_complemento = row[-2]
            monto_complementario = row[-1]
            estatus = row[5]

            valores_origen = (
                pago_insuficiente,
                pago_excedido,
                complemento,
                id_complemento,
                monto_complementario,
                estatus
            )

            valores_destino = mapa_destino.get(id_compra)

            if valores_destino:
                pago_insuf_dest = bool(valores_destino[0]) if valores_destino[0] is not None else False
                pago_exced_dest = bool(valores_destino[1]) if valores_destino[1] is not None else False
                complemento_dest = bool(valores_destino[2]) if valores_destino[2] is not None else False
                valores_destino_normalizados = (
                    pago_insuf_dest,
                    pago_exced_dest,
                    complemento_dest,
                    valores_destino[3],
                    valores_destino[4],
                    valores_destino[5]
                )
                if valores_origen != valores_destino_normalizados:
                    #print("Id compra: ", id_compra)
                    #print("Origen: ", valores_origen)
                    #print("Destino: ", valores_destino_normalizados)
                    actualizaciones.append((*valores_origen, id_compra))

        print(f"Se atualizaran {len(actualizaciones)} registros")
        print("Iniciando actualización de registros...") 


        # Realizar actualizaciones mediante tabla temporal
        if actualizaciones:
            # Crear tabla temporal
            cur_destino.execute("""
                CREATE TEMP TABLE tmp_actualizaciones (
                    pago_insuficiente BOOLEAN,
                    pago_excedido BOOLEAN,
                    complemento BOOLEAN,
                    id_complemento INTEGER,
                    monto_complementario MONEY,
                    estatus text,
                    id_compra BIGINT
                ) ON COMMIT DROP;
            """)

            # Insertar valores en la tabla temporal
            insert_sql = """
                INSERT INTO tmp_actualizaciones (
                    pago_insuficiente, pago_excedido, complemento,
                    id_complemento, monto_complementario, estatus, id_compra
                ) VALUES %s
            """
            execute_values(cur_destino, insert_sql, actualizaciones)

            # Actualizar tabla destino desde tabla temporal
            cur_destino.execute("""
                UPDATE fact_membresias fm
                SET
                    pago_insuficiente = t.pago_insuficiente,
                    pago_excedido = t.pago_excedido,
                    complemento = t.complemento,
                    id_complemento = t.id_complemento,
                    monto_complementario = t.monto_complementario,
                    estatus = t.estatus
                FROM tmp_actualizaciones t
                WHERE fm.id_compra = t.id_compra;
            """)
        
            conn_destino.commit()
            registros_insertados = len(actualizaciones)
            print(f"Se han actualizado {registros_insertados} registros existentes.")
        else: 
            print("No hubo anualidades por actualizar.")
            registros_insertados = 0
        
        return {
            "estatus": "success",
            "tabla": "fact_membresias",
            "proceso": "actualizar_membresias",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_membresias",
            "proceso": "actualizar_membresias",
            "registros_insertados": 0,
            "error_text": str(e)
        }
