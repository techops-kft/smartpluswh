import psycopg2
from psycopg2.extras import execute_values

def insertar_fact_conciliacion_ventas(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        print("Ejecutando consulta de conciliación de ventas en cur_origen...")

        # --- Consulta principal en base origen ---
        cur_origen.execute("""
            SELECT
                c.compra, 
                c.socio, 
                c.prospecto, 
                c.tipo_item, 
                c.item, 
                c.item2, 
                CASE 
                    WHEN c.forma_pago = 4 THEN 'Polygon - CNKT'
                    WHEN c.forma_pago = 5 THEN 'Polygon - BTC'
                    WHEN c.forma_pago = 7 THEN 'Polygon - USDT'
                END AS forma_pago,
                c.promocion, 
                c.wallet_destino,
                -- Limpieza de valores numéricos
                REPLACE(REPLACE(c.precio::text, '$', ''), ',', '')::numeric AS precio,
                c.moneda, 
                REPLACE(REPLACE(c.precio_btc::text, '$', ''), ',', '')::numeric AS precio_btc,
                REPLACE(REPLACE(c.cotiza_btc::text, '$', ''), ',', '')::numeric AS cotiza_btc,
                REPLACE(REPLACE(c.monto_pagado::text, '$', ''), ',', '')::numeric AS monto_pagado,
                REPLACE(REPLACE(c.monto_pagado_btc::text, '$', ''), ',', '')::numeric AS monto_pagado_btc,
                c.fue_insuficiente, 
                c.es_complementario,
                c.complemento_de, 
                c.fecha_pago, 
                c.fecha_insert, 
                c.referencia2, 
                c.referencia3, 
                c.referencia,
                CASE
                    WHEN c.tipo_item = 5 THEN 'Mantenimiento'
                    WHEN c.tipo_item = 1 THEN 'Membresia'
                    ELSE 'Planes'
                END AS nombre_del_producto
            FROM compras c
            WHERE c.estatus = 6 
            AND c.forma_pago IN (4,5,7);
        """)

        resultados = cur_origen.fetchall()
        print(f"Registros obtenidos desde origen: {len(resultados)}")

        # --- Validación: si no hay resultados, salir ---
        if not resultados:
            print("ℹ No hay registros para insertar en fact_conciliacion_ventas.")
            return {
                "estatus": "success",
                "tabla": "fact_conciliacion_ventas",
                "proceso": "insertar_fact_conciliacion_ventas",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # --- Inserción o actualización (UPSERT) en base destino ---
        print("Insertando o actualizando registros en fact_conciliacion_ventas (destino)...")

        insert_sql = """
            INSERT INTO fact_conciliacion_ventas (
                compra, socio, prospecto, tipo_item, item, item2, forma_pago, promocion, wallet_destino,
                precio, moneda, precio_btc, cotiza_btc, monto_pagado, monto_pagado_btc,
                fue_insuficiente, es_complementario, complemento_de, fecha_pago, fecha_insert,
                referencia2, referencia3, referencia, nombre_del_producto
            ) VALUES %s
            ON CONFLICT (compra)
            DO UPDATE SET
                socio = EXCLUDED.socio,
                prospecto = EXCLUDED.prospecto,
                tipo_item = EXCLUDED.tipo_item,
                item = EXCLUDED.item,
                item2 = EXCLUDED.item2,
                forma_pago = EXCLUDED.forma_pago,
                promocion = EXCLUDED.promocion,
                wallet_destino = EXCLUDED.wallet_destino,
                precio = EXCLUDED.precio,
                moneda = EXCLUDED.moneda,
                precio_btc = EXCLUDED.precio_btc,
                cotiza_btc = EXCLUDED.cotiza_btc,
                monto_pagado = EXCLUDED.monto_pagado,
                monto_pagado_btc = EXCLUDED.monto_pagado_btc,
                fue_insuficiente = EXCLUDED.fue_insuficiente,
                es_complementario = EXCLUDED.es_complementario,
                complemento_de = EXCLUDED.complemento_de,
                fecha_pago = EXCLUDED.fecha_pago,
                fecha_insert = EXCLUDED.fecha_insert,
                referencia2 = EXCLUDED.referencia2,
                referencia3 = EXCLUDED.referencia3,
                referencia = EXCLUDED.referencia,
                nombre_del_producto = EXCLUDED.nombre_del_producto,
                fecha_registro = NOW();
        """

        execute_values(cur_destino, insert_sql, resultados)
        conn_destino.commit()

        registros_insertados = len(resultados)
        print(f"✅ Se han insertado o actualizado {registros_insertados} registros en fact_conciliacion_ventas.")

        return {
            "estatus": "success",
            "tabla": "fact_conciliacion_ventas",
            "proceso": "insertar_fact_conciliacion_ventas",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        print(f"❌ Error en insertar_fact_conciliacion_ventas: {e}")
        return {
            "estatus": "failed",
            "tabla": "fact_conciliacion_ventas",
            "proceso": "insertar_fact_conciliacion_ventas",
            "registros_insertados": 0,
            "error_text": str(e)
        }
