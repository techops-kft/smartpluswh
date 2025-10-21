import psycopg2
from psycopg2.extras import execute_values

def insertar_fact_conciliacion_pagos(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        print("Ejecutando consulta de conciliación de pagos en cur_origen...")

        # --- Consulta principal (origen) ---
        cur_origen.execute("""
            SELECT 
                pt.pago_tx, 
                pt.socio, 
                -- Limpieza de valores numéricos por si vienen con símbolos o texto
                NULLIF(REPLACE(REPLACE(pt.monto::text, '$', ''), ',', ''), '')::numeric AS monto,
                pt.fecha_pagado::date AS fecha_pagado, 
                pt.wallet,
                NULLIF(REPLACE(REPLACE(pt.monto_pagado_btc::text, '$', ''), ',', ''), '')::numeric AS monto_pagado_btc,
                NULLIF(REPLACE(REPLACE(pt.cotiza_btc::text, '$', ''), ',', ''), '')::numeric AS cotiza_btc,
                pt.mensaje
            FROM public.pagos_transacciones pt
            WHERE pt.mensaje ILIKE '%konfront%'
              AND pt.estatus = 1;
        """)

        resultados = cur_origen.fetchall()
        print(f"Registros obtenidos desde origen: {len(resultados)}")

        # --- Validación: si no hay resultados, salir ---
        if not resultados:
            print("ℹ No hay registros para insertar en fact_conciliacion_pagos.")
            return {
                "estatus": "success",
                "tabla": "fact_conciliacion_pagos",
                "proceso": "insertar_fact_conciliacion_pagos",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # --- Inserción o actualización (UPSERT) en base destino ---
        print("Insertando o actualizando registros en fact_conciliacion_pagos (destino)...")

        insert_sql = """
            INSERT INTO fact_conciliacion_pagos (
                pago_tx, socio, monto, fecha_pagado, wallet,
                monto_pagado_btc, cotiza_btc, mensaje
            ) VALUES %s
            ON CONFLICT (pago_tx)
            DO UPDATE SET
                socio = EXCLUDED.socio,
                monto = EXCLUDED.monto,
                fecha_pagado = EXCLUDED.fecha_pagado,
                wallet = EXCLUDED.wallet,
                monto_pagado_btc = EXCLUDED.monto_pagado_btc,
                cotiza_btc = EXCLUDED.cotiza_btc,
                mensaje = EXCLUDED.mensaje,
                fecha_registro = NOW();
        """

        execute_values(cur_destino, insert_sql, resultados)
        conn_destino.commit()

        registros_insertados = len(resultados)
        print(f"✅ Se han insertado o actualizado {registros_insertados} registros en fact_conciliacion_pagos.")

        return {
            "estatus": "success",
            "tabla": "fact_conciliacion_pagos",
            "proceso": "insertar_fact_conciliacion_pagos",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        print(f"❌ Error en insertar_fact_conciliacion_pagos: {e}")
        return {
            "estatus": "failed",
            "tabla": "fact_conciliacion_pagos",
            "proceso": "insertar_fact_conciliacion_pagos",
            "registros_insertados": 0,
            "error_text": str(e)
        }
