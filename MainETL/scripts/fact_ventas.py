import psycopg2
from psycopg2.extras import execute_values


#---- este codigo corresponde a los planes
def insertar_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        #----------EXTRACT DATA
        # 1. Eliminar registros del año 2025 en destino
        cur_destino.execute("""
            DELETE FROM fact_ventas
            WHERE EXTRACT(YEAR FROM fecha_compra) = 2025
        """)
        print("Datos del año 2025 eliminados correctamente de la tabla de destino.")

        # 2. Obtener registros desde origen con nombre_producto incluido
        cur_origen.execute("""
            SELECT
                sp.compra,
                sp.socio,
                sp.producto,
                pr.nombre as nombre_del_producto,
                sp.activo,
                sp.cerrado,
                sp.capital_inicial,
                sp.moneda,
                sp.capital,
                sp.saldo,
                sp.fecha_compra,
                sp.fecha_inicio,
                sp.fecha_cierre,
                sp.estatus,
                sp.plazo_forzoso,
                sp.plan_base,
                c.precio,
                CASE
                    WHEN c.forma_pago = 1 THEN 'BTC'
                    WHEN c.forma_pago = 2 AND c.entidad_pago = 1 THEN 'FIAT - OpenPay'
                    WHEN c.forma_pago = 2 AND c.entidad_pago = 2 THEN 'FIAT - Banorte'
                    WHEN c.forma_pago = 3 AND c.referencia ILIKE 'TRX-%' THEN 'FLEXPAY - TRX_CNKT'
                    WHEN c.forma_pago = 3 AND c.referencia3 = 'BTC' THEN 'FLEXPAY - BTC'
                    WHEN c.forma_pago = 3 AND c.referencia3 = 'CNKT_POL' THEN 'FLEXPAY - CNKT_POL'
                    WHEN c.forma_pago = 3 AND c.referencia3 = 'USDT_TRON' THEN 'FLEXPAY - USDT_TRON'
                    WHEN c.forma_pago = 0 THEN 'Bono único'
                    WHEN c.forma_pago = 6 AND c.entidad_pago = 1 AND (c.referencia2 IS NULL OR c.referencia2 = '012180001156091786') THEN 'FIAT - TRX - BBVA'
                    WHEN c.forma_pago = 6 AND c.entidad_pago = 1 AND (c.referencia2 IS NOT NULL OR c.referencia2 <> '012180001156091786') THEN 'FIAT - TRX - STP'
                    WHEN c.forma_pago = 4 THEN 'Polygon - CNKT'
                    WHEN c.forma_pago = 5 THEN 'Polygon - BTC'
                    WHEN c.forma_pago = 7 THEN 'Polygon - USDT'
                    WHEN c.forma_pago = 8 THEN 'FIAT - STRIPE'
                    when c.forma_pago in (6, 9) and c.entidad_pago = 1 and (c.referencia2 is not null or c.referencia2 <> '012180001156091786') then 'FIAT - TRX - STP'
                    when c.forma_pago = 12 then 'Polygon - CNKT+'
                    ELSE 'x'
                END AS forma_de_pago
            FROM socio_productos sp
            LEFT JOIN compras c ON c.compra = sp.compra
            LEFT JOIN productos pr ON pr.producto = c.item
            WHERE sp.fecha_compra::date > '2024-12-31'
              AND sp.plan_base IS NULL
              AND sp.compra NOT IN (2030616, 2030618)
            ORDER BY sp.fecha_compra ASC;
        """)

        registros = cur_origen.fetchall()
        print('Datos origen recuperados exitosamente')

        # 3. Extraer solo los datos necesarios para insertar
        valores = [(
            r[0],  # id_compra
            r[1],  # id_socio
            r[2],  # id_producto
            r[3],  # nombre_producto
            r[4],  # activo
            r[5],  # cerrado
            r[6],  # capital_inicial
            r[7],  # moneda
            r[8],  # capital
            r[9],  # saldo
            r[10], # fecha_compra
            r[11], # fecha_inicio
            r[12], # fecha_cierre
            r[13], # estatus
            r[14], # plazo_forzoso
            r[15], # plan_base
            r[16], # precio
            r[17]  # tipo_pago
        ) for r in registros]

        registros_insertados = len(valores)
        print(f'Planes a insertar: {registros_insertados}')
        print('Ejecutando inserción...')

        # 4. Inserción optimizada en fact_ventas
        sql = """
            INSERT INTO fact_ventas (
                id_compra, id_socio, id_producto, nombre_producto, activo, cerrado, 
                capital_inicial, moneda, capital, saldo, 
                fecha_compra, fecha_inicio, fecha_cierre, 
                estatus, plazo_forzoso, plan_base, precio, tipo_pago
            )
            VALUES %s
        """
        
        execute_values(cur_destino, sql, valores)
        conn_destino.commit()
        print(f"\n\nSe han insertado {registros_insertados} planes correctamente.")

        return {
            "estatus": "success",
            "tabla": "fact_ventas",
            "proceso": "insertar_planes",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_ventas",
            "proceso": "insertar_planes",
            "registros_insertados": 0,
            "error_text": str(e)
        }
