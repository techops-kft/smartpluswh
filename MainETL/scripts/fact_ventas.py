import psycopg2
from psycopg2.extras import execute_values


#---- este codigo corresponde a los planes
def insertar_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        #----------EXTRACT DATA
        # 1. Obtener todos los registros del origen y borrar registros de 2025
        cur_destino.execute("""
            DELETE FROM fact_ventas
            WHERE EXTRACT(YEAR FROM fecha_compra) = 2025
        """)
        print("Datos del año 2025 eliminados correctamente de la tabla de destino.")

        cur_origen.execute("""
                SELECT
                    sp.compra,
                    sp.socio,
                    sp.producto,
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
                    case
                        when c.forma_pago = 1 then 'BTC'
                        when c.forma_pago = 2
                            and c.entidad_pago = 1 then 'FIAT - OpenPay'
                            when c.forma_pago = 2
                            and c.entidad_pago = 2 then 'FIAT - Banorte'
                            when c.forma_pago = 3
                            and c.referencia ilike 'TRX-%' then 'FLEXPAY - TRX_CNKT'
                            when c.forma_pago = 3
                            and c.referencia3 = 'BTC' then 'FLEXPAY - BTC'
                            when c.forma_pago = 3
                            and c.referencia3 = 'CNKT_POL' then 'FLEXPAY - CNKT_POL'
                            when c.forma_pago = 3
                            and c.referencia3 = 'USDT_TRON' then 'FLEXPAY - USDT_TRON'
                            when c.forma_pago = 0 then 'Bono único'
                            when c.forma_pago = 6
                            and c.entidad_pago = 1
                            and (c.referencia2 is null
                                or c.referencia2 = '012180001156091786') then 'FIAT - TRX - BBVA'
                            when c.forma_pago = 6
                            and c.entidad_pago = 1
                            and (c.referencia2 is not null
                                or c.referencia2 <> '012180001156091786') then 'FIAT - TRX - STP'
                            when c.forma_pago = 4 then 'Polygon - CNKT'
                            when c.forma_pago = 5 then 'Polygon - BTC'
                            when c.forma_pago = 7 then 'Polygon - USDT'
                            when c.forma_pago = 8 then 'FIAT - STRIPE'
                            else 'x'
                    end as forma_de_pago
                FROM socio_productos sp
                LEFT JOIN compras c
                    ON c.compra = sp.compra
                WHERE sp.fecha_compra::date > '2024-12-31'
                ORDER BY sp.fecha_compra ASC;
        """)

        registros = cur_origen.fetchall()
        print('Datos origen recuperados exitosamente')

        # Extrae solo los datos necesarios para insertar
        valores = [(
                    r[0],  # id_compra
                    r[1],  # id_socio
                    r[2],  # id_producto
                    r[3],  # activo
                    r[4],  # cerrado
                    r[5],  # capital_inicial
                    r[6],  # moneda
                    r[7],  # capital
                    r[8],  # saldo
                    r[9],  # fecha_compra
                    r[10], # fecha_inicio
                    r[11], # fecha_cierre
                    r[12], # estatus
                    r[13], # plazo_forzoso
                    r[14], # plan_base
                    r[15],  # precio
                    r[16] # tipo_pago
                ) for r in registros]


        registros_insertados = len(valores)
        print(f'Planes a insertar: {registros_insertados}')
        print('Ejecutando...')

        # Inserta todos los valores de una sola vez
        sql = """
            INSERT INTO fact_ventas (
                    id_compra, id_socio, id_producto, activo, cerrado, 
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
            "proceso": "insertar_ventas",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_ventas",
            "proceso": "insertar_ventas",
            "registros_insertados": 0,
            "error_text": str(e)
        }
