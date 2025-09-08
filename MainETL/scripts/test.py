import time
import pandas as pd
import psycopg2
import openpyxl
from datetime import datetime
 
#querySocios
querySocios = """select socio,fecha_ingreso::date from socios where socio not in (1) and estatus in(1,2) order by fecha_ingreso"""
#Query Mantenimientos
queryMantConsol = """  
                select
                    c1.socio,
                    TRIM(s.nombre) || ' ' || TRIM(s.apellido_pat) || ' ' || TRIM(s.apellido_mat) AS Nombre_Socio,
                    CASE
                        WHEN c1.estatus in (6) THEN 'COMPLETADA'
                        ELSE 'No identificado'
                    END AS estatus_compra,
                    CASE
                        WHEN c1.tipo_item = 5 AND c1.forma_pago = 2 THEN 'MANTENIMIENTO'
                        ELSE 'No identificado'
                    END AS tipo_de_producto,
                    CASE
                        WHEN c1.forma_pago = 2 THEN 'OPENPAY'
                        ELSE 'No identificado'
                    END AS plataforma_cobro,
                    c1.precio,
                    c1.moneda,
                    c1.referencia as id_transacción,
                    (COALESCE(c1.fecha_insert::date)) AS fecha_de_pago,
                    c1.observaciones
                FROM
                    compras c1
                LEFT JOIN
                    socios s ON (s.socio = c1.socio)
                WHERE
                    c1.estatus IN (6)
                    and c1.tipo_item = 5
                    and c1.forma_pago = 2
                    AND c1.es_complementario IS null
                order by fecha_de_pago desc"""
#Query Academy
queryCursos = """select
                    c1.socio,
                    TRIM(s.nombre) || ' ' || TRIM(s.apellido_pat) || ' ' || TRIM(s.apellido_mat) AS Nombre_Socio,
                    c2.nombre as Nombre_Curso,
                    CASE
                        WHEN c1.estatus in (4,6) THEN 'COMPLETADA'
                        WHEN c1.estatus = -1 THEN 'DEVOLUCIÓN'
                        ELSE 'No identificado'
                    END AS estatus_compra,
                    CASE
                        WHEN c1.forma_pago = 2 THEN 'OPENPAY'
                        ELSE 'No identificado'
                    END AS plataforma_cobro,
                    c1.precio,
                    c1.moneda,
                    c1.referencia as id_transacción,
                    c2.ponentes,
                    COALESCE(c1.fecha_pago AT TIME ZONE 'UTC', c1.fecha_pagado AT TIME ZONE 'UTC') AS fecha_de_pago,
                    c1.observaciones
                FROM
                    learning.compras c1
                LEFT JOIN
                    learning.curso c2 ON (c2.id_curso = c1.item)
                LEFT JOIN
                    socios s ON (s.socio = c1.socio)
                WHERE
                    c1.estatus IN (4, 6) and precio != '0' """
# Membresias
queryMem = """select
                    s.socio,
                    TRIM(s.nombre) || ' ' || TRIM(s.apellido_pat) || ' ' || TRIM(s.apellido_mat) AS Nombre,
                    CASE
                        WHEN c.estatus = 6 THEN 'COMPLETADA'
                        WHEN c.estatus = -1 THEN 'DEVOLUCIÓN'
                        ELSE 'No identificado'
                    END AS estatus_compra,
                    CASE
                        WHEN c.tipo_item = 1 AND c.precio = '$795.00' AND c.forma_pago = 2 THEN 'MEMBRESIA BASIC'
                        WHEN c.tipo_item = 1 AND c.precio = '$995.00' AND c.forma_pago = 2 THEN 'MEMBRESIA PREMIUM'
                        ELSE 'No identificado'
                    END AS tipo_de_producto,
                    CASE
                        WHEN c.tipo_item = 1 AND c.forma_pago = 2 THEN 'OPENPAY'
                        ELSE 'No identificado'
                    END AS plataforma_cobro,
                    c.moneda,
                    c.monto_pagado,
                    c.hash as id_transacción,
                    c.fecha_insert::DATE,
                    c.fecha_pago AT TIME ZONE 'UTC' as fecha_de_pago,
                    c.observaciones
                FROM
                    public.compras c
                LEFT JOIN
                    productos p ON (p.producto = c.item2)
                LEFT JOIN
                    prospectos p2 ON (p2.prospecto = c.prospecto)
                LEFT JOIN
                    socios s ON (s.socio = p2.nuevo_socio)
                where
                    c.tipo_item = 1
                    and c.forma_pago = 2
                    AND c.estatus in (6)
                    AND c.es_complementario IS NULL
                    AND c.socio IS NULL;"""
#Query Mantenimiento
queryMant = """select
                    socio,
                    referencia as id_transacción,
                    case
                        when forma_pago = 1 THEN 'BTC'
                        when forma_pago = 2 THEN 'OPENPAY'
                        else 'X'
                    end,
                    forma_pago,
                    monto_pagado,
                    CASE
                        WHEN monto_pagado_btc = 0 THEN '0'
                        ELSE TO_CHAR(monto_pagado_btc, '999,999.999999999')
                    END AS BTC,
                    monto_pagado_btc,
                    fecha_insert::date as fecha_pago
                from compras c WHERE c.tipo_item = 5 AND c.estatus = 6 AND c.es_complementario IS null"""
#Query Menbresias_B
queryMant2 = """                
                select
                    s.socio,
                    c.referencia as id_transacción,
                    case
                        when c.forma_pago = 1 THEN 'BTC'
                        when c.forma_pago = 2 THEN 'OPENPAY'
                        else 'X'
                    end,
                    c.forma_pago,
                    c.monto_pagado,
                    CASE
                        WHEN c.monto_pagado_btc = 0 THEN '0'
                        ELSE TO_CHAR(c.monto_pagado_btc, '999,999.999999999')
                    END AS BTC,
                    c.monto_pagado_btc,
                    c.fecha_insert::date as fecha_pago
                from compras c
                    join prospectos p on c.prospecto = p.prospecto
                    join socios s on s.socio = p.nuevo_socio
                WHERE c.tipo_item = 1 AND c.estatus = 6 AND c.es_complementario IS null
            """
 
 
# Bonos y Comisiones
queryByC = """select count(*) as numero,e.nombre,p.tipo,sum(monto),fecha_pagado::date from pagos p
                    left join "enum" e on e.codigo = p.tipo
                    where e.categoria = 'TipoPago' and p.estatus = 1 and p.tipo in (0,1,2,3,5,6,7,8,11,12,13,14,15,18,19,20,22)
                    group by e.nombre,p.tipo,fecha_pagado::date"""
queryByCMASTER = """select
                        count(*) as numero,
                        case
                            when p.monto = '$500.00' and p.tipo = 4 then 'MASTER BONUS JUNIOR'
                            when p.monto = '$1,500.00' and p.tipo = 4 then 'MASTER BONUS SENIOR'
                            when p.monto = '$5,000.00' and p.tipo = 4 then 'MASTER BONUS TRAINER'
                            when p.monto = '$10,000.00' and p.tipo = 4 then 'MASTER BONUS MANAGER'
                            when p.monto = '$20,000.00' and p.tipo = 4 then 'MASTER BONUS ELITE'
                            when p.monto = '$45,000.00' and p.tipo = 4 then 'MASTER BONUS SUPERIOR'
                            when p.monto = '$100,000.00' and p.tipo = 4 then 'MASTER BONUS PLATINUM'
                            else 'MASTER BONUS NO IDENTIFICADO'
                        end,
                        p.monto,e.nombre,p.tipo,sum(monto),fecha_pagado::date from pagos p
                    left join "enum" e on e.codigo = p.tipo
                    where e.categoria = 'TipoPago' and p.estatus = 1 and p.tipo = 4 and p.monto != '0'
                    group by p.monto,e.nombre,p.tipo,fecha_pagado::date"""
#Query Entradas y Salidas
queryEntradas = """select
                        sum(monto) as USD,
                        to_char((sum(monto_pagado_btc)),'999,999.999,999,999') as BTC,
                        fecha_pagado::date from pagos_transacciones pt
                    where estatus in (1,0)
                    group by fecha_pagado::date """
querySalidas = """select
                        sum(monto) as USD,
                        CASE
                            WHEN SUM(monto_pagado_btc) = 0 THEN '0'
                            ELSE TO_CHAR(SUM(monto_pagado_btc), '999,999.999999999')
                        END AS BTC,
                        fecha_pagado::date from pagos_transacciones pt
                    where estatus in (1,0)
                    group by fecha_pagado::date """
# AUM
queryAUMpas = """select
                    max(fecha_compra::date),
                    sum(saldo) as Saldo_Total_Productos,
                    sum(capital) as Capital_Inicial_Total_Productos
                from socio_productos sp
                where
                    cerrado is false
                    and producto not in (280,281,282)
                    and socio in (select socio from socios where estatus in (1,2))"""
 
queryAUMBonos = """select max(fecha_pago::date), sum(monto) from pagos where estatus = 0 and tipo not in (18,19,20)"""
 
queryAUMSelect = """select
                            CASE
                                WHEN nombre = 'Select 3x' THEN (fecha_compra::date + INTERVAL '1 year')::date
                                WHEN nombre = 'Select 4x' THEN (fecha_compra::date + INTERVAL '1 year')::date
                                WHEN nombre = 'Select 5x' THEN (fecha_compra::date + INTERVAL '1 year')::date
                                WHEN nombre = 'Select 8x' THEN (fecha_compra::date + INTERVAL '2 year')::date
                                WHEN nombre = 'Select 9x' THEN (fecha_compra::date + INTERVAL '2 year')::date
                                WHEN nombre = 'Select 10x' THEN (fecha_compra::date + INTERVAL '2 year')::date
                            END AS fecha_compra,
                            nombre,
                            valor_contrato,
                            cantidad_de_select,
                            CAST(REPLACE(REPLACE(valor_contrato, '$', ''), ',', '') AS NUMERIC) * cantidad_de_select AS valor_total
                        FROM (
                                SELECT
                                max(fecha_compra)as fecha_compra,
                                    CASE
                                        WHEN producto = 280 THEN 'Select 3x'
                                        WHEN producto = 281 THEN 'Select 4x'
                                        WHEN producto = 282 THEN 'Select 5x'
                                        WHEN producto = 283 THEN 'Select 8x'
                                        WHEN producto = 284 THEN 'Select 9x'
                                        WHEN producto = 285 THEN 'Select 10x'
                                    END AS nombre,
                                    CASE
                                        WHEN producto = 280 THEN '$60,000.00'
                                        WHEN producto = 281 THEN '$80,000.00'
                                        WHEN producto = 282 THEN '$100,000.00'
                                        WHEN producto = 283 THEN '$160,000.00'
                                        WHEN producto = 284 THEN '$180,000.00'
                                        WHEN producto = 285 THEN '$200,000.00'
                                    END AS valor_contrato,
                                    COUNT(*) AS cantidad_de_select
                                FROM socio_productos sp2  
                                WHERE
                                    cerrado IS FALSE
                                    AND producto IN (280, 281, 282,283,284,285)
                                    AND socio IN (SELECT socio FROM socios WHERE estatus IN (1, 2)) --select 3x,4x,5x
                                GROUP BY nombre,valor_contrato
                        ) AS resultados"""
 
queryAUMSelectActual = """select
                                case
                                    when tipo = 18 then 'Bono Select 3x'
                                    when tipo = 19 then 'Bono Select 4x'
                                    when tipo = 20 then 'Bono Select 5x'
                                    when tipo = 28 then 'Bono Select 8x'
                                    when tipo = 29 then 'Bono Select 9x'
                                    when tipo = 30 then 'Bono Select 10x'
                                end as Nombre_Bono,
                                tipo,sum(monto)
                            from pagos
                            where tipo in (18,19,20,28,29,30)
                            and estatus = 1
                            and socio in (select socio from socio_productos where producto in (280,281,282,283,284,285))group by Nombre_Bono,tipo """
 
 
 
#Conexión DB --------------------------------
host = 'prod-pos-sql-ome.cnr1vz2jxtct.us-east-1.rds.amazonaws.com'
database = 'smartov2'
user = 'smart_usr_reports'
password = 'J_XroX$uJC!z9ryv_rd]Wf3Qr^nsAj'
port = '5432'
#Conexión DB --------------------------------
 
 
def socios(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\socios.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def cursos(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\CursosReporteConsolidado.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def membresias(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\MembresiasReporteConsolidado.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def mantenimientoConsol(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\MantenimientoReporteConsolidadoOpenpay.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def mantenimiento(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\MantenimientoReporteConsolidado.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
#Almacenado del archivo
def mantenimiento2(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\Membresias2ReporteConsolidado.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def bonosYcomisiones(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\BonosReporteConsolidado.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def bonosYcomisionesMaster(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\BonosMasterReporteConsolidado.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def entradas(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\EntradasReporteConsolidado.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def salidas(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\SalidasReporteConsolidado.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def aumPas(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\AUMReporteConsolidado.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def aumBonos(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\AUMBonosReporteConsolidado.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def aumBonosSelect(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        print("EXTRACCIONDDEDATA")
        data = pd.read_sql(query, conn)
        conn.close()
        print("EXCEL")
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\AUMBonosSelect.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def aumBonosSelectActual(query, nombre_hoja):
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        data = pd.read_sql(query, conn)
        conn.close()
       
        ruta_excel = r'C:\Users\QA\Documents\Reportes_excel_Auto\AUMBonosSelectActual.xlsx'
        data.to_excel(ruta_excel,sheet_name=nombre_hoja, index=False, engine='openpyxl')
        hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Guardado " + hora_actual)
    except Exception as e:
        print("Error:", e)
 
def actualizar_datos_finanzas():
    while True:
        ahora = datetime.now().time()
        if ahora >= datetime.strptime("00:00", "%H:%M").time() and ahora <= datetime.strptime("23:59", "%H:%M").time():
           
            print("inicio socios")
            socios(querySocios, "Socios")
            print("fin socios")
 
            print("inicio queryCursos")
            cursos(queryCursos, "CursosReporte")
            print("fin queryCursos")
 
            print("inicio queryMem")
            membresias(queryMem, "MembresiasReporte")
            print("fin queryMem")
 
            print("inicio queryMantConsol")
            mantenimientoConsol(queryMantConsol, "MantenimientoConsol")
            print("fin queryMantConsol")
 
            print("inicio queryMant")
            mantenimiento(queryMant, "Mantenimiento")
            print("fin queryMant")
            # Generacion adicional de membresías
            print("inicio queryMant")
            mantenimiento2(queryMant2, "Membresías2")
            print("fin queryMant")
 
            print("inicio queryByC")
            bonosYcomisiones(queryByC, "Bonos")
            print("fin queryByC")
 
            print("inicio queryByCMaster")
            bonosYcomisionesMaster(queryByCMASTER, "BonosMaster")
            print("fin queryByCMaster")
 
            print("inicio queryEntardas")
            entradas(queryEntradas, "Entradas")
            print("fin queryEntradas")
 
            print("inicio querySsalidas")
            salidas(querySalidas, "Salidas")
            print("fin querySalidas")
           
            print("inicio queryAUMpas")
            aumPas(queryAUMpas, "AUMpas")
            print("fin queryAUMpas")
 
            print("inicio queryAUMBonos")
            aumBonos(queryAUMBonos, "AUMbonos")
            print("fin queryAUMBonos")
 
            print("inicio queryAUMSelect")
            aumBonosSelect(queryAUMSelect, "AUMbonosSelect")
            print("fin queryAUMSelect")
 
            print("inicio queryAUMSelectActual")
            aumBonosSelectActual(queryAUMSelectActual, "AUMbonosSelectActual")
            print("fin queryAUMSelectActual")
        time.sleep(10800)
 
if __name__ == "__main__":
    actualizar_datos_finanzas()