import psycopg2
from psycopg2.extras import execute_values
import json
import requests
from decimal import Decimal


def insertar_kft_compras_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ------ EXTRACT DATA
        #1. codigo de extraccción (API) para obtener todos los registros del origen
    
        # Configuración
        url = "https://products.konfront.mx/api/1.1/obj/compras_smart"
        token = "9d54cb1bdacbf694837ac0d286445f76"
 
        # Headers con autenticación Bearer
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
 
        # Parámetros (constraints para fechas)
        constraints = [
            {"key":"Created Date","constraint_type":"greater than","value":"2025-01-01"},
            {"key":"Created Date","constraint_type":"less than","value":"2025-08-27"},
            {"sort_field": "Created Date", "descending": "false"},
            {"key":"Fecha_pago","constraint_type":"is_not_empty","value":"yes"}
        ]
 
        params = {
            "constraints": json.dumps(constraints)
        }
 
        # Hacer la petición GET
        response = requests.get(url, headers=headers, params=params)
 
        # Mostrar la respuesta
        if response.status_code == 200:
            compras_origen = response.json()
            print(json.dumps(compras_origen, indent=4))
            registros_origen = compras_origen["response"]["results"]
            print(f"Registros origen: {len(registros_origen)}")
        else:
            print(f"Error {response.status_code}: {response.text}")
 
        #2. conexion a base de datos destino postgress para obtener registros destino
 
        print('Obteniendo registros de destino...')
        cur_destino.execute("""
            SELECT id_compra FROM fact_kft_compras_planes;
        """)
        ids_destino = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(ids_destino)}")
       
        # ------- TRANSFORM DATA
        # Los valores que existen en origen pero NO en destino se tomarán como valores a insertar
        # 3. Filtrar solo los registros que no están en destino
        # codigo de transformacion de datos
        print('Transformando datos para la inserción SQL...')
        registros_nuevos = [r for r in registros_origen if r["id_compra"] not in ids_destino]
       
       
        # 4. Preparar los valores para insertarlos
        valores_a_insertar = [
            (
                int(r["id_compra"][2:]),                  # "co1674745"
                r.get("address", None),                    # dirección o None
                Decimal(r["cantidad"]) if "cantidad" in r else None,
                Decimal(r["Monto_recibido"]) if "Monto_recibido" in r else None,
                r.get("status", None),                     # status o None
                r.get("Fecha_pago", None)                  # fecha o None
            )
            for r in registros_nuevos
        ]
       
        # ------- LOAD DATA
        print('Iniciando inserción SQL..')
        # 5. Ejecutar insert masivo en caso de que sí existan valores a insertar
        # codigo de carga de datos (usar funcion execute_values de psycopg2)
        insert_sql = """
            INSERT INTO fact_kft_compras_planes (
                id_compra, address, cantidad, monto_recibido, status, fecha_pago
            ) VALUES %s
            """        
 
        registros_insertados = 0
        if valores_a_insertar:
            execute_values(cur_destino, insert_sql, valores_a_insertar)
            conn_destino.commit()
            registros_insertados = len(valores_a_insertar)
            print(f'¡Se insertaron {registros_insertados} nuevas compras_planes!')
        else:
            print('No hay compras_planes nuevos para insertar.')
       
        return {
            "estatus": "success",
            "tabla": "fact_kft_compras_planes",
            "proceso": "insertar_kft_compras_planes",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
   
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_kft_compras_planes",
            "proceso": "insertar_kft_compras_planes",
            "registros_insertados": 0,
            "error_text": str(e)
        }
   
def actualizar_kft_compras_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ------ EXTRACT DATA
        #1. codigo de extraccción (API) para obtener todos los registros del origen
       
        # Configuración
        url = "https://products.konfront.mx/api/1.1/obj/compras_smart"
        token = "9d54cb1bdacbf694837ac0d286445f76"
 
        # Headers con autenticación Bearer
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
 
        # Parámetros (constraints para fechas)
        constraints = [
            {"key":"Created Date","constraint_type":"greater than","value":"2025-01-01"},
            {"key":"Created Date","constraint_type":"less than","value":"2025-08-27"},
            {"sort_field": "Created Date", "descending": "false"},
            {"key":"Fecha_pago","constraint_type":"is_not_empty","value":"yes"}
        ]
 
        params = {
            "constraints": json.dumps(constraints)
        }
 
        # Hacer la petición GET
        response = requests.get(url, headers=headers, params=params)
 
        # Mostrar la respuesta
        if response.status_code == 200:
            compras_origen = response.json()
            print(json.dumps(compras_origen, indent=4))
            registros_origen = compras_origen["response"]["results"]
            print(f"Registros origen: {len(registros_origen)}")
        else:
            print(f"Error {response.status_code}: {response.text}")
 
        #2. conexion a base de datos destino postgress para obtener registros destino
 
        print('Obteniendo registros de destino...')
        cur_destino.execute("""
            SELECT id_compra, address, cantidad, monto_recibido, status, fecha_pago, created_date, modified_date, created_by
            FROM fact_kft_compras_planes;
        """)
        registros_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(registros_destino)}")
 
        # ------- TRANSFORM DATA
        # codigo de transformacion de datos
        print('Transformando datos para la inserción SQL...')
        # 3. Crear diccionario destino con id_compra como clave
        dict_destino = {
            row[0]: (
                row[1],                      # address
                row[2],                      # cantidad
                row[3],                      # monto_recibido
                row[4],                      # status
                row[5].date() if row[5] else None,  # fecha_pago
                row[6].date() if row[6] else None,  # created_date
                row[7].date() if row[7] else None,  # modified_date
                row[8]                       # created_by
            )
            for row in registros_destino
        }
 
        # 4. Identificar registros que necesitan actualización
        registros_para_update = []
 
        for r in registros_origen:
            id_compra = r["id_compra"]
            clave_origen = (
                r["address"],        # address
                r["cantidad"],       # cantidad
                r["Monto_recibido"], # monto_recibido
                r["status"],         # status
                r["Fecha_pago"],     # fecha_pago
                r["Modified Date"]   # modified_date
            )
 
            clave_destino = dict_destino.get(id_compra)
 
            if clave_destino and clave_origen != clave_destino:
                registros_para_update.append((
                    id_compra,           # Para la condición del JOIN
                    r["address"],        # address
                    r["cantidad"],       # cantidad
                    r["Monto_recibido"], # monto_recibido
                    r["status"],         # status
                    r["Fecha_pago"],     # fecha_pago
                    r["Modified Date"]   # modified_date
                ))
 
        print(f'Se actualizarán {len(registros_para_update)} registros.')
 
        # ------- LOAD DATA
        # codigo de carga de datos (usar funcion execute_values de psycopg2)
 
        if registros_para_update:
            # 5. Crear tabla temporal
            print('Creando tabla temporal para fact_kft_compras_planes...')
            cur_destino.execute("""
                CREATE TEMP TABLE tmp_compras_planes_update (
                    id_compra TEXT,
                    address TEXT,
                    cantidad NUMERIC,
                    monto_recibido NUMERIC,
                    status TEXT,
                    fecha_pago TIMESTAMP,
                    modified_date TIMESTAMP
                ) ON COMMIT DROP;
            """)
 
        # 6. Insertar registros en la tabla temporal usando execute_values
        print('Insertando registros en tabla temporal...')
        insert_sql = """
            INSERT INTO tmp_compras_planes_update (
                id_compra, address, cantidad, monto_recibido, status,
                fecha_pago, modified_date
            ) VALUES %s
            """        
        execute_values(cur_destino, insert_sql, registros_para_update, page_size=1000)
 
        # 7. Actualizar registros en bloque usando JOIN
        print('Actualizando registros en fact_kft_compras_planes...')
        update_sql = """
            UPDATE fact_kft_compras_planes AS f
            SET estatus = t.estatus,
                address = t.address,
                cantidad = t.cantidad,
                monto_recibido = t.monto_recibido,
                status = t.status,
                fecha_pago = t.fecha_pago,
                modified_date = t.modified_date
            FROM tmp_compras_planes_update AS t
            WHERE f.id_compra = t.id_compra;
        """
        cur_destino.execute(update_sql)
 
        conn_destino.commit()
        registros_actualizados = len(registros_para_update)
        print(f'¡Se actualizaron {registros_actualizados} exitosamente!')
 
        return {
            "estatus": "success",
            "tabla": "fact_kft_compras_planes",
            "proceso": "actualizar_kft_compras_planes",
            "registros_actualizados": registros_actualizados,
            "error_text": "No error"
        }
   
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_kft_compras_planes",
            "proceso": "actualizar_kft_compras_planes",
            "registros_actualizados": 0,
            "error_text": str(e)
        }