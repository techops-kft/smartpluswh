import psycopg2
from psycopg2.extras import execute_values
import json
import requests
from decimal import Decimal
from dateutil.parser import parser
from dateutil.parser import parse as parse_date
    
def insertar_kft_compras_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:

        # ------ EXTRACT DATA
        #1. codigo de extraccción (API) para obtener todos los registros del origen

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
            {"key":"Created Date","constraint_type":"less than","value":"2050-09-17"},
            {"sort_field": "Created Date", "descending": "false"},
            {"key":"Fecha_pago","constraint_type":"is_not_empty","value":"yes"}
        ]

        limit = 100
        cursor = 0
        todos_registros = []

        # ------ EXTRACT DATA con cursor
        while True:
            params = {
                "constraints": json.dumps(constraints),
                "limit": limit,
                "cursor": cursor
            }

        # Hacer la petición GET
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                break

        # Mostrar la respuesta
            registros = response.json()["response"]["results"]
            print(f"Cursor {cursor}, registros obtenidos: {len(registros)}")
            todos_registros.extend(registros)

            if len(registros) < limit:
                break  # no hay más registros
            cursor += limit

        print(f"Total registros obtenidos: {len(todos_registros)}")

        #2. conexion a base de datos destino postgress para obtener registros destino
        print('Obteniendo registros de destino...')
        cur_destino.execute("SELECT id_compra FROM fact_kft_compras_planes;")
        ids_destino = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(ids_destino)}")

        # ------- TRANSFORM DATA
        # Los valores que existen en origen pero NO en destino se tomarán como valores a insertar
        # 3. Filtrar solo los registros que no están en destino
        # codigo de transformacion de datos
        print('Transformando datos para la inserción SQL...')
        registros_nuevos = [r for r in todos_registros if int(r["id_compra"][2:]) not in ids_destino]


        # 4. Preparar los valores para insertarlos
        valores_a_insertar = []
        for r in registros_nuevos:
            try:
                complemento_de = int(r["complemento_de"][2:]) if r.get("complemento_de") else None
            except:
                complemento_de = None

            try:
                fecha_pago = parser.parse(r["Fecha_pago"]) if r.get("Fecha_pago") else None
            except:
                fecha_pago = None

            valores_a_insertar.append((
                int(r["id_compra"][2:]),                  # id_compra
                r.get("address", None),                    # address
                Decimal(r["cantidad"]) if r.get("cantidad") else None, #cantidad
                Decimal(r["Monto_recibido"]) if r.get("Monto_recibido") else None, #monto_recibido
                r.get("status", None),                     # status
                r.get("Fecha_pago", None),                 # fecha_pago
                r.get("coin", None),                       # coin
                complemento_de,                            # complemento_de
                Decimal(r["USD"]) if r.get("USD") else None,  # usd
                r.get("wallet", None)                      # wallet
            ))

        # ------- LOAD DATA
        print('Iniciando inserción SQL..')
        # 5. Ejecutar insert masivo en caso de que sí existan valores a insertar
        # codigo de carga de datos (usar funcion execute_values de psycopg2)
        registros_insertados = 0
        if valores_a_insertar:
            insert_sql = """
                INSERT INTO fact_kft_compras_planes (
                    id_compra, address, cantidad, monto_recibido, status, fecha_pago, coin, complemento_de, usd, wallet
                ) VALUES %s
            """
            execute_values(cur_destino, insert_sql, valores_a_insertar)
            conn_destino.commit()
            registros_insertados = len(valores_a_insertar)
            print(f'¡Se insertaron {registros_insertados} nuevas compras_planes!')
        else:
            print("No hay compras_planes nuevos para insertar.")

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
        
def actualizar1_kft_compras_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        url = "https://products.konfront.mx/api/1.1/obj/compras_smart"
        token = "9d54cb1bdacbf694837ac0d286445f76"


        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        constraints = [
            {"key":"Created Date","constraint_type":"greater than","value":"2025-01-01"},
            {"key":"Created Date","constraint_type":"less than","value":"2025-08-27"},
            {"sort_field": "Created Date", "descending": "false"},
            {"key":"Fecha_pago","constraint_type":"is_not_empty","value":"yes"}
        ]

        limit = 100
        cursor = 0
        todos_registros = []

        # --- Iteración con cursor
        while True:
            params = {
                "constraints": json.dumps(constraints),
                "limit": limit,
                "cursor": cursor
            }

            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")

            registros_origen = response.json()["response"]["results"]
            todos_registros.extend(registros_origen)
            print(f"Cursor {cursor}, registros obtenidos: {len(registros_origen)}")

            if len(registros_origen) < limit:
                break
            cursor += limit

        print(f"Total registros origen: {len(todos_registros)}")

        # ------ DESTINO
        cur_destino.execute("""
            SELECT id_compra, address, cantidad, monto_recibido, status, fecha_pago,
                coin, complemento_de, usd, wallet
            FROM fact_kft_compras_planes;
        """)
        registros_destino = cur_destino.fetchall()

        dict_destino = {
            str(row[0]): (
                row[1],  # address
                row[2],  # cantidad
                row[3],  # monto_recibido
                row[4],  # status
                row[5],  # fecha_pago datetime
                row[6],  # coin
                str(row[7]) if row[7] is not None else None,  # complemento_de como str
                row[8],  # usd
                row[9]   # wallet
            )
            for row in registros_destino
        }

        # ------ FUNCIONES DE COMPARACIÓN
        def iguales_decimal(a, b):
            try:
                a = Decimal(a) if a is not None else None
                b = Decimal(b) if b is not None else None
            except:
                return False
            if a is None and b is None:
                return True
            if a is None or b is None:
                return False
            return round(a, 2) == round(b, 2)

        def iguales_str(a, b):
            a = "" if a is None else str(a).strip().lower()
            b = "" if b is None else str(b).strip().lower()
            return a == b

        def iguales_fecha(a, b):
            if a is None and b is None:
                return True
            if a is None or b is None:
                return False
            if isinstance(a, str):
                a = parse_date(a)
            if isinstance(b, str):
                b = parse_date(b)
            return a.replace(microsecond=0) == b.replace(microsecond=0)

        def iguales(a, b):
            if isinstance(a, (int, float, Decimal)) or isinstance(b, (int, float, Decimal)):
                return iguales_decimal(a, b)
            elif isinstance(a, (str, type(None))) or isinstance(b, (str, type(None))):
                return iguales_str(a, b)
            else:
                return iguales_fecha(a, b)

        # ------ TRANSFORM
        registros_para_update = []
        for r in todos_registros:
            id_compra = str(r["id_compra"])  # mantener como string
            clave_origen = (
                r.get("address"),
                Decimal(r["cantidad"]) if r.get("cantidad") else None,
                Decimal(r["Monto_recibido"]) if r.get("Monto_recibido") else None,
                r.get("status"),
                r.get("Fecha_pago"),
                r.get("coin"),
                str(r["complemento_de"]) if r.get("complemento_de") else None,  # mantener como str
                Decimal(r["USD"]) if r.get("USD") else None,
                r.get("wallet")
            )

            clave_destino = dict_destino.get(id_compra)
            necesita_update = False
            if clave_destino:
                for co, cd in zip(clave_origen, clave_destino):
                    if not iguales(co, cd):
                        necesita_update = True
                        break
                if necesita_update:
                    registros_para_update.append((
                        id_compra,
                        *clave_origen
                    ))

        registros_insertados = len(registros_para_update)
        if registros_insertados > 0:
            print(f"Actualizando {registros_insertados} registros...")

            cur_destino.execute("""
                CREATE TEMP TABLE tmp_compras_planes_update (
                    id_compra TEXT,
                    address TEXT,
                    cantidad NUMERIC(20,2),
                    monto_recibido NUMERIC(20,2),
                    status TEXT,
                    fecha_pago TIMESTAMP WITH TIME ZONE,
                    coin TEXT,
                    complemento_de TEXT,
                    usd NUMERIC(20,2),
                    wallet TEXT
                ) ON COMMIT DROP;
            """)

            insert_sql = """
                INSERT INTO tmp_compras_planes_update (
                    id_compra, address, cantidad, monto_recibido, status,
                    fecha_pago, coin, complemento_de, usd, wallet
                ) VALUES %s
            """
            execute_values(cur_destino, insert_sql, registros_para_update, page_size=1000)

            update_sql = """
                UPDATE fact_kft_compras_planes AS f
                SET address = t.address,
                    cantidad = t.cantidad,
                    monto_recibido = t.monto_recibido,
                    status = t.status,
                    fecha_pago = t.fecha_pago,
                    coin = t.coin,
                    complemento_de = t.complemento_de,
                    usd = t.usd,
                    wallet = t.wallet
                FROM tmp_compras_planes_update AS t
                WHERE f.id_compra = t.id_compra;
            """
            cur_destino.execute(update_sql)
            conn_destino.commit()
            print(f"¡Se actualizaron {registros_insertados} registros exitosamente!")
        else:
            print("No hay registros nuevos que actualizar.")

        return {
            "estatus": "success",
            "tabla": "fact_kft_compras_planes",
            "proceso": "actualizar_kft_compras_planes",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_kft_compras_planes",
            "proceso": "actualizar_kft_compras_planes",
            "registros_insertados": 0,
            "error_text": str(e)
        }
