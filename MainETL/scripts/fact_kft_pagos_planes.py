import psycopg2
from psycopg2.extras import execute_values
import json
import requests
from dateutil import parser
from decimal import Decimal

def insertar_kft_pagos_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ------ EXTRACT DATA
        url = "https://products.konfront.mx/api/1.1/obj/pagos_smart"
        token = "9d54cb1bdacbf694837ac0d286445f76"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        constraints = [
            {"key": "Created Date", "constraint_type": "greater than", "value": "2025-01-01"},
            {"key": "Created Date", "constraint_type": "less than", "value": "2050-09-17"},
            {"sort_field": "Created Date", "descending": "false"},
            {"key": "id_pago", "constraint_type": "is_not_empty", "value": "yes"}
        ]

        limit = 100
        cursor = 0
        todos_registros = []

        while True:
            params = {
                "constraints": json.dumps(constraints),
                "cursor": cursor,
                "limit": limit
            }

            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                break

            registros = response.json()["response"]["results"]
            print(f"Cursor {cursor}, registros obtenidos: {len(registros)}")
            todos_registros.extend(registros)

            if len(registros) < limit:
                break

            cursor += limit

        print(f"Total registros obtenidos: {len(todos_registros)}")

        # ------ DESTINO
        cur_destino.execute("SELECT pago_tx FROM fact_kft_pagos_planes;")
        ids_destino = set(row[0] for row in cur_destino.fetchall())

        # ------- TRANSFORM DATA
        registros_nuevos = [r for r in todos_registros if int(r["id_pago"][2:]) not in ids_destino]

        valores_a_insertar = [
            (
                int(r["id_pago"][2:]) if "id_pago" in r and r["id_pago"] else None,  # pago_tx
                Decimal(r["amount"]) if "amount" in r and r["amount"] else None,
                r.get("Asset", None),                      # asset
                r.get("coin", None),                       # coin
                Decimal(r["cotizacion"]) if "cotizacion" in r and r["cotizacion"] else None,
                r.get("status", None),                     # estatus
                r.get("id_tx", None),                      # id_tx
                r.get("id_wallet", None),                  # id_wallet
                parser.parse(r["Created Date"]) if "Created Date" in r and r["Created Date"] else None,  # created_at
                r.get("address", None),                    # nueva columna address
                Decimal(r["amount_cripto"]) if "amount_cripto" in r and r["amount_cripto"] else None,    # nueva columna amount_crypto
                r.get("moneda", None),                     # nueva columna moneda
                int(r["socio"]) if "socio" in r and r["socio"] else None  # nueva columna socio
            )
            for r in registros_nuevos
        ]

        # ------- LOAD DATA
        print("Iniciando inserción SQL...")
        if valores_a_insertar:
            insert_sql = """
                INSERT INTO fact_kft_pagos_planes (
                    pago_tx, amount, asset, coin, cotizacion, estatus, id_tx, id_wallet, created_at,
                    address, amount_crypto, moneda, socio
                ) VALUES %s
            """
            execute_values(cur_destino, insert_sql, valores_a_insertar)
            conn_destino.commit()
            print(f"¡Se insertaron {len(valores_a_insertar)} registros nuevos!")
        else:
            print("No hay registros nuevos para insertar.")

        return {
            "estatus": "success",
            "tabla": "fact_kft_pagos_planes",
            "proceso": "insertar_kft_pagos_planes",
            "registros_insertados": len(valores_a_insertar),
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_kft_pagos_planes",
            "proceso": "insertar_kft_pagos_planes",
            "registros_insertados": 0,
            "error_text": str(e)
        }
 


def actualizar_kft_pagos_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        url = "https://products.konfront.mx/api/1.1/obj/pagos_smart"
        token = "9d54cb1bdacbf694837ac0d286445f76"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        constraints = [
            {"key":"Created Date","constraint_type":"greater than","value":"2025-01-01"},
            {"key":"Created Date","constraint_type":"less than","value":"2025-09-15"},
            {"sort_field": "Created Date", "descending": "false"}
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
            SELECT id_pago, amount, asset, coin, cotizacion, estatus, id_tx, id_wallet, usd
            FROM fact_kft_pagos_planes;
        """)
        registros_destino = cur_destino.fetchall()

        dict_destino = {
            str(row[0]): (
                row[1],  # amount
                row[2],  # asset
                row[3],  # coin
                row[4],  # cotizacion
                row[5],  # estatus
                row[6],  # id_tx
                row[7],  # id_wallet
                row[8]   # usd
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

        # ------ TRANSFORM
        registros_para_update = []
        for r in todos_registros:
            id_pago = int(r["id_pago"].upper().replace("PA", ""))

            clave_origen = (
                Decimal(r.get("amount")) if r.get("amount") else None,
                r.get("Asset"),
                r.get("coin"),
                Decimal(r.get("cotizacion")) if r.get("cotizacion") else None,
                r.get("estatus"),
                r.get("id_tx"),
                r.get("id_wallet"),
                Decimal(r.get("USD")) if r.get("USD") else None
            )

            clave_destino = dict_destino.get(str(id_pago))

            necesita_update = False
            for co, cd in zip(clave_origen, clave_destino):
                if isinstance(co, Decimal) or isinstance(cd, Decimal):
                    if not iguales_decimal(co, cd):
                        necesita_update = True
                        break
                else:
                    if not iguales_str(co, cd):
                        necesita_update = True
                        break

                if necesita_update:
                    registros_para_update.append((
                        id_pago,
                        clave_origen[0],
                        clave_origen[1],
                        clave_origen[2],
                        clave_origen[3],
                        clave_origen[4],
                        clave_origen[5],
                        clave_origen[6],
                        clave_origen[7]
                    ))

        registros_actualizados = len(registros_para_update)
        if registros_actualizados > 0:
            print(f"Actualizando {registros_actualizados} registros...")
            cur_destino.execute("""
                CREATE TEMP TABLE tmp_pagos_planes_update (
                    id_pago INTEGER,
                    amount NUMERIC(20,2),
                    asset TEXT,
                    coin TEXT,
                    cotizacion NUMERIC(20,2),
                    estatus TEXT,
                    id_tx TEXT,
                    id_wallet TEXT,
                    usd NUMERIC(20,2)
                ) ON COMMIT DROP;
            """)

            insert_sql = """
                INSERT INTO tmp_pagos_planes_update (
                    id_pago, amount, asset, coin, cotizacion, estatus, id_tx, id_wallet, usd
                ) VALUES %s
            """
            execute_values(cur_destino, insert_sql, registros_para_update, page_size=1000)

            update_sql = """
                UPDATE fact_kft_pagos_planes AS f
                SET amount = t.amount,
                    asset = t.asset,
                    coin = t.coin,
                    cotizacion = t.cotizacion,
                    estatus = t.estatus,
                    id_tx = t.id_tx,
                    id_wallet = t.id_wallet,
                    usd = t.usd
                FROM tmp_pagos_planes_update AS t
                WHERE f.id_pago = t.id_pago;
            """
            cur_destino.execute(update_sql)
            conn_destino.commit()
            print(f"¡Se actualizaron {registros_actualizados} registros exitosamente!")
        else:
            print("No hay registros nuevos que actualizar.")

        return {
            "estatus": "success",
            "tabla": "fact_kft_pagos_planes",
            "proceso": "actualizar_kft_pagos_planes",
            "registros_insertados": registros_actualizados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_kft_pagos_planes",
            "proceso": "actualizar_kft_pagos_planes",
            "registros_insertados": 0,
            "error_text": str(e)
        }
