import psycopg2
from psycopg2.extras import execute_values
import requests
from decimal import Decimal
import time
import json


def insertar_wallets(cur_origen, conn_origen, cur_destino, conn_destino):
    try:

        # --------------------------
        # EXTRACT DATA con reintentos
        # --------------------------
        url = "https://71yr5msw9i.execute-api.us-east-1.amazonaws.com/Live/get_saldos"
        max_retries = 10  # número máximo de reintentos antes de fallar
        delay = 30         # segundos iniciales entre reintentos
        intento = 0

        data_api_list = None

        while intento < max_retries:
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data_api = response.json()

                if isinstance(data_api.get('body'), str):
                    data_api_list = json.loads(data_api['body'])
                else:
                    data_api_list = data_api['body']

                # Si llegamos aquí, la petición fue exitosa
                break

            except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
                intento += 1
                print(f"⚠️ Error en petición (intento {intento}/{max_retries}): {e}")
                if intento < max_retries:
                    time.sleep(delay)
                    delay *= 2  # backoff exponencial
                else:
                    return {
                        "estatus": "failed",
                        "tabla": "dim_wallets",
                        "proceso": "insertar_wallets",
                        "registros_insertados": 0,
                        "error_text": f"Error persistente tras {max_retries} intentos: {e}"
                    }

        # --------------------------
        # TRANSFORM DATA
        # --------------------------
        compras = [
            "67ed6c3a0affc0f35eb5df8710d54143",
            "687c062c9aeb8b8038cee3cc4a99ca06",
            "687c06da4dcf6d1b48bc1b0524331936",
            "687c0742002909010bd39d26f0c8f980",
            "67ec8d2bec44928b0a38fca5378bdba4",
            "67ec8e498b76a8dcaaeb1c897a0f016d",
            "67ec8eb50ad24b16bcda7c9ca18ab1a8",
            "67ec8f168bb450e10521c8abac1f881c",
            "687aa8de7fbb498f1ea9da316e8c2ac1",
            "687c04002a1b16e1d6bd5fc4453a2c3d",
            "687c046f42054b9470f7904e83e28449",
            "687c04b45ee520dc2df9ec360dcfb3d3"
        ]

        pagos = [
            "687c07df52c4ad57874c7bb0a2b3c239",
            "67ec8f9aa8d508a8a26766e79cde68fc",
            "687c0533d0dc8d6d4413fbf4aeab8b53"
        ]

        datos_a_insertar = []
        for item in data_api_list:
            wallet_id = item.get("Id")
            coin = item.get("Coin")
            saldo = Decimal(str(item.get("Saldo", 0)))  # convertir a Decimal

            if wallet_id in compras:
                tipo = "compra"
            elif wallet_id in pagos:
                tipo = "pago"
            else:
                tipo = "otro"

            datos_a_insertar.append((wallet_id, coin, saldo, tipo))

        # --------------------------
        # LOAD DATA
        # --------------------------
        sql = """
            INSERT INTO fact_wallets (wallet_id, coin, saldo, tipo)
            VALUES %s;
        """
        execute_values(cur_destino, sql, datos_a_insertar)
        conn_destino.commit()
        count_datos = len(datos_a_insertar)
        print(f"{count_datos} registros insertados/actualizados en wallets")

        return {
            "estatus": "success",
            "tabla": "dim_wallets",
            "proceso": "insertar_wallets",
            "registros_insertados": count_datos,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_wallets",
            "proceso": "insertar_wallets",
            "registros_insertados": 0,
            "error_text": str(e)
        }
