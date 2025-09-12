import psycopg2
from psycopg2.extras import execute_values
import requests

def insertar_transacciones_internas(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------------------------
        # 1️⃣ Obtener registros existentes en la DB
        # ---------------------------
        cur_destino.execute("SELECT kft_id FROM fact_transacciones_internas")
        registros_existentes = {row[0] for row in cur_destino.fetchall()}  # set de kft_id
        #print(f"Registros existentes en DB: {len(registros_existentes)}")

        # ---------------------------
        # 2️⃣ Obtener datos del API
        # ---------------------------
        url = "https://products.konfront.mx/api/1.1/obj/SmartTransaccionesInternas"
        token = "9d54cb1bdacbf694837ac0d286445f76"  # reemplaza con tu token real
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            raise Exception(f"Error {response.status_code}: {response.text}")
        data = response.json()

        # ---------------------------
        # 3️⃣ Filtrar solo registros nuevos
        # ---------------------------
        registros_nuevos = []
        for item in data.get("response", {}).get("results", []):
            kft_id = item.get("_id")
            if kft_id not in registros_existentes:
                registros_nuevos.append((
                    item.get("walletId"),
                    item.get("type"),
                    item.get("coin"),
                    item.get("value"),
                    kft_id
                ))

        # ---------------------------
        # 4️⃣ Insertar registros nuevos
        # ---------------------------
        if registros_nuevos:
            insert_query = """
                INSERT INTO fact_transacciones_internas
                (wallet_id, tipo, moneda, monto, kft_id)
                VALUES %s
            """
            execute_values(cur_destino, insert_query, registros_nuevos)
            conn_destino.commit()

        registros_nuevos = None
        registros_existentes = None
        data = None

        return {
            "estatus": "success",
            "tabla": "fact_transacciones_internas",
            "proceso": "insertar_transacciones_internas",
            "registros_insertados": len(registros_nuevos),
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_transacciones_internas",
            "proceso": "insertar_transacciones_internas",
            "registros_insertados": 0,
            "error_text": str(e)
        }
