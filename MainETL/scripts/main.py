import os
import psycopg2
import importlib
import traceback
from dotenv import load_dotenv
from dim_socios import insertar_socios
import time

inicio = time.time()  # Marca de tiempo inicial

load_dotenv()

# Configuración de conexión a la base de datos origen
DB_ORIGEN_CONFIG = {
    "dbname": f"{os.getenv('origen_dbname')}",
    "user": f"{os.getenv('origen_user')}",
    "password": f"{os.getenv('origen_password')}",
    "host": f"{os.getenv('origen_host')}",
    "port": f"{os.getenv('origen_port')}"
}

# Configuración de conexión a la base de datos destino
DB_DESTINO_CONFIG = {
    "dbname": f"{os.getenv('destino_dbname')}",
    "user": f"{os.getenv('destino_user')}",
    "password": f"{os.getenv('destino_password')}",
    "host": f"{os.getenv('destino_host')}",
    "port": f"{os.getenv('destino_port')}"
}

plan_ejecucion = [
    {"modulo": "dim_socios", "funcion": "insertar_socios", "dependencia": [], "ejecucion": None},
    {"modulo": "dim_socios", "funcion": "actualizar_socios", "dependencia": [], "ejecucion": None},
    {"modulo": "fact_retiros", "funcion": "insertar_retiros", "dependencia": ["dim_socios"], "ejecucion": None},
    {"modulo": "fact_retiros", "funcion": "actualizar_retiros", "dependencia": [], "ejecucion": None},
    {"modulo": "fact_ventas", "funcion": "insertar_planes", "dependencia": ["dim_socios"], "ejecucion": None}, #codigo para planes
    {"modulo": "fact_masterbonus", "funcion": "insertar_masterbonus", "dependencia": ["dim_socios"], "ejecucion": None},
    {"modulo": "fact_masterbonus", "funcion": "actualizar_masterbonus", "dependencia": [], "ejecucion": None},
    {"modulo": "dim_prospectos", "funcion": "insertar_prospectos", "dependencia": ["dim_socios"], "ejecucion": None},
    {"modulo": "dim_prospectos", "funcion": "actualizar_prospectos", "dependencia": [], "ejecucion": None},
    {"modulo": "dim_primerplatinum", "funcion": "insertar_primerplatinum", "dependencia": ["dim_socios"], "ejecucion": None},
    {"modulo": "dim_primerplatinum", "funcion": "actualizar_primerplatinum", "dependencia": [], "ejecucion": None},
    {"modulo": "dim_metas_socios", "funcion": "insertar_metas_socios", "dependencia": ["dim_primerplatinum"], "ejecucion": None},
    {"modulo": "dim_metas_planes", "funcion": "insertar_metas_planes", "dependencia": ["dim_primerplatinum"], "ejecucion": None},
    {"modulo": "fact_anualidades", "funcion": "insertar_anualidades", "dependencia": ["dim_socios"], "ejecucion": None},
    #{"modulo": "fact_anualidades", "funcion": "actualizar_anualidades", "dependencia": [], "ejecucion": None},
    {"modulo": "fact_membresias", "funcion": "insertar_membresias", "dependencia": ["dim_socios"], "ejecucion": None},
    #{"modulo": "fact_membresias", "funcion": "actualizar_membresias", "dependencia": [], "ejecucion": None},
    {"modulo": "hist_membresias", "funcion": "insertar_hist_membresias", "dependencia": [], "ejecucion": None},
    {"modulo": "hist_rangos", "funcion": "insertar_hist_rangos", "dependencia": [], "ejecucion": None}
]

try:
    conn_origen = psycopg2.connect(**DB_ORIGEN_CONFIG)
    cur_origen = conn_origen.cursor()
    print("Conexión origen exitosa")

    conn_destino = psycopg2.connect(**DB_DESTINO_CONFIG)
    cur_destino = conn_destino.cursor()
    conn_destino.autocommit = False
    print("Conexión destino exitosa\n")

    for step in plan_ejecucion:
        modulo = step["modulo"]
        funcion = step["funcion"]
        dependencias = step["dependencia"]

        print(f'Ejecutando: {modulo} -> {funcion}')

        # Verificar si todas las dependencias se ejecutaron con éxito
        dependencias_fallidas = [
            dep for dep in dependencias
            if next((s for s in plan_ejecucion if s["modulo"] == dep), {}).get("ejecucion") != "success"
        ]

        if dependencias_fallidas:
            resultado = {
                "estatus": "failed",
                "tabla": modulo,
                "proceso": funcion,
                "registros_insertados": 0,
                "error_text": f"No se ejecutó por dependencia fallida: {', '.join(dependencias_fallidas)}"
            }
            step["ejecucion"] = "failed"

        else:
            try:
                mod = importlib.import_module(modulo)
                func = getattr(mod, funcion)

                resultado = func(cur_origen, conn_origen, cur_destino, conn_destino)
                step["ejecucion"] = resultado["estatus"]

            except Exception as e:
                resultado = {
                    "estatus": "failed",
                    "tabla": modulo,
                    "proceso": funcion,
                    "registros_insertados": 0,
                    "error_text": traceback.format_exc()
                }
                step["ejecucion"] = "failed"
                conn_destino.rollback()

        # Insertar log usando directamente el resultado
        sql_log = """
            INSERT INTO migration_logs (
                estatus,
                tabla,
                proceso,
                registros_insertados,
                error_text
            ) VALUES (%s, %s, %s, %s, %s)
        """
        cur_destino.execute(sql_log, (
            resultado["estatus"],
            resultado["tabla"],
            resultado["proceso"],
            resultado["registros_insertados"],
            resultado["error_text"]
        ))
        conn_destino.commit()

        print(f"{'Success' if resultado['estatus'] == 'success' else 'Failed'} {modulo}.{funcion}: {resultado['error_text']}\n\n")

except Exception as e:
    print(f"Error general: {e}")

finally:
    cur_origen.close()
    conn_origen.close()
    cur_destino.close()
    conn_destino.close()
    print("\n Conexiones cerradas.")
    fin = time.time()  # Marca de tiempo final
    print(f"Tiempo de ejecución: {fin - inicio:.4f} segundos")