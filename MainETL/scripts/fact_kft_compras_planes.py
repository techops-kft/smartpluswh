import psycopg2
from psycopg2.extras import execute_values

def insertar_kft_compras_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ------ EXTRACT DATA
            # codigo de extraccción (API)
            # codigo de extracción (Pyscopg2)

        # ------- TRANSFORM DATA
            # codigo de transformacion de datos


        # ------- LOAD DATA
            # codigo de carga de datos (usar funcion execute_values de psycopg2)
            

        registros_insertados = 0
        return {
            "estatus": "success",
            "tabla": "fact_membresias", # <---- cambiar
            "proceso": "insertar_membresias", # <---- cambiar
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_membresias", # <---- cambiar
            "proceso": "insertar_membresias", # <---- cambiar
            "registros_insertados": 0,
            "error_text": str(e)
        }
    
def actualizar_kft_compras_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ------ EXTRACT DATA
            # codigo de extraccción (API)

        # ------- TRANSFORM DATA
            # codigo de transformacion de datos

        # ------- LOAD DATA
            # codigo de carga de datos (usar funcion execute_values de psycopg2)

        registros_insertados = 0
        return {
            "estatus": "success",
            "tabla": "fact_membresias", # <---- cambiar
            "proceso": "actualizar_membresias", # <---- cambiar
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_membresias", # <---- cambiar
            "proceso": "actualizar_membresias", # <---- cambiar
            "registros_insertados": 0,
            "error_text": str(e)
        }
