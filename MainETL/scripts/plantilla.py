import psycopg2
from psycopg2.extras import execute_values

def insertar(cur_origen, conn_origen, cur_destino, conn_destino):
    try:

        # aqui va la logica del codigo

        return {
            "estatus": "success",
            "tabla": "********",
            "proceso": "**********",
            "registros_insertados": 1000,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "*********",
            "proceso": "**********",
            "registros_insertados": 0,
            "error_text": str(e)
        }
    
def actualizar(cur_origen, conn_origen, cur_destino, conn_destino):
    try:

        # aqui va la logica del codigo

        return {
            "estatus": "success",
            "tabla": "********",
            "proceso": "**********",
            "registros_insertados": 1000,
            "error_text": "No error"
        }
    
    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "*********",
            "proceso": "**********",
            "registros_insertados": 0,
            "error_text": str(e)
        }