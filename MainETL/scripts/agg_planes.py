from collections import OrderedDict
from psycopg2.extras import execute_values

def insertar_agg_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # -------- EXTRACT & TRANSFORM DATA
        print("Obteniendo y agregando datos desde fact_ventas con join a dim_cat_productos...")

        cur_destino.execute("""
            SELECT 
                CASE
                    WHEN cp.nombre ILIKE '%PLAN SELECT%' THEN 'PLAN SELECT'
                    WHEN cp.nombre ILIKE '%SELECT24%' THEN 'SELECT24'
                    WHEN cp.nombre ILIKE '%SELECT36%' THEN 'SELECT36'
                    WHEN cp.nombre ILIKE '%SELECT%' THEN 'SELECT'
                    WHEN cp.nombre ILIKE '%PA%' OR cp.nombre ILIKE '%PA24%' THEN 'PA / PA24'
                    ELSE 'OTROS'
                END AS nombre_agrupado,
                COUNT(*) AS cantidad,
                SUM(fv.precio) AS valor_total,
                DATE_TRUNC('day', fv.fecha_compra) AS fecha
            FROM fact_ventas fv
            LEFT JOIN dim_cat_productos cp ON fv.id_producto = cp.id_producto
            GROUP BY nombre_agrupado, DATE_TRUNC('day', fv.fecha_compra)
            ORDER BY fecha;
        """)
        datos_agregados = cur_destino.fetchall()
        print(f"Registros agregados encontrados: {len(datos_agregados)}")

        # -------- VERIFICAR EXISTENTES EN DESTINO
        cur_destino.execute("SELECT nombre, fecha FROM agg_planes")
        registros_existentes = set((row[0], row[1]) for row in cur_destino.fetchall())
        print(f"Registros ya existentes en destino: {len(registros_existentes)}")

        # -------- FILTRAR NUEVOS REGISTROS
        nuevos_datos = []
        for row in datos_agregados:
            clave = (row[0], row[3])  # (nombre_agrupado, fecha)
            if clave not in registros_existentes:
                nuevos_datos.append(row)

        if not nuevos_datos:
            print("ℹ No hay nuevos registros para insertar en agregacion_planes.")
            return {
                "estatus": "success",
                "tabla": "agg_planes",
                "proceso": "insertar_agregacion_planes",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # -------- LOAD NUEVOS DATOS
        print(f"Insertando {len(nuevos_datos)} nuevos registros en agregacion_planes...")

        execute_values(cur_destino, """
            INSERT INTO agg_planes (
                nombre,
                cantidad,
                valor_total,
                fecha
            ) VALUES %s
            ON CONFLICT (nombre, fecha) DO NOTHING
        """, nuevos_datos)

        conn_destino.commit()
        registros_insertados = len(nuevos_datos)
        print(f"✅ Insertados correctamente {registros_insertados} registros nuevos.")

        nuevos_datos = None
        datos_agregados = None
        return {
            "estatus": "success",
            "tabla": "agg_planes",
            "proceso": "insertar_agregacion_planes",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        nuevos_datos = None
        datos_agregados = None
        return {
            "estatus": "failed",
            "tabla": "agg_planes",
            "proceso": "insertar_agregacion_planes",
            "registros_insertados": 0,
            "error_text": str(e)
        }
