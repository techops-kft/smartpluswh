from psycopg2.extras import execute_values

def insertar_agg_bonos_comisiones(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # -------- EXTRACT & TRANSFORM DATA
        print("Obteniendo y agregando datos desde pagos con join a enum...")

        cur_origen.execute("""
            SELECT
                e.nombre,
                COUNT(*) AS cantidad,
                SUM(p.monto) AS valor_total,
                DATE_TRUNC('day', p.fecha_pagado) AS fecha
            FROM pagos p
            LEFT JOIN "enum" e ON e.codigo = p.tipo
            WHERE
                e.categoria = 'TipoPago'
                AND p.estatus = 1
            GROUP BY e.nombre, DATE_TRUNC('day', p.fecha_pagado)
            ORDER BY fecha;
        """)
        datos_agregados = cur_origen.fetchall()
        print(f"Registros agregados encontrados: {len(datos_agregados)}")

        # -------- VERIFICAR EXISTENTES EN DESTINO
        cur_destino.execute("SELECT nombre, fecha FROM agg_bonos")
        registros_existentes = set((row[0], row[1]) for row in cur_destino.fetchall())
        print(f"Registros ya existentes en destino: {len(registros_existentes)}")

        # -------- FILTRAR NUEVOS REGISTROS
        nuevos_datos = [row for row in datos_agregados if (row[0], row[3]) not in registros_existentes]

        if not nuevos_datos:
            print("ℹ No hay nuevos registros para insertar en agg_bonos.")
            return {
                "estatus": "success",
                "tabla": "agg_bonos",
                "proceso": "insertar_agregacion_bonos_comisiones",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # -------- LOAD NUEVOS DATOS
        print(f"Insertando {len(nuevos_datos)} nuevos registros en agg_bonos...")

        execute_values(cur_destino, """
            INSERT INTO agg_bonos (
                nombre,
                cantidad,
                valor_total,
                fecha
            ) VALUES %s
        """, nuevos_datos)

        conn_destino.commit()
        registros_insertados = len(nuevos_datos)
        print(f"✅ Insertados correctamente {registros_insertados} registros nuevos.")

        return {
            "estatus": "success",
            "tabla": "agg_bonos",
            "proceso": "insertar_agregacion_bonos_comisiones",
            "registros_insertados": registros_insertados,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        print(f"❌ Error: {str(e)}")
        return {
            "estatus": "failed",
            "tabla": "agg_bonos",
            "proceso": "insertar_agregacion_bonos_comisiones",
            "registros_insertados": 0,
            "error_text": str(e)
        }
