import psycopg2
from psycopg2.extras import execute_values

def insertar_metas_planes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        #---------- DELETE EXISTENTES
        cur_destino.execute("DELETE FROM dim_metas_planes;")

        #---------- INSERT NUEVOS REGISTROS
        cur_destino.execute("""
            INSERT INTO dim_metas_planes (
                fecha,
                id_platinum,
                nombre_platinum,
                region,
                total_real,
                total_objetivo
            )
            WITH socios_por_mes AS (
                SELECT 
                    date_trunc('month', v.fecha_compra)::date AS fecha, 
                    pm.id_platinum, 
                    pm.nombre_platinum,
                    pm.region,
                    SUM(v.precio) AS total_real
                FROM 
                    dim_primerplatinum pm
                LEFT JOIN fact_ventas v
                    ON v.id_socio = pm.id_socio
                WHERE 
                    v.fecha_compra BETWEEN '2025-01-01' AND '2025-12-31'
                GROUP BY 
                    date_trunc('month', v.fecha_compra)::date, 
                    pm.id_platinum, 
                    pm.nombre_platinum,
                    pm.region
            )
            SELECT 
                spm.*,
                -- Valor de la meta según el mes
                CASE 
                    WHEN spm.fecha = DATE '2025-01-01' THEN os.enero
                    WHEN spm.fecha = DATE '2025-02-01' THEN os.febrero
                    WHEN spm.fecha = DATE '2025-03-01' THEN os.marzo
                    WHEN spm.fecha = DATE '2025-04-01' THEN os.abril
                    WHEN spm.fecha = DATE '2025-05-01' THEN os.mayo
                    WHEN spm.fecha = DATE '2025-06-01' THEN os.junio
                    WHEN spm.fecha = DATE '2025-07-01' THEN os.julio
                    WHEN spm.fecha = DATE '2025-08-01' THEN os.agosto
                    WHEN spm.fecha = DATE '2025-09-01' THEN os.septiembre
                    WHEN spm.fecha = DATE '2025-10-01' THEN os.octubre
                    WHEN spm.fecha = DATE '2025-11-01' THEN os.noviembre
                    WHEN spm.fecha = DATE '2025-12-01' THEN os.diciembre
                    ELSE NULL
                END AS total_objetivo
            FROM 
                socios_por_mes spm
            LEFT JOIN dim_cat_objetivos_planes os 
                ON os.id_socio_platinum = spm.id_platinum
            ORDER BY 
                spm.fecha desc;
        """)

        # Confirmar transacción
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "dim_metas_planes",
            "proceso": "insertar_metas_planes",
            "registros_insertados": cur_destino.rowcount,
            "error_text": "No error"
        }

    except Exception as e:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_metas_planes",
            "proceso": "insertar_metas_planes",
            "registros_insertados": 0,
            "error_text": str(e)
        }
