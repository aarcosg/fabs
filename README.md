{{ config(
    materialized='incremental',
    unique_key='id_cliente',
    incremental_strategy='insert_overwrite'
) }}

WITH ventas_cte AS (
    SELECT 
        v.id_cliente,
        v.id_producto,
        v.monto,
        p.categoria,
        DATE(v.fecha_venta) AS fecha_venta
    FROM {{ source('mi_esquema', 'ventas') }} v
    JOIN {{ source('mi_esquema', 'productos') }} p 
        ON v.id_producto = p.id_producto
    {% if is_incremental() %}
    WHERE DATE(v.fecha_venta) > (SELECT MAX(fecha_venta) FROM {{ this }})
    {% endif %}
), 

ventas_agrupadas AS (
    SELECT 
        id_cliente,
        categoria,
        SUM(monto) AS total_gastado
    FROM ventas_cte
    GROUP BY id_cliente, categoria
),

devoluciones_cte AS (
    SELECT 
        v.id_cliente,
        COUNT(d.id_venta) AS total_devoluciones
    FROM {{ source('mi_esquema', 'devoluciones') }} d
    JOIN {{ source('mi_esquema', 'ventas') }} v 
        ON d.id_venta = v.id_venta
    {% if is_incremental() %}
    WHERE DATE(d.fecha_devolucion) > (SELECT MAX(fecha_venta) FROM {{ this }})
    {% endif %}
    GROUP BY v.id_cliente
),

clientes_ranking AS (
    SELECT 
        c.id_cliente,
        c.nombre_cliente,
        c.pais,
        COALESCE(va.total_gastado, 0) AS total_gastado,
        COALESCE(d.total_devoluciones, 0) AS total_devoluciones,
        RANK() OVER (PARTITION BY c.pais ORDER BY va.total_gastado DESC) AS ranking_pais
    FROM {{ source('mi_esquema', 'clientes') }} c
    LEFT JOIN ventas_agrupadas va ON c.id_cliente = va.id_cliente
    LEFT JOIN devoluciones_cte d ON c.id_cliente = d.id_cliente
)

SELECT * FROM clientes_ranking;
