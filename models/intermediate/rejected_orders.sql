{{ config(materialized='table') }}

SELECT *
FROM {{ ref('stg_orders') }}
WHERE QUANTITY IS NULL
   OR QUANTITY <= 0
   OR ORDERDATE IS NULL