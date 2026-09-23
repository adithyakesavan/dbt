{{ config(materialized='table') }}

SELECT
    o.*
FROM {{ ref('stg_orders') }} o

INNER JOIN {{ ref('stg_customers') }} c
    ON o.CUSTOMERID = c.CUSTOMERID

WHERE o.QUANTITY > 0
  AND o.ORDERDATE IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY o.ORDERID
    ORDER BY o.ORDERDATE DESC
) = 1