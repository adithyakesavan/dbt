{{ config(materialized='table') }}

SELECT
    p.ORDERID,
    p.PAYMENTSTATUS

FROM {{ ref('stg_payments') }} p

LEFT JOIN {{ ref('orders_clean') }} o
    ON p.ORDERID = o.ORDERID

WHERE o.ORDERID IS NULL