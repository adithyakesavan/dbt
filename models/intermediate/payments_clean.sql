{{ config(materialized='table') }}

SELECT
    p.ORDERID,
    p.PAYMENTSTATUS

FROM {{ ref('stg_payments') }} p

INNER JOIN {{ ref('orders_clean') }} o
    ON p.ORDERID = o.ORDERID