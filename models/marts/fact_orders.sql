{{ config(materialized='table') }}

SELECT
    o.ORDERID,
    o.CUSTOMERID,
    o.PRODUCTID,
    o.ORDERDATE,
    o.QUANTITY,
    o.DISCOUNT,
    p.UNITPRICE,

    o.QUANTITY
        * p.UNITPRICE
        * (1 - o.DISCOUNT) AS REVENUE,

    o.ORDER_STATUS,
    pay.PAYMENTSTATUS

FROM {{ ref('orders_clean') }} o

LEFT JOIN {{ ref('stg_products') }} p
    ON o.PRODUCTID = p.PRODUCTID

LEFT JOIN {{ ref('payments_clean') }} pay
    ON o.ORDERID = pay.ORDERID