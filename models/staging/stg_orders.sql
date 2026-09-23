{{ config(materialized='table') }}

SELECT
    ORDERID,
    CUSTOMERID,
    PRODUCTID,
    ORDERDATE,
    QUANTITY,
    DISCOUNT,
    STATUS AS ORDER_STATUS

FROM {{ source('raw', 'orders') }}