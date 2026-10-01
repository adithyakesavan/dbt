{{ config(materialized='table') }}

SELECT
    ORDERID,
    PAYMENTSTATUS

FROM {{ source('raw', 'payments') }}