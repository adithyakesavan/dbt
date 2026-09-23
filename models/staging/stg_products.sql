{{ config(materialized='table') }}

SELECT DISTINCT
    PRODUCTID,
    TRIM(PRODUCTNAME) AS PRODUCTNAME,
    TRIM(CATEGORY) AS CATEGORY,
    UNITPRICE

FROM {{ source('raw', 'products') }}