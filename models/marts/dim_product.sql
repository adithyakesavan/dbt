{{ config(materialized='table') }}

SELECT
    PRODUCTID,
    PRODUCTNAME,
    CATEGORY,
    UNITPRICE

FROM {{ ref('stg_products') }}