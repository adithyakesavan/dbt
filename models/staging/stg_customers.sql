{{ config(materialized='table') }}

SELECT DISTINCT
    CUSTOMERID,
    AGE,
    TRIM(CITY) AS CITY,
    SIGNUPDATE,
    TRIM(CUSTOMERSEGMENT) AS CUSTOMER_SEGMENT

FROM {{ source('raw', 'customers') }}