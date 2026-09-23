{{ config(materialized='table') }}

SELECT
    CUSTOMERID,
    AGE,
    CITY,
    SIGNUPDATE,
    CUSTOMER_SEGMENT

FROM {{ ref('stg_customers') }}