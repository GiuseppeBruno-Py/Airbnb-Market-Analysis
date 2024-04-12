CREATE TABLE airbnb_project.Dim_Property_Details AS
SELECT DISTINCT
  unified_id,
  zipcode,
  city,
  host_type,
  bedrooms,
  bathrooms,
  CAST(REGEXP_REPLACE(guests, r'[^0-9]', '') AS INT64) AS guests 
FROM
  airbnb_project.market_analysis;