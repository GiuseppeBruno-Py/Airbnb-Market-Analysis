CREATE TABLE airbnb_project.Dim_Time AS
SELECT DISTINCT
  PARSE_DATE('%Y-%m', month) AS date_id,
  EXTRACT(YEAR FROM PARSE_DATE('%Y-%m', month)) AS year,
  EXTRACT(QUARTER FROM PARSE_DATE('%Y-%m', month)) AS quarter,
  EXTRACT(MONTH FROM PARSE_DATE('%Y-%m', month)) AS month
FROM
airbnb_project.market_analysis;