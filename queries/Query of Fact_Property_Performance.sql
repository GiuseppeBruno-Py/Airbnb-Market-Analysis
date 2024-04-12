CREATE TABLE airbnb_project.Fact_Property_Performance AS
SELECT
  ma.unified_id,
  PARSE_DATE('%Y-%m', month) AS date_id,
  ma.revenue,
  ma.openness,
  ma.occupancy,
  ma.nightly_rate,
  ma.lead_time,
  ma.length_stay
FROM
  airbnb_project.market_analysis ma
