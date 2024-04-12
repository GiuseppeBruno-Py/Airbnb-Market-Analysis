CREATE TABLE airbnb_project.Dim_Amenities AS
SELECT DISTINCT
  unified_id,
  hot_tub,
  pool
FROM
  airbnb_project.amenities;