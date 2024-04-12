CREATE TABLE airbnb_project.Dim_Geolocation AS
SELECT DISTINCT
  unified_id,
  latitude,
  longitude,
  street
FROM
  airbnb_project.geolocation_ac;