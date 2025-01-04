-- Query 1: Find typical duration and number of trips for the top 10 most common one-way rentals
-- This query calculates the median trip duration and counts the number of trips grouped by start and end station.
-- It retrieves the top 10 most popular trips where the start and end stations are different.
SELECT
  MIN(start_station_name) AS start_station_name,
  MIN(end_station_name) AS end_station_name,
  APPROX_QUANTILES(tripduration, 10)[OFFSET (5)] AS typical_duration,
  COUNT(tripduration) AS num_trips
FROM
  `bigquery-public-data.new_york_citibike.citibike_trips`
WHERE
  start_station_id != end_station_id
GROUP BY
  start_station_id,
  end_station_id
ORDER BY
  num_trips DESC
LIMIT
  10;

-- Query 2: Calculate the total distance traveled by each bicycle
-- This query computes the total distance traveled (in kilometers) by each bike,
-- using geolocation data from the `citibike_stations` table for start and end stations.
WITH
  trip_distance AS (
    SELECT
      bikeid,
      ST_Distance(ST_GeogPoint(s.longitude, s.latitude),
                  ST_GeogPoint(e.longitude, e.latitude)) AS distance
    FROM
      `bigquery-public-data.new_york_citibike.citibike_trips`,
      `bigquery-public-data.new_york_citibike.citibike_stations` AS s,
      `bigquery-public-data.new_york_citibike.citibike_stations` AS e
    WHERE
      start_station_name = s.name
      AND end_station_name = e.name
  )
SELECT
  bikeid,
  SUM(distance) / 1000 AS total_distance
FROM
  trip_distance
GROUP BY
  bikeid
ORDER BY
  total_distance DESC
LIMIT
  5;

-- Query 3: Retrieve rainfall data for New York Central Park Tower in 2015
-- This query retrieves daily rainfall (in mm) for a specific weather station in New York for the year 2015.
SELECT
  wx.date,
  wx.value / 10.0 AS prcp
FROM
  `bigquery-public-data.ghcn_d.ghcnd_2015` AS wx
WHERE
  id = 'USW00094728'
  AND qflag IS NULL
  AND element = 'PRCP'
ORDER BY
  wx.date;

-- Query 4: Correlate bicycle rentals with rainfall
-- This query determines if there are fewer bicycle rentals on rainy days
-- by joining bicycle rental data with weather data.
WITH
  bicycle_rentals AS (
    SELECT
      COUNT(starttime) AS num_trips,
      EXTRACT(DATE FROM starttime) AS trip_date
    FROM
      `bigquery-public-data.new_york_citibike.citibike_trips`
    GROUP BY
      trip_date
  ),
  rainy_days AS (
    SELECT
      date,
      (MAX(prcp) > 5) AS rainy
    FROM (
      SELECT
        wx.date AS date,
        IF (wx.element = 'PRCP', wx.value / 10, NULL) AS prcp
      FROM
        `bigquery-public-data.ghcn_d.ghcnd_2015` AS wx
      WHERE
        wx.id = 'USW00094728'
    )
    GROUP BY
      date
  )
SELECT
  ROUND(AVG(bk.num_trips)) AS num_trips,
  wx.rainy
FROM
  bicycle_rentals AS bk
JOIN
  rainy_days AS wx
ON
  wx.date = bk.trip_date
GROUP BY
  wx.rainy;
