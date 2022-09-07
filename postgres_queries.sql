
CREATE TABLE points_gaps (devicekey varchar, geom geometry, pointnum varchar, coord_timestamp varchar, speed float, odometer float, chargeRange float, tripid int);

CREATE TABLE fulltrips (deviceid varchar, tripstart timestamp, tripend timestamp, tripid varchar);

TRUNCATE fulltrips

SELECT *, EXTRACT(EPOCH FROM (tripend - tripstart))/60 AS triplength FROM fulltrips

ALTER TABLE points_gaps
  ALTER COLUMN geom TYPE geometry(POINT, 4326)
    USING ST_SetSRID(geom,4326);

DROP TABLE fulltrips

 ALTER TABLE trips_odometer RENAME TO points_odometer;

SELECT devicekey, pointnum, ST_MakeLine(geom) geom FROM trips GROUP BY devicekey, pointnum


ALTER TABLE trips DROP COLUMN endtime;
select * from fulltrips limit 40;

SELECT tripend::TIME - tripstart::TIME from fulltrips

SELECT devicekey, ST_MakeLine(trps.geom ORDER BY pointnum::int), CONCAT(pointnum, " ") FROM trips as trps GROUP BY devicekey

CREATE OR REPLACE
FUNCTION public.trips_speed(
    z integer, x integer, y integer,
            device_key text default '')
RETURNS bytea
AS $$
DECLARE
    result bytea;
BEGIN
    WITH
    bounds AS (
      SELECT ST_TileEnvelope(z, x, y) AS geom
    ),
    lines AS (
      SELECT trps.devicekey, trps.tripid, ST_MakeLine(trps.geom ORDER BY pointnum::int) geom FROM points_speed AS trps GROUP BY devicekey, tripid
    ),
    mvtgeom AS (
        SELECT ST_AsMVTGeom(ST_Transform(t.geom, 3857), bounds.geom) AS geom, t.devicekey
        FROM lines t, bounds
        WHERE ST_Intersects(t.geom, ST_Transform(bounds.geom, 4326))
            AND CASE WHEN device_key = '' THEN TRUE 
            ELSE upper(t.devicekey) = upper(device_key) END
    )
    SELECT ST_AsMVT(mvtgeom, 'default')
    INTO result
    FROM mvtgeom;

    RETURN result;
END;
$$
LANGUAGE 'plpgsql'
STABLE
PARALLEL SAFE;

DROP TABLE tester;
CREATE TABLE tester AS (
  SELECT devicekey, pointnum, ST_MakeLine(geom) geom FROM trips WHERE devicekey = '2CzWI9DKiCU9KTxUAlo0wTHQjy3' GROUP BY devicekey, pointnum ORDER BY geom DESC
)
ALTER TABLE tester
  ALTER COLUMN geom TYPE geometry(POINT, 4326)
    USING ST_SetSRID(geom,4326);

DROP TABLE tester;
CREATE TABLE tester AS (
  SELECT devicekey, geom, pointnum, coord_timestamp, ST_LineMerge(
        ST_Collect(geom)
    ) lineattempt FROM trips WHERE devicekey = '2CzWI9DKiCU9KTxUAlo0wTHQjy3' GROUP BY devicekey, geom, pointnum, coord_timestamp ORDER BY pointnum::int ASC
)
ALTER TABLE tester
  ALTER COLUMN geom TYPE geometry(POINT, 4326)
    USING ST_SetSRID(geom,4326);


DROP TABLE linetest;
CREATE TABLE linetest AS (
    SELECT gps.devicekey, ST_MakeLine(gps.geom ORDER BY pointnum::int) As geom
      FROM tester As gps
      GROUP BY devicekey
);
ALTER TABLE linetest
  ALTER COLUMN geom TYPE geometry(LINESTRING, 4326)
    USING ST_SetSRID(geom,4326);


SELECT ST_Centroid(ST_UNION(geom)), devicekey FROM public.trips 
GROUP BY devicekey;