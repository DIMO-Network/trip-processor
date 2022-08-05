
CREATE TABLE trips (name varchar, geom geometry pointnum varchar);

ALTER TABLE trips
  ALTER COLUMN geom TYPE geometry(POINT, 4326)
    USING ST_SetSRID(geom,4326);

SELECT devicekey, pointnum, ST_MakeLine(geom) geom FROM trips GROUP BY devicekey, pointnum

ALTER TABLE trips DROP COLUMN endtime;
select * from trips limit 4;


CREATE OR REPLACE
FUNCTION public.device_trips(
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
      SELECT devicekey, pointnum, ST_MakeLine(geom) geom FROM trips GROUP BY devicekey, pointnum
    ),
    mvtgeom AS (
        SELECT ST_AsMVTGeom(ST_Transform(t.geom, 3857), bounds.geom) AS geom, t.devicekey, t.pointnum
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