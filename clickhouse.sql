-- DROP TABLE IF EXISTS flat_temperatures;

CREATE TABLE flat_temperatures (
      year Int16
    , date String
    , epoch Int64
    , year_actual Int16
    , day_of_year Int16
    , month_actual Int8
    , quarter_actual Int8
    , station_id String
    , latitude Float32
    , longitude Float32
    , elevation Float32
    , name String
    , country String
    , tmin Int16
    , tmax Int16
    , tavg Int16
)   ENGINE = MergeTree()
    ORDER BY date
    PARTITION BY year;

-- DROP DATABASE IF EXISTS postgresql;

CREATE DATABASE postgresql
ENGINE = PostgreSQL('localhost:5432', 'postgres', 'postgres', 'postgres', 'dwh', 1);

INSERT INTO flat_temperatures
SELECT * FROM postgresql.view_temperatures;