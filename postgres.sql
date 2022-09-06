-- DROP SCHEMA IF EXISTS stage CASCADE;

CREATE SCHEMA stage;

-- DROP SCHEMA IF EXISTS dwh CASCADE;

CREATE SCHEMA dwh;

-- DROP SERVER IF EXISTS cstore_server;

-- DROP EXTENSION IF EXISTS cstore_fdw;

CREATE EXTENSION cstore_fdw;

CREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;

-- Stage tables

-- DROP TABLE IF EXISTS stage.stations;

CREATE TABLE stage.stations (
      id char(11)
    , latitude float
    , longitude float
    , elevation float
    , state char(2)
    , "name" varchar(255)
    , gsn_flag char(3)
    , hcn_crn_flag char(3)
    , wmo_id char(5)
);

COPY stage.stations FROM '/mnt/source/ghcnd-stations.csv' DELIMITER ',' CSV;

-- DROP TABLE IF EXISTS stage.inventory;

CREATE TABLE stage.inventory (
      id char(11)
    , latitude float
    , longitude float
    , "element" char(4)
    , firstyear int
    , lastyear int
);

COPY stage.inventory FROM '/mnt/source/ghcnd-inventory.txt.csv' DELIMITER ',' CSV;

-- DROP TABLE IF EXISTS stage.states;

CREATE TABLE stage.states (
      code char(2)
    , "name" varchar(50)
);

COPY stage.states FROM '/mnt/source/ghcnd-states.txt.csv' DELIMITER ',' CSV;

-- DROP TABLE IF EXISTS stage.countries;

CREATE TABLE stage.countries (
      code char(2)
    , "name" varchar(50)
);

COPY stage.countries FROM '/mnt/source/ghcnd-countries.txt.csv' DELIMITER ',' CSV;

--- Dimension tables

-- DROP TABLE IF EXISTS dwh.dim_calendar CASCADE;

CREATE TABLE dwh.dim_calendar (
      id int PRIMARY KEY
    , "date" date NOT NULL
    , epoch bigint NOT NULL
    , day_suffix varchar(4) NOT NULL
    , day_name varchar(15) NOT NULL
    , day_of_week int NOT NULL
    , day_of_month int NOT NULL
    , day_of_quarter int NOT NULL
    , day_of_year int NOT NULL
    , week_of_month int NOT NULL
    , week_of_year int NOT NULL
    , month_actual int NOT NULL
    , month_name varchar(9) NOT NULL
    , month_name_short char(3) NOT NULL
    , quarter_actual int NOT NULL
    , quarter_name varchar(9) NOT NULL
    , year_actual int NOT NULL
    , first_day_of_week date NOT NULL
    , last_day_of_week date NOT NULL
    , first_day_of_month date NOT NULL
    , last_day_of_month date NOT NULL
    , first_day_of_quarter date NOT NULL
    , last_day_of_quarter date NOT NULL
    , first_day_of_year date NOT NULL
    , last_day_of_year date NOT NULL
    , mmyyyy char(6) NOT NULL
    , mmddyyyy char(10) NOT NULL
    , weekend bool NOT NULL
);

INSERT INTO dwh.dim_calendar
SELECT 
      TO_CHAR(ts, 'yyyymmdd')::INT AS id
    , ts AS date_actual
    , EXTRACT(EPOCH FROM ts) AS epoch
    , TO_CHAR(ts, 'fmDDth') AS day_suffix
    , TO_CHAR(ts, 'TMDay') AS day_name
    , EXTRACT(ISODOW FROM ts) AS day_of_week
    , EXTRACT(DAY FROM ts) AS day_of_month
    , ts - DATE_TRUNC('quarter', ts)::DATE + 1 AS day_of_quarter
    , EXTRACT(DOY FROM ts) AS day_of_year
    , TO_CHAR(ts, 'W')::INT AS week_of_month
    , EXTRACT(WEEK FROM ts) AS week_of_year
    , EXTRACT(MONTH FROM ts) AS month_actual
    , TO_CHAR(ts, 'TMMonth') AS month_name
    , TO_CHAR(ts, 'Mon') AS month_name_short
    , EXTRACT(QUARTER FROM ts) AS quarter_actual
    , CASE
           WHEN EXTRACT(QUARTER FROM ts) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM ts) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM ts) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM ts) = 4 THEN 'Fourth'
           END AS quarter_name
    , EXTRACT(YEAR FROM ts) AS year_actual
    , ts + (1 - EXTRACT(ISODOW FROM ts))::INT AS first_day_of_week
    , ts + (7 - EXTRACT(ISODOW FROM ts))::INT AS last_day_of_week
    , ts + (1 - EXTRACT(DAY FROM ts))::INT AS first_day_of_month
    , (DATE_TRUNC('MONTH', ts) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month
    , DATE_TRUNC('quarter', ts)::DATE AS first_day_of_quarter
    , (DATE_TRUNC('quarter', ts) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter
    , TO_DATE(EXTRACT(YEAR FROM ts) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year
    , TO_DATE(EXTRACT(YEAR FROM ts) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year
    , TO_CHAR(ts, 'mmyyyy') AS mmyyyy
    , TO_CHAR(ts, 'mmddyyyy') AS mmddyyyy
    , CASE
           WHEN EXTRACT(ISODOW FROM ts) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend
FROM (SELECT '1750-01-01'::DATE + SEQUENCE.DAY AS ts
      FROM GENERATE_SERIES(0, 100000) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;


-- DROP TABLE IF EXISTS dwh.dim_countries;

-- DROP SEQUENCE IF EXISTS dim_countries_null_sequence;

CREATE SEQUENCE dim_countries_null_sequence
    MINVALUE 0
    start 0
    increment 1;

CREATE TABLE dwh.dim_countries (
      id int UNIQUE NOT NULL DEFAULT nextval('dim_countries_null_sequence')
    , code char(2)
    , "name" varchar(50)
    , start_ts date
    , end_ts date
    , is_current bool
    , "version" int
);

INSERT INTO dwh.dim_countries (
      code
    , "name"
    , start_ts
    , end_ts
    , is_current
    , "version"
) VALUES (
      NULL
    , NULL
    , NULL
    , NULL
    , NULL
    , 1
);

INSERT INTO dwh.dim_countries (
      code
    , "name"
    , start_ts
    , end_ts
    , is_current
    , "version"
)
SELECT
      code
    , "name"
    , '1700-01-01' AS start_ts
    , '2199-12-31' AS end_ts
    , TRUE AS is_current
    , 1 AS VERSION
FROM stage.countries;

-- DROP TABLE IF EXISTS dwh.dim_stations;

-- DROP SEQUENCE IF EXISTS dim_stations_null_sequence;

CREATE SEQUENCE dim_stations_null_sequence
    MINVALUE 0
    start 0
    increment 1;

CREATE TABLE dwh.dim_stations (
      id int UNIQUE NOT NULL DEFAULT nextval('dim_stations_null_sequence')
    , station_id char(11)
    , country_id int NOT NULL REFERENCES dwh.dim_countries(id)
    , latitude float
    , longitude float
    , elevation float
    , state char(2)
    , "name" varchar(255)
    , gsn_flag char(3)
    , hcn_crn_flag char(3)
    , wmo_id char(5)
    , start_ts date
    , end_ts date
    , is_current bool
    , "version" int
);

INSERT INTO dwh.dim_stations (
      station_id
    , country_id
    , latitude
    , longitude
    , elevation
    , state
    , "name"
    , gsn_flag
    , hcn_crn_flag
    , wmo_id
    , start_ts
    , end_ts
    , is_current
    , "version"
) VALUES (
      NULL
    , 0
    , NULL
    , NULL
    , NULL
    , NULL
    , NULL
    , NULL
    , NULL
    , NULL
    , NULL
    , NULL
    , NULL
    , 1
);

INSERT INTO dwh.dim_stations (
      station_id
    , country_id
    , latitude
    , longitude
    , elevation
    , state
    , "name"
    , gsn_flag
    , hcn_crn_flag
    , wmo_id
    , start_ts
    , end_ts
    , is_current
    , "version"
)
SELECT
      ss.id AS station_id
    , dc.id AS country_id
    , ss.latitude
    , ss.longitude
    , ss.elevation
    , ss.state
    , ss."name"
    , ss.gsn_flag
    , ss.hcn_crn_flag
    , ss.wmo_id
    , '1700-01-01' AS start_ts
    , '2199-12-31' AS end_ts
    , TRUE AS is_current
    , 1 AS version
FROM stage.stations ss
LEFT JOIN dwh.dim_countries dc
ON substring(ss.id, 0, 3) = dc.code;

-- DROP TABLE IF EXISTS dwh.dim_inventory CASCADE;

CREATE TABLE dwh.dim_inventory (
      id int NOT NULL REFERENCES dwh.dim_stations(id)
    , "element" char(4)
    , firstyear int
    , lastyear int
    , start_ts date
    , end_ts date
    , is_current bool
    , "version" int
);

INSERT INTO dwh.dim_inventory    
SELECT 
      st.id
    , i."element"
    , i.firstyear
    , i.lastyear
    , '1700-01-01' AS start_ts
    , '2199-12-31' AS end_ts
    , TRUE AS is_current
    , 1 AS version
FROM dwh.dim_stations AS st
LEFT JOIN stage.inventory AS i ON st.station_id = i.id;

-- DROP TABLE IF EXISTS dwh.dim_states;

-- DROP SEQUENCE IF EXISTS dim_states_null_sequence;

CREATE SEQUENCE dim_states_null_sequence
    MINVALUE 0
    start 0
    increment 1;

CREATE TABLE dwh.dim_states (
      id int UNIQUE NOT NULL DEFAULT nextval('dim_states_null_sequence')
    , code char(2)
    , "name" varchar(50)
    , start_ts date
    , end_ts date
    , is_current bool
    , "version" int
);

INSERT INTO dwh.dim_states (
      code
    , "name"
    , start_ts
    , end_ts
    , is_current
    , "version"
) VALUES (
      NULL
    , NULL
    , NULL
    , NULL
    , NULL
    , 1
);

INSERT INTO dwh.dim_states (
      code
    , "name"
    , start_ts
    , end_ts
    , is_current
    , "version"
)
SELECT
      code
    , "name"
    , '1700-01-01' AS start_ts
    , '2199-12-31' AS end_ts
    , TRUE AS is_current
    , 1 AS VERSION
FROM stage.states;

-- DROP TABLE IF EXISTS dwh.fact_temperatures CASCADE;

-- DROP TABLESPACE IF EXISTS fact_temperatures;

CREATE TABLESPACE fact_temperatures LOCATION '/mnt/tablespace/fact_temperatures';

CREATE TABLE dwh.fact_temperatures (
      id serial NOT NULL
    , station_id int NOT NULL REFERENCES dwh.dim_stations(id)
    , "year" char(4) NOT NULL
    , "date" int NOT NULL REFERENCES dwh.dim_calendar(id)
    , tmin int NOT null
    , tmax int NOT null
    , tavg int NOT null
)
PARTITION BY list("year")
TABLESPACE fact_temperatures;

DO
$$
DECLARE
    _years char(4)[] :=   '{1750, 1763, 1764, 1765, 1766, 1767, 1768, 1769, 
                            1770, 1771, 1772, 1773, 1774, 1775, 1776, 1777, 
                            1778, 1779, 1780, 1781, 1782, 1783, 1784, 1785, 
                            1786, 1787, 1788, 1789, 1790, 1791, 1792, 1793, 
                            1794, 1795, 1796, 1797, 1798, 1799, 1800, 1801, 
                            1802, 1803, 1804, 1805, 1806, 1807, 1808, 1809, 
                            1810, 1811, 1812, 1813, 1814, 1815, 1816, 1817, 
                            1818, 1819, 1820, 1821, 1822, 1823, 1824, 1825, 
                            1826, 1827, 1828, 1829, 1830, 1831, 1832, 1833, 
                            1834, 1835, 1836, 1837, 1838, 1839, 1840, 1841, 
                            1842, 1843, 1844, 1845, 1846, 1847, 1848, 1849, 
                            1850, 1851, 1852, 1853, 1854, 1855, 1856, 1857, 
                            1858, 1859, 1860, 1861, 1862, 1863, 1864, 1865, 
                            1866, 1867, 1868, 1869, 1870, 1871, 1872, 1873, 
                            1874, 1875, 1876, 1877, 1878, 1879, 1880, 1881, 
                            1882, 1883, 1884, 1885, 1886, 1887, 1888, 1889, 
                            1890, 1891, 1892, 1893, 1894, 1895, 1896, 1897, 
                            1898, 1899, 1900, 1901, 1902, 1903, 1904, 1905, 
                            1906, 1907, 1908, 1909, 1910, 1911, 1912, 1913, 
                            1914, 1915, 1916, 1917, 1918, 1919, 1920, 1921, 
                            1922, 1923, 1924, 1925, 1926, 1927, 1928, 1929, 
                            1930, 1931, 1932, 1933, 1934, 1935, 1936, 1937, 
                            1938, 1939, 1940, 1941, 1942, 1943, 1944, 1945, 
                            1946, 1947, 1948, 1949, 1950, 1951, 1952, 1953, 
                            1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 
                            1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 
                            1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 
                            1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 
                            1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 
                            1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 
                            2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 
                            2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 
                            2018, 2019, 2020, 2021}';
    _iter char(4);
BEGIN
    FOREACH _iter IN ARRAY _years LOOP
        EXECUTE format('DROP FOREIGN TABLE IF EXISTS stage.fact_raw_' || _iter);
        EXECUTE format('CREATE FOREIGN TABLE stage.fact_raw_' || _iter || '(
                              id bpchar(11) NULL
                            , "date" int4 NULL
                            , "element" bpchar(4) NULL
                            , value int4 NULL
                            , m_flag bpchar(1) NULL
                            , q_flag bpchar(1) NULL
                            , s_flag bpchar(1) NULL
                            , obs_time bpchar(4) NULL
                            ) 
                        SERVER cstore_server 
                        OPTIONS (filename ''/mnt/tablespace/postgres/fact_raw_' || _iter || '.cstore'', compression ''pglz'')');
        EXECUTE format('COPY stage.fact_raw_'|| _iter || ' FROM ''/mnt/source/'|| _iter || '.csv'' DELIMITER '','' CSV');
        RAISE NOTICE 'Year % loaded into stage table', _iter;
        EXECUTE format('CREATE TABLE dwh.fact_temperatures_' || _iter ||
                       ' PARTITION OF dwh.fact_temperatures 
                         FOR VALUES IN (' || _iter || ')
                         TABLESPACE fact_temperatures');
        EXECUTE format('
                        INSERT INTO dwh.fact_temperatures (
                              station_id
                            , year
                            , "date"
                            , tmin
                            , tmax
                            , tavg
                        )
                        WITH result_cte AS (
                            WITH outer_cte AS (
                                WITH inner_cte AS (
                                    SELECT id, date,
                                        max(CASE "element"
                                                WHEN ''TMIN'' THEN value
                                                ELSE NULL
                                            END) TMIN,
                                        max(CASE "element" 
                                                WHEN ''TMAX'' THEN value
                                                ELSE NULL
                                            END) TMAX,
                                        max(CASE "element"
                                                WHEN ''TAVG'' THEN value
                                                ELSE NULL
                                            END) TAVG
                                    FROM stage.fact_raw_' || _iter ||
                                  ' WHERE "q_flag" IS NULL and ("element" = ''TMAX'' OR "element" = ''TMIN'' OR "element" = ''TAVG'') 
                                    GROUP BY date, id)
                                SELECT    id 
                                        , date
                                        , tmin
                                        , tmax
                                        , tavg
                                        , COALESCE (tmin, tavg, tmax) AS tmin_reduced
                                        , COALESCE (tmax, tavg, tmin) AS tmax_reduced
                                FROM inner_cte)
                            SELECT    id
                                    , date
                                    , tmin
                                    , tmax
                                    , tavg
                                    , tmin_reduced
                                    , tmax_reduced
                                    , CASE 
                                        WHEN tavg IS NULL
                                        THEN (tmax_reduced + tmin_reduced)/2
                                        ELSE tavg
                                      END tavg_reduced
                            FROM outer_cte)
                        SELECT
                              stations.id AS station_id
                            , ' || _iter || ' as year
                            , res."date"
                            , res.tmin_reduced AS tmin
                            , res.tmax_reduced AS tmax
                            , res.tavg_reduced AS tavg
                        FROM result_cte AS res
                        JOIN dwh.dim_stations AS stations ON res.id = stations.station_id');
        RAISE NOTICE 'Year % loaded into fact table', _iter;
    END LOOP;
END
$$;

-- Flat mart view

-- DROP VIEW IF EXISTS dwh.view_temperatures;

CREATE VIEW dwh.view_temperatures
as
SELECT
      ft."year"
    , dc."date"
    , dc.epoch
    , dc.year_actual
    , dc.day_of_year
    , dc.month_actual
    , dc.quarter_actual
    , ds.station_id
    , ds.latitude
    , ds.longitude
    , ds.elevation 
    , ds."name"
    , dc1."name" AS country
    , ft.tmin 
    , ft.tmax 
    , ft.tavg 
FROM dwh.fact_temperatures AS ft
JOIN dwh.dim_stations AS ds ON ft.station_id = ds.id
JOIN dwh.dim_calendar AS dc ON ft."date" = dc.id
JOIN dwh.dim_countries AS dc1 ON ds.country_id = dc1.id;

-- COPY (select * from dwh.view_temperatures) TO '/mnt/source/flat_temperatures.csv' WITH DELIMITER ',' CSV;