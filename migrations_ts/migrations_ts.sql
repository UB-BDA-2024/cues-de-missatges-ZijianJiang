CREATE TABLE IF NOT EXISTS sensor_data (
    sensor_id int NOT NULL,
    velocity float,
    temperature float,
    humidity float,
    battery_level float NOT NULL,
    last_seen timestamp NOT NULL,
    PRIMARY KEY(sensor_id, last_seen)
);

SELECT create_hypertable('sensor_data','last_seen',if_not_exists => true);

CREATE MATERIALIZED VIEW hour
WITH (timescaledb.continuous)
AS
  SELECT
    sensor_id,
    time_bucket('1 hour', last_seen) AS hour,
    AVG(velocity) AS avg_velocity,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    AVG(battery_level) AS avg_battery_level
  FROM sensor_data
  GROUP BY sensor_id, hour
WITH NO DATA;

CREATE MATERIALIZED VIEW day
WITH (timescaledb.continuous)
AS
  SELECT
    sensor_id,
    time_bucket('1 day', last_seen) AS day,
    AVG(velocity) AS avg_velocity,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    AVG(battery_level) AS avg_battery_level
  FROM sensor_data
  GROUP BY sensor_id, day
WITH NO DATA;

CREATE MATERIALIZED VIEW week
WITH (timescaledb.continuous)
AS
  SELECT
    sensor_id,
    time_bucket('1 week', last_seen) AS week,
    AVG(velocity) AS avg_velocity,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    AVG(battery_level) AS avg_battery_level
  FROM sensor_data
  GROUP BY sensor_id, week
WITH NO DATA;

CREATE MATERIALIZED VIEW month
WITH (timescaledb.continuous)
AS
  SELECT
    sensor_id,
    time_bucket('1 month', last_seen) AS month,
    AVG(velocity) AS avg_velocity,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    AVG(battery_level) AS avg_battery_level
  FROM sensor_data
  GROUP BY sensor_id, month
WITH NO DATA;