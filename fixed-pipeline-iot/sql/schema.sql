-- Table for temperature readings
CREATE TABLE IF NOT EXISTS temperature_readings (
    id SERIAL PRIMARY KEY,
    room TEXT,
    ts TIMESTAMP NOT NULL,
    temperature_c NUMERIC NOT NULL,
    location TEXT CHECK (location IN ('In','Out'))
);

-- Daily stats view
CREATE OR REPLACE VIEW v_daily_stats AS
SELECT
  DATE(ts) AS day,
  AVG(temperature_c) AS temp_avg,
  MAX(temperature_c) AS temp_max,
  MIN(temperature_c) AS temp_min,
  COUNT(*)          AS readings
FROM temperature_readings
GROUP BY DATE(ts)
ORDER BY day;

-- Latest reading per room
CREATE OR REPLACE VIEW v_latest_per_room AS
SELECT DISTINCT ON (room)
  room, ts, temperature_c, location
FROM temperature_readings
ORDER BY room, ts DESC;
