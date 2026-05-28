-- Add segments_fetched flag to strava_activities
ALTER TABLE strava_activities
  ADD COLUMN segments_fetched TINYINT(1) NOT NULL DEFAULT 0;

-- Optional backfill: if you already have run/ride segments table populated,
-- mark activities that have segments as fetched.
-- Uncomment and run if you want to backfill based on existing segment tables.
--
-- UPDATE strava_activities a
-- JOIN (
--   SELECT DISTINCT activity_id FROM strava_run_segments
--   UNION
--   SELECT DISTINCT activity_id FROM strava_ride_segments
-- ) s ON a.activity_id = s.activity_id
-- SET a.segments_fetched = 1;
