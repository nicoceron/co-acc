MATCH (run:IngestionRun)
WITH run.source_id AS source_id, run
ORDER BY source_id, run.started_at DESC
WITH source_id, collect(run)[0] AS latest
RETURN source_id, latest.status AS status
