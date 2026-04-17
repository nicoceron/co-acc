MATCH (run:SignalRun)
WHERE run.status = 'completed'
RETURN run.run_id AS run_id,
       run.finished_at AS finished_at,
       run.status AS status,
       run.hit_count AS hit_count
ORDER BY run.finished_at DESC
LIMIT 1
