MATCH (run:SignalRun)
WHERE run.status = 'completed'
RETURN run.run_id AS run_id,
       run.finished_at AS finished_at
ORDER BY run.finished_at DESC
LIMIT 1
