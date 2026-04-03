MATCH (run:SignalRun)
WHERE run.scope_type = 'entity'
  AND (run.entity_id = $entity_id OR run.entity_key = $entity_key)
RETURN run.run_id AS run_id,
       run.finished_at AS finished_at,
       run.status AS status
ORDER BY coalesce(run.finished_at, run.started_at) DESC
LIMIT 1

