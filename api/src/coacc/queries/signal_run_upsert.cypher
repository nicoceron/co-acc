MERGE (run:SignalRun {run_id: $run_id})
ON CREATE SET
  run.created_at = datetime($started_at)
SET
  run.registry_version = $registry_version,
  run.scope_type = coalesce($scope_type, 'entity'),
  run.scope_ref = coalesce($scope_ref, $entity_key),
  run.entity_id = $entity_id,
  run.entity_key = $entity_key,
  run.lang = $lang,
  run.trigger = $trigger,
  run.created_by = $created_by,
  run.git_sha = $git_sha,
  run.data_snapshot_id = $data_snapshot_id,
  run.started_at = datetime($started_at),
  run.finished_at = CASE
    WHEN $finished_at IS NULL THEN run.finished_at
    ELSE datetime($finished_at)
  END,
  run.status = $status,
  run.hit_count = $hit_count
RETURN run.run_id AS run_id
