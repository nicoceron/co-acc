MATCH (u:User {id: $user_id})-[:OWNS]->(c:Case {id: $case_id})
OPTIONAL MATCH (c)-[existing:INCLUDES_SIGNAL_HIT]->(:SignalHit)
DELETE existing
WITH c
OPTIONAL MATCH (c)-[:HAS_EVENT]->(event:CaseEvent)
DETACH DELETE event
WITH c
SET c.case_last_refreshed_at = datetime($refreshed_at),
    c.case_last_run_id = $run_id,
    c.case_stale = false
RETURN c.id AS case_id

