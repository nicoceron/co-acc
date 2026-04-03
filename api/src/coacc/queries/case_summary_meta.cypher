MATCH (u:User {id: $user_id})-[:OWNS]->(c:Case {id: $case_id})
OPTIONAL MATCH (c)-[:INCLUDES_SIGNAL_HIT]->(hit:SignalHit)
RETURN count(DISTINCT hit) AS signal_count,
       sum(CASE WHEN hit.public_safe THEN 1 ELSE 0 END) AS public_signal_count,
       toString(c.case_last_refreshed_at) AS last_refreshed_at,
       c.case_last_run_id AS last_run_id,
       coalesce(c.case_stale, true) AS stale
