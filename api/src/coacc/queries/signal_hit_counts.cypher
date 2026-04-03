MATCH (hit:SignalHit)
RETURN hit.signal_id AS signal_id,
       count(hit) AS hit_count,
       max(hit.last_seen_at) AS last_seen_at
ORDER BY hit_count DESC, signal_id ASC
