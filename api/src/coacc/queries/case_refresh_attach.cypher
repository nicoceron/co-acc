MATCH (u:User {id: $user_id})-[:OWNS]->(c:Case {id: $case_id})
UNWIND $hits AS hit
MATCH (signal:SignalHit {hit_id: hit.hit_id})
MERGE (c)-[:INCLUDES_SIGNAL_HIT]->(signal)
MERGE (event:CaseEvent {event_id: hit.event_id})
SET event.type = hit.type,
    event.label = hit.label,
    event.date = datetime(hit.date),
    event.entity_id = hit.entity_id,
    event.signal_hit_id = hit.hit_id,
    event.evidence_bundle_id = hit.evidence_bundle_id,
    event.created_at = datetime(hit.date),
    event.updated_at = datetime(hit.date)
MERGE (c)-[:HAS_EVENT]->(event)
WITH event, hit
OPTIONAL MATCH (bundle:EvidenceBundle {bundle_id: hit.evidence_bundle_id})
FOREACH (_ IN CASE WHEN bundle IS NULL THEN [] ELSE [1] END |
  MERGE (event)-[:EVIDENCIA]->(bundle)
)

