MATCH (u:User {id: $user_id})-[:OWNS]->(c:Case {id: $case_id})-[:HAS_EVENT]->(event:CaseEvent)
OPTIONAL MATCH (event)-[:EVIDENCIA]->(bundle:EvidenceBundle)
OPTIONAL MATCH (bundle)-[:CONTIENE]->(item:EvidenceItem)
WITH event, bundle, collect(DISTINCT item) AS items
ORDER BY event.date DESC
RETURN event.event_id AS id,
       event.type AS type,
       event.label AS label,
       toString(event.date) AS date,
       event.entity_id AS entity_id,
       event.signal_hit_id AS signal_hit_id,
       event.evidence_bundle_id AS evidence_bundle_id,
       size([entry IN items WHERE entry IS NOT NULL]) AS bundle_document_count

