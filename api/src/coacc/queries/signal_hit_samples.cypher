MATCH (hit:SignalHit)
WHERE hit.signal_id IN $signal_ids
OPTIONAL MATCH (hit)-[:SUSTENTADO_POR]->(bundle:EvidenceBundle)
OPTIONAL MATCH (bundle)-[:CONTIENE]->(item:EvidenceItem)
WITH hit, collect(DISTINCT item) AS items
ORDER BY hit.last_seen_at DESC
LIMIT $limit
RETURN hit.hit_id AS hit_id,
       hit.run_id AS run_id,
       hit.signal_id AS signal_id,
       hit.signal_version AS signal_version,
       hit.title AS title,
       hit.description AS description,
       hit.category AS category,
       hit.severity AS severity,
       hit.public_safe AS public_safe,
       hit.reviewer_only AS reviewer_only,
       hit.entity_id AS entity_id,
       hit.entity_key AS entity_key,
       hit.entity_label AS entity_label,
       hit.scope_key AS scope_key,
       hit.scope_type AS scope_type,
       hit.dedup_key AS dedup_key,
       hit.score AS score,
       hit.identity_confidence AS identity_confidence,
       hit.identity_match_type AS identity_match_type,
       hit.identity_quality AS identity_quality,
       hit.evidence_count AS evidence_count,
       bundle.bundle_id AS evidence_bundle_id,
       hit.evidence_refs AS evidence_refs,
       hit.data_json AS data_json,
       hit.sources AS sources,
       hit.created_at AS created_at,
       hit.first_seen_at AS first_seen_at,
       hit.last_seen_at AS last_seen_at,
       [entry IN items WHERE entry IS NOT NULL | {
         item_id: entry.item_id,
         source_id: entry.source_id,
         record_id: entry.record_id,
         url: entry.url,
         label: entry.label,
         item_type: entry.item_type,
         node_ref: entry.node_ref,
         observed_at: toString(entry.observed_at),
         public_safe: coalesce(entry.public_safe, true),
         identity_match_type: entry.identity_match_type,
         identity_quality: entry.identity_quality
       }] AS evidence_items
