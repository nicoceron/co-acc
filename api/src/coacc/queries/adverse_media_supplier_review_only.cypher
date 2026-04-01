CALL {
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Company'
  MATCH (c:Company)
  WHERE elementId(c) = entity_id
     OR coalesce(c.nit, c.document_id) = entity_key
  MATCH (c)-[:MENCIONADO_EN]->(m:MediaItem)
  WITH collect(DISTINCT coalesce(m.url, m.media_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       count(DISTINCT m) AS media_count
  WHERE media_count > 0
  RETURN coalesce(evidence_refs[0], entity_key) AS scope_key,
         toFloat(media_count + size(evidence_refs)) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_COMPANY_NIT' AS identity_match_type,
         'exact' AS identity_quality,
         'MediaItem:' + coalesce(evidence_refs[0], entity_key) AS node_ref
  UNION
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Person'
  MATCH (p:Person)
  WHERE elementId(p) = entity_id
     OR coalesce(p.document_key, p.document_id, p.cedula) = entity_key
  MATCH (p)-[:MENCIONADO_EN]->(m:MediaItem)
  WITH collect(DISTINCT coalesce(m.url, m.media_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       count(DISTINCT m) AS media_count
  WHERE media_count > 0
  RETURN coalesce(evidence_refs[0], entity_key) AS scope_key,
         toFloat(media_count + size(evidence_refs)) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_PERSON_DOCUMENT' AS identity_match_type,
         'exact' AS identity_quality,
         'MediaItem:' + coalesce(evidence_refs[0], entity_key) AS node_ref
}
RETURN scope_key, risk_signal, evidence_refs, evidence_count, identity_match_type, identity_quality, node_ref
