CALL {
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Company'
  MATCH (c:Company)
  WHERE elementId(c) = entity_id
     OR coalesce(c.nit, c.document_id) = entity_key
  MATCH (c)-[:BENEFICIARIO_FINAL]->(p:Person)
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
  WITH p,
       collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs
  RETURN coalesce(p.document_id, p.cedula, elementId(p)) AS scope_key,
         toFloat(size(evidence_refs) + 1) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_COMPANY_NIT' AS identity_match_type,
         'exact' AS identity_quality,
         'Person:' + coalesce(p.document_id, p.cedula, elementId(p)) AS node_ref
  UNION
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Person'
  MATCH (p:Person)
  WHERE elementId(p) = entity_id
     OR coalesce(p.document_key, p.document_id, p.cedula) = entity_key
  MATCH (c:Company)-[:BENEFICIARIO_FINAL]->(p)
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
  WITH c,
       collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs
  RETURN coalesce(c.document_id, c.nit, elementId(c)) AS scope_key,
         toFloat(size(evidence_refs) + 1) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_PERSON_DOCUMENT' AS identity_match_type,
         'exact' AS identity_quality,
         'Company:' + coalesce(c.document_id, c.nit, elementId(c)) AS node_ref
}
RETURN scope_key, risk_signal, evidence_refs, evidence_count, identity_match_type, identity_quality, node_ref
