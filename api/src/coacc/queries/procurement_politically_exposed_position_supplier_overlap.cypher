CALL {
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Person'
  MATCH (p:Person)
  WHERE elementId(p) = entity_id
     OR coalesce(p.document_key, p.document_id, p.cedula) = entity_key
  MATCH (p)-[salary:RECIBIO_SALARIO]->(o:PublicOffice)
  WHERE coalesce(salary.sensitive_position, false) OR coalesce(o.sensitive_position, false)
  OPTIONAL MATCH (p)-[:OFFICER_OF]->(c:Company)
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
  WITH p, c,
       collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       count(DISTINCT o) AS role_count
  WHERE role_count > 0 AND c IS NOT NULL
  RETURN coalesce(c.document_id, c.nit, entity_key) AS scope_key,
         toFloat(role_count + size(evidence_refs)) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_PERSON_DOCUMENT' AS identity_match_type,
         'exact' AS identity_quality,
         'Person:' + coalesce(p.document_id, p.cedula, entity_key) AS node_ref
  UNION
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Company'
  MATCH (c:Company)
  WHERE elementId(c) = entity_id
     OR coalesce(c.nit, c.document_id) = entity_key
  MATCH (p:Person)-[salary:RECIBIO_SALARIO]->(o:PublicOffice)
  WHERE coalesce(salary.sensitive_position, false) OR coalesce(o.sensitive_position, false)
  MATCH (p)-[:OFFICER_OF]->(c)
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
  WITH p,
       collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       count(DISTINCT o) AS role_count
  WHERE role_count > 0
  RETURN coalesce(p.document_id, p.cedula, elementId(p)) AS scope_key,
         toFloat(role_count + size(evidence_refs)) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_COMPANY_NIT' AS identity_match_type,
         'exact' AS identity_quality,
         'Company:' + entity_key AS node_ref
}
RETURN scope_key, risk_signal, evidence_refs, evidence_count, identity_match_type, identity_quality, node_ref
