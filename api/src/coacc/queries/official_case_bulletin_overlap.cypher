CALL {
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Company'
  MATCH (c:Company)
  WHERE elementId(c) = entity_id
     OR coalesce(c.nit, c.document_id) = entity_key
  MATCH (c)-[r:REFERENTE_A]->(i:Inquiry)
  WHERE coalesce(i.type, '') = 'OFFICIAL_CASE_BULLETIN'
    AND coalesce(r.subject_match, '') = 'document_id'
    AND coalesce(r.public_safe, false)
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
  WITH collect(DISTINCT coalesce(i.inquiry_id, elementId(i))) AS inquiry_refs,
       collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS award_refs
  WITH [x IN inquiry_refs + award_refs WHERE x IS NOT NULL AND x <> ''][0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       inquiry_refs
  WHERE size(inquiry_refs) > 0
  RETURN coalesce(inquiry_refs[0], entity_key) AS scope_key,
         toFloat(size(inquiry_refs) + size(award_refs)) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_COMPANY_NIT' AS identity_match_type,
         'exact' AS identity_quality,
         'Inquiry:' + coalesce(inquiry_refs[0], entity_key) AS node_ref
  UNION
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Person'
  MATCH (p:Person)
  WHERE elementId(p) = entity_id
     OR coalesce(p.document_key, p.document_id, p.cedula) = entity_key
  MATCH (p)-[r:REFERENTE_A]->(i:Inquiry)
  WHERE coalesce(i.type, '') = 'OFFICIAL_CASE_BULLETIN'
    AND coalesce(r.subject_match, '') = 'document_id'
    AND coalesce(r.public_safe, false)
  OPTIONAL MATCH (p)-[:OFFICER_OF]->(c:Company)
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
  WITH collect(DISTINCT coalesce(i.inquiry_id, elementId(i))) AS inquiry_refs,
       collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS award_refs
  WITH [x IN inquiry_refs + award_refs WHERE x IS NOT NULL AND x <> ''][0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       inquiry_refs
  WHERE size(inquiry_refs) > 0
  RETURN coalesce(inquiry_refs[0], entity_key) AS scope_key,
         toFloat(size(inquiry_refs) + size(award_refs)) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_PERSON_DOCUMENT' AS identity_match_type,
         'exact' AS identity_quality,
         'Inquiry:' + coalesce(inquiry_refs[0], entity_key) AS node_ref
}
RETURN scope_key, risk_signal, evidence_refs, evidence_count, identity_match_type, identity_quality, node_ref
