CALL {
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Company'
  MATCH (c:Company)
  WHERE elementId(c) = entity_id
     OR coalesce(c.nit, c.document_id) = entity_key
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
  OPTIONAL MATCH (j:JudicialCase)
  WHERE entity_key <> ''
    AND toUpper(coalesce(j.search_text, '')) CONTAINS toUpper(entity_key)
  WITH collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS award_refs,
       collect(DISTINCT coalesce(j.radicado, j.case_id)) AS case_refs
  WITH [x IN award_refs + case_refs WHERE x IS NOT NULL AND x <> ''][0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(case_refs) AS case_count
  WHERE case_count > 0
  RETURN coalesce(case_refs[0], entity_key) AS scope_key,
         toFloat(case_count + size(award_refs)) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_COMPANY_NIT' AS identity_match_type,
         'exact' AS identity_quality,
         'JudicialCase:' + coalesce(case_refs[0], entity_key) AS node_ref
  UNION
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Person'
  MATCH (p:Person)
  WHERE elementId(p) = entity_id
     OR coalesce(p.document_key, p.document_id, p.cedula) = entity_key
  OPTIONAL MATCH (p)-[:OFFICER_OF]->(c:Company)
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
  OPTIONAL MATCH (j:JudicialCase)
  WHERE entity_key <> ''
    AND toUpper(coalesce(j.search_text, '')) CONTAINS toUpper(entity_key)
  WITH collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS award_refs,
       collect(DISTINCT coalesce(j.radicado, j.case_id)) AS case_refs
  WITH [x IN award_refs + case_refs WHERE x IS NOT NULL AND x <> ''][0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(case_refs) AS case_count
  WHERE case_count > 0
  RETURN coalesce(case_refs[0], entity_key) AS scope_key,
         toFloat(case_count + size(award_refs)) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_PERSON_DOCUMENT' AS identity_match_type,
         'exact' AS identity_quality,
         'JudicialCase:' + coalesce(case_refs[0], entity_key) AS node_ref
}
RETURN scope_key, risk_signal, evidence_refs, evidence_count, identity_match_type, identity_quality, node_ref
