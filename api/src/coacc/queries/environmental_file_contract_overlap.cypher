CALL {
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Company'
  MATCH (c:Company)
  WHERE elementId(c) = entity_id
     OR coalesce(c.nit, c.document_id) = entity_key
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
  OPTIONAL MATCH (c)-[:REGISTRO_AMBIENTAL]->(e:EnvironmentalFile)
  WITH collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS award_refs,
       collect(DISTINCT coalesce(e.file_id, e.expediente)) AS env_refs
  WITH [x IN award_refs + env_refs WHERE x IS NOT NULL AND x <> ''][0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(env_refs) AS env_count
  WHERE env_count > 0
  RETURN coalesce(env_refs[0], entity_key) AS scope_key,
         toFloat(env_count + size(award_refs)) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_COMPANY_NIT' AS identity_match_type,
         'exact' AS identity_quality,
         'EnvironmentalFile:' + coalesce(env_refs[0], entity_key) AS node_ref
  UNION
  WITH $entity_id AS entity_id, $entity_key AS entity_key, $entity_label AS entity_label
  WITH entity_id, entity_key WHERE entity_label = 'Project'
  MATCH (pr:Project)
  WHERE elementId(pr) = entity_id
     OR coalesce(pr.project_id, pr.bpin_code) = entity_key
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(:Company)
  WHERE coalesce(award.bpin_code, '') = coalesce(pr.project_id, pr.bpin_code)
  OPTIONAL MATCH (e:EnvironmentalFile)
  WHERE entity_key <> '' AND toUpper(coalesce(e.search_text, '')) CONTAINS toUpper(entity_key)
  WITH collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS award_refs,
       collect(DISTINCT coalesce(e.file_id, e.expediente)) AS env_refs
  WITH [x IN award_refs + env_refs WHERE x IS NOT NULL AND x <> ''][0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(env_refs) AS env_count
  WHERE env_count > 0
  RETURN entity_key AS scope_key,
         toFloat(env_count + size(award_refs)) AS risk_signal,
         evidence_refs,
         size(evidence_refs) AS evidence_count,
         'EXACT_BPIN' AS identity_match_type,
         'exact' AS identity_quality,
         'Project:' + entity_key AS node_ref
}
RETURN scope_key, risk_signal, evidence_refs, evidence_count, identity_match_type, identity_quality, node_ref
