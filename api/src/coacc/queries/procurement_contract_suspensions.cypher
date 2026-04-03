MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
MATCH (:Company)-[award:CONTRATOU]->(c)
WHERE coalesce(award.suspension_event_count, 0) > 0
WITH collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     sum(coalesce(award.suspension_event_count, 0)) AS suspension_count,
     count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_count
WHERE contract_count >= 1
  AND suspension_count >= 2
RETURN coalesce(evidence_refs[0], $entity_key) AS scope_key,
       toFloat(contract_count + suspension_count) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Contract:' + coalesce(evidence_refs[0], $entity_key) AS node_ref
