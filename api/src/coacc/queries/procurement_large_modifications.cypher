MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
MATCH (:Company)-[award:CONTRATOU]->(c)
WHERE coalesce(award.modification_event_count, 0) > 0
   OR coalesce(award.addition_event_count, 0) > 0
WITH c,
     collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     sum(coalesce(award.modification_total_value, 0.0) + coalesce(award.addition_total_value, 0.0)) AS delta_total,
     sum(coalesce(award.total_value, 0.0)) AS amount_total,
     sum(coalesce(award.modification_event_count, 0) + coalesce(award.addition_event_count, 0)) AS event_count
WHERE event_count > 0
  AND (amount_total = 0 OR delta_total >= amount_total * 0.5)
RETURN coalesce(evidence_refs[0], $entity_key) AS scope_key,
       toFloat(event_count) + (delta_total / 1000000000.0) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Contract:' + coalesce(evidence_refs[0], $entity_key) AS node_ref
