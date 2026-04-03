MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
MATCH (buyer:Company)-[award:CONTRATOU]->(c)
WITH collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     count(DISTINCT buyer) AS buyer_count,
     count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_count
WHERE buyer_count >= 5
  AND contract_count >= toInteger($pattern_min_contract_count)
RETURN $entity_key AS scope_key,
       toFloat(buyer_count + contract_count) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Company:' + $entity_key AS node_ref
