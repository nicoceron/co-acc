MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
MATCH (buyer:Company)-[award:CONTRATOU]->(c)
WITH c,
     buyer,
     collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS award_refs,
     count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_count,
     sum(coalesce(award.total_value, 0.0)) AS amount_total
WHERE contract_count >= toInteger($pattern_min_contract_count)
RETURN coalesce(buyer.document_id, buyer.nit, elementId(buyer)) AS scope_key,
       toFloat(contract_count) + (amount_total / 1000000000.0) AS risk_signal,
       award_refs AS evidence_refs,
       size(award_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Company:' + coalesce(buyer.document_id, buyer.nit, elementId(buyer)) AS node_ref
