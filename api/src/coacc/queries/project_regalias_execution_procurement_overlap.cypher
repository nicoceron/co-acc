MATCH (pr:Project)
WHERE elementId(pr) = $entity_id
   OR coalesce(pr.project_id, pr.bpin_code) = $entity_key
OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(:Company)
WHERE coalesce(award.bpin_code, '') = coalesce(pr.project_id, pr.bpin_code)
OPTIONAL MATCH (company:Company)-[:SUMINISTRO]->(f:Finance {type: 'SGR_EXPENSE_EXECUTION'})
WHERE coalesce(f.project_id, '') = coalesce(pr.project_id, pr.bpin_code)
WITH pr,
     collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS contract_refs,
     collect(DISTINCT f.finance_id)[0..toInteger($pattern_max_evidence_refs)] AS finance_refs,
     count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_count,
     count(DISTINCT f) AS finance_count,
     sum(coalesce(f.value, f.amount, 0.0)) AS finance_total
WHERE contract_count > 0
  AND finance_count > 0
RETURN coalesce(pr.project_id, pr.bpin_code) AS scope_key,
       toFloat(contract_count + finance_count) + (finance_total / 1000000000.0) AS risk_signal,
       contract_refs + finance_refs AS evidence_refs,
       size(contract_refs + finance_refs) AS evidence_count,
       'EXACT_BPIN' AS identity_match_type,
       'exact' AS identity_quality,
       'Project:' + coalesce(pr.project_id, pr.bpin_code) AS node_ref
