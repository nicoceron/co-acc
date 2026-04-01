MATCH (pr:Project)
WHERE elementId(pr) = $entity_id
   OR coalesce(pr.project_id, pr.bpin_code) = $entity_key
OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(:Company)
WHERE coalesce(award.bpin_code, '') = coalesce(pr.project_id, pr.bpin_code)
WITH pr,
     collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_count,
     sum(coalesce(award.total_value, 0.0)) AS amount_total
WHERE contract_count > 0
RETURN coalesce(pr.project_id, pr.bpin_code) AS scope_key,
       toFloat(contract_count) + (amount_total / 1000000000.0) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_BPIN' AS identity_match_type,
       'exact' AS identity_quality,
       'Project:' + coalesce(pr.project_id, pr.bpin_code) AS node_ref
