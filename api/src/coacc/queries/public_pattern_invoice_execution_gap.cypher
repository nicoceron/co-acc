MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
MATCH (:Company)-[award:CONTRATOU]->(c)
WHERE coalesce(award.invoice_total_value, 0.0) >= toFloat($pattern_min_contract_value)
  AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
WITH c,
     collect(DISTINCT award.summary_id) AS summary_ids,
     sum(coalesce(award.invoice_total_value, 0.0)) AS amount_total,
     count(DISTINCT award.summary_id) AS contract_count,
     max(coalesce(award.execution_actual_progress_max, 0.0)) AS max_progress,
     min(coalesce(award.first_date, award.last_date)) AS window_start,
     max(coalesce(award.last_date, award.first_date)) AS window_end
WITH c,
     amount_total,
     contract_count,
     max_progress,
     window_start,
     window_end,
     [x IN summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE contract_count >= toInteger($pattern_min_contract_count)
RETURN 'invoice_execution_gap' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(contract_count + size(evidence_refs)) AS risk_signal,
       amount_total AS amount_total,
       toInteger(contract_count) AS contract_count,
       toFloat(max_progress) AS max_progress,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
