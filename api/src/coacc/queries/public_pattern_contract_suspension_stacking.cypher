MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
MATCH (:Company)-[award:CONTRATOU]->(c)
WHERE coalesce(award.suspension_event_count, 0) > 0
WITH c,
     collect(DISTINCT award.summary_id) AS summary_ids,
     count(DISTINCT award.summary_id) AS contract_count,
     coalesce(sum(coalesce(award.suspension_event_count, 0)), 0) AS suspension_event_count,
     coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS amount_total,
     coalesce(sum(coalesce(award.invoice_total_value, 0.0)), 0.0) AS invoice_total,
     coalesce(sum(coalesce(award.addition_event_count, 0)), 0) AS addition_event_count,
     min(coalesce(award.first_date, award.last_date)) AS window_start,
     max(coalesce(award.latest_suspension_date, award.last_date, award.first_date)) AS window_end
WITH c,
     contract_count,
     suspension_event_count,
     amount_total,
     invoice_total,
     addition_event_count,
     window_start,
     window_end,
     [x IN summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE contract_count >= toInteger($pattern_min_contract_count)
   OR suspension_event_count >= 2
RETURN 'contract_suspension_stacking' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(contract_count + suspension_event_count + addition_event_count) AS risk_signal,
       amount_total AS amount_total,
       toFloat(invoice_total) AS invoice_total,
       toInteger(contract_count) AS contract_count,
       toInteger(suspension_event_count) AS suspension_event_count,
       toInteger(addition_event_count) AS addition_event_count,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
