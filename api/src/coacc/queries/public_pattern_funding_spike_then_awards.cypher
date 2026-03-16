MATCH (c:Company)
WHERE (
    elementId(c) = $company_id
    OR c.document_id = $company_identifier
    OR c.document_id = $company_identifier_formatted
    OR c.nit = $company_identifier
    OR c.nit = $company_identifier_formatted
    OR c.cnpj = $company_identifier
    OR c.cnpj = $company_identifier_formatted
  )
  AND coalesce(c.document_id, c.nit, c.cnpj, '') <> '0'
MATCH (c)-[:SUMINISTRO]->(f:Finance {type: 'SGR_EXPENSE_EXECUTION'})
CALL {
  WITH c
  MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT award.summary_id) AS contract_count,
         sum(coalesce(award.total_value, 0.0)) AS amount_total,
         collect(DISTINCT award.summary_id) AS summary_ids
}
WITH c,
     contract_count,
     amount_total,
     count(DISTINCT f) AS funding_event_count,
     sum(coalesce(f.value, 0.0)) AS funding_amount_total,
     min(f.date) AS window_start,
     max(f.date) AS window_end,
     [x IN summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE contract_count >= toInteger($pattern_min_contract_count)
  AND funding_event_count >= 2
RETURN 'funding_spike_then_awards' AS pattern_id,
       coalesce(c.document_id, c.nit, c.cnpj) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(contract_count + funding_event_count) AS risk_signal,
       toInteger(contract_count) AS contract_count,
       amount_total AS amount_total,
       toInteger(funding_event_count) AS funding_event_count,
       funding_amount_total AS funding_amount_total,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
