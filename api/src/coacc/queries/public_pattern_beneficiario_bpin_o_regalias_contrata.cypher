MATCH (c:Company)
WHERE (
    elementId(c) = $company_id
    OR c.document_id = $company_identifier
    OR c.document_id = $company_identifier_formatted
    OR c.nit = $company_identifier
    OR c.nit = $company_identifier_formatted
  )
CALL {
  WITH c
  OPTIONAL MATCH (c)-[:ADMINISTRA|EJECUTA|REFERENTE_A]->(p:Project)
  RETURN count(DISTINCT p) AS project_count,
         collect(DISTINCT p.project_id) AS project_ids,
         collect(DISTINCT coalesce(p.project_id, p.bpin_code)) AS project_refs
}
CALL {
  WITH c
  OPTIONAL MATCH (c)-[:SUMINISTRO]->(f:Finance {type: 'SGR_EXPENSE_EXECUTION'})
  WHERE coalesce(f.project_id, '') <> ''
  RETURN count(DISTINCT f) AS finance_event_count,
         sum(coalesce(f.value, 0.0)) AS funding_amount_total,
         collect(DISTINCT f.project_id) AS finance_project_ids,
         collect(DISTINCT f.finance_id) AS finance_refs
}
CALL {
  WITH c
  MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_count,
         sum(coalesce(award.total_value, 0.0)) AS amount_total,
         min(coalesce(award.first_date, award.last_date)) AS window_start,
         max(coalesce(award.last_date, award.first_date)) AS window_end,
         collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_refs,
         collect(DISTINCT award.bpin_code) AS contract_bpin_codes
}
WITH c,
     contract_count,
     amount_total,
     window_start,
     window_end,
     [x IN contract_refs WHERE x IS NOT NULL AND x <> ''] AS contract_refs,
     [x IN contract_bpin_codes WHERE x IS NOT NULL AND x <> '' AND x <> '0'] AS contract_bpin_codes,
     project_count,
     finance_event_count,
     funding_amount_total,
     [x IN project_ids WHERE x IS NOT NULL AND x <> '' AND x <> '0'] AS project_ids,
     [x IN finance_project_ids WHERE x IS NOT NULL AND x <> '' AND x <> '0'] AS finance_project_ids,
     [x IN project_refs WHERE x IS NOT NULL AND x <> '' AND x <> '0'] AS project_refs,
     [x IN finance_refs WHERE x IS NOT NULL AND x <> ''] AS finance_refs
WITH c,
     contract_count,
     amount_total,
     window_start,
     window_end,
     project_count,
     finance_event_count,
     funding_amount_total,
     project_ids + finance_project_ids AS all_project_ids,
     contract_refs,
     project_refs,
     finance_refs,
     [x IN contract_bpin_codes WHERE x IN (project_ids + finance_project_ids)] AS overlap_bpin_codes
WITH c,
     contract_count,
     project_count,
     finance_event_count,
     funding_amount_total,
     amount_total,
     window_start,
     window_end,
     overlap_bpin_codes,
     contract_refs + project_refs + finance_refs + overlap_bpin_codes AS evidence_refs
WHERE contract_count >= toInteger($pattern_min_contract_count)
  AND (project_count > 0 OR finance_event_count > 0)
RETURN 'beneficiario_bpin_o_regalias_contrata' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(contract_count + project_count + finance_event_count + size(overlap_bpin_codes)) AS risk_signal,
       toInteger(contract_count) AS contract_count,
       toInteger(project_count) AS project_count,
       toInteger(finance_event_count) AS finance_event_count,
       toInteger(size(overlap_bpin_codes)) AS overlapping_bpin_count,
       amount_total AS amount_total,
       funding_amount_total AS funding_amount_total,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
