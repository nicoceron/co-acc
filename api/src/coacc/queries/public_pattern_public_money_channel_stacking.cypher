MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
CALL {
  WITH c
  OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT award.summary_id) AS contract_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS contract_total,
         collect(DISTINCT award.summary_id) AS summary_ids,
         min(coalesce(award.first_date, award.last_date)) AS window_start,
         max(coalesce(award.last_date, award.first_date)) AS window_end
}
CALL {
  WITH c
  OPTIONAL MATCH (c)-[:SUMINISTRO]->(f:Finance {type: 'SGR_EXPENSE_EXECUTION'})
  RETURN count(DISTINCT f) AS sgr_event_count,
         coalesce(sum(coalesce(f.value, 0.0)), 0.0) AS sgr_total,
         collect(DISTINCT f.finance_id) AS sgr_ids
}
CALL {
  WITH c
  OPTIONAL MATCH (c)-[:ADMINISTRA]->(f:Finance)
  RETURN count(DISTINCT f) AS administered_flow_count,
         coalesce(sum(coalesce(f.value, 0.0) + coalesce(f.value_paid, 0.0)), 0.0) AS administered_total,
         collect(DISTINCT f.finance_id) AS administered_ids
}
CALL {
  WITH c
  OPTIONAL MATCH (c)-[:BENEFICIO]->(f:Finance)
  RETURN count(DISTINCT f) AS beneficiary_flow_count,
         coalesce(sum(coalesce(f.value, 0.0) + coalesce(f.value_paid, 0.0)), 0.0) AS beneficiary_total,
         collect(DISTINCT f.finance_id) AS beneficiary_ids
}
CALL {
  WITH c
  OPTIONAL MATCH (c)-[:OPERA_UNIDAD]->(h:Health)
  RETURN count(DISTINCT h) AS health_site_count,
         collect(DISTINCT elementId(h)) AS health_ids
}
WITH c,
     contract_count,
     contract_total,
     sgr_event_count,
     sgr_total,
     administered_flow_count,
     administered_total,
     beneficiary_flow_count,
     beneficiary_total,
     health_site_count,
     window_start,
     window_end,
     [x IN summary_ids + sgr_ids + administered_ids + beneficiary_ids + health_ids
        WHERE x IS NOT NULL AND x <> ''] AS evidence_refs,
     (
       CASE WHEN contract_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN sgr_event_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN administered_flow_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN beneficiary_flow_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN health_site_count > 0 THEN 1 ELSE 0 END
     ) AS channel_count
WHERE contract_count >= 1
  AND channel_count >= 2
RETURN 'public_money_channel_stacking' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(channel_count + contract_count) AS risk_signal,
       toInteger(channel_count) AS channel_count,
       toInteger(contract_count) AS contract_count,
       toInteger(sgr_event_count) AS sgr_event_count,
       toInteger(administered_flow_count) AS administered_flow_count,
       toInteger(beneficiary_flow_count) AS beneficiary_flow_count,
       toInteger(health_site_count) AS health_site_count,
       contract_total + sgr_total + administered_total + beneficiary_total AS amount_total,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
