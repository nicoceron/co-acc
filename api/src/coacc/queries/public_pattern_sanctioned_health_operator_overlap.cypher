MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
MATCH (c)-[:OPERA_UNIDAD]->(h:Health)
MATCH (c)-[:SANCIONADA]->(s:Sanction)
OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
WITH c,
     count(DISTINCT h) AS health_site_count,
     count(DISTINCT s) AS sanction_count,
     count(DISTINCT award.summary_id) AS contract_count,
     coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS amount_total,
     collect(DISTINCT elementId(h)) AS health_ids,
     collect(DISTINCT coalesce(s.sanction_id, s.reference, s.name)) AS sanction_ids,
     collect(DISTINCT award.summary_id) AS summary_ids,
     min(coalesce(award.first_date, award.last_date)) AS window_start,
     max(coalesce(award.last_date, award.first_date)) AS window_end
WITH c,
     health_site_count,
     sanction_count,
     contract_count,
     amount_total,
     window_start,
     window_end,
     [x IN health_ids + sanction_ids + summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE contract_count >= 1
RETURN 'sanctioned_health_operator_overlap' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(health_site_count + sanction_count + contract_count) AS risk_signal,
       toInteger(health_site_count) AS health_site_count,
       toInteger(sanction_count) AS sanction_count,
       toInteger(contract_count) AS contract_count,
       amount_total AS amount_total,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
