MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
MATCH (p:Person)-[:OFFICER_OF]->(c)
MATCH (p)-[salary:RECIBIO_SALARIO]->(o:PublicOffice)
WHERE coalesce(salary.sensitive_position, false)
   OR coalesce(o.sensitive_position, false)
OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
WITH c,
     count(DISTINCT p) AS sensitive_officer_count,
     count(DISTINCT o) AS sensitive_role_count,
     collect(DISTINCT coalesce(o.office_id, o.name, o.role_name, o.org)) AS office_refs,
     collect(DISTINCT award.summary_id) AS summary_ids,
     coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS amount_total,
     count(DISTINCT award.summary_id) AS contract_count,
     min(coalesce(award.first_date, award.last_date)) AS window_start,
     max(coalesce(award.last_date, award.first_date)) AS window_end
WITH c,
     sensitive_officer_count,
     sensitive_role_count,
     amount_total,
     contract_count,
     window_start,
     window_end,
     [x IN office_refs + summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE contract_count >= 1
RETURN 'sensitive_public_official_supplier_overlap' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(sensitive_officer_count + sensitive_role_count + contract_count) AS risk_signal,
       amount_total AS amount_total,
       toInteger(contract_count) AS contract_count,
       toInteger(sensitive_officer_count) AS sensitive_officer_count,
       toInteger(sensitive_role_count) AS sensitive_role_count,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
