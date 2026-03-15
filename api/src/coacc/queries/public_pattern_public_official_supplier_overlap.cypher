MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
   OR c.cnpj = $company_identifier
   OR c.cnpj = $company_identifier_formatted
MATCH (p:Person)-[:OFFICER_OF]->(c)
WHERE EXISTS { (p)-[:RECEBEU_SALARIO]->(:PublicOffice) }
OPTIONAL MATCH (p)-[:RECEBEU_SALARIO]->(o:PublicOffice)
OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
WITH c,
     collect(DISTINCT coalesce(p.name, p.nome, p.document_id))[0..5] AS official_names,
     count(DISTINCT p) AS official_officer_count,
     count(DISTINCT o) AS official_role_count,
     collect(DISTINCT award.summary_id) AS summary_ids,
     sum(coalesce(award.total_value, 0.0)) AS amount_total,
     count(DISTINCT award.summary_id) AS contract_count,
     min(coalesce(award.first_date, award.last_date)) AS window_start,
     max(coalesce(award.last_date, award.first_date)) AS window_end
WITH c,
     official_names,
     official_officer_count,
     official_role_count,
     amount_total,
     contract_count,
     window_start,
     window_end,
     [x IN summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE contract_count >= 1
RETURN 'public_official_supplier_overlap' AS pattern_id,
       coalesce(c.document_id, c.nit, c.cnpj) AS company_identifier,
       coalesce(c.razao_social, c.name) AS company_name,
       toFloat(official_officer_count + official_role_count + contract_count) AS risk_signal,
       amount_total AS amount_total,
       toInteger(contract_count) AS contract_count,
       toInteger(official_officer_count) AS official_officer_count,
       toInteger(official_role_count) AS official_role_count,
       official_names AS official_names,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
