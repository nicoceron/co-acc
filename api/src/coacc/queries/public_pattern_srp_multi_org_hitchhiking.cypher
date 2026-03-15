MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
   OR c.cnpj = $company_identifier
   OR c.cnpj = $company_identifier_formatted
MATCH (buyer:Company)-[award:ADJUDICOU_A]->(c)
WHERE coalesce(award.srp, false) = true
WITH c,
     collect(DISTINCT coalesce(award.buyer_name, buyer.razao_social, buyer.name)) AS orgs,
     collect(coalesce(award.evidence_refs, [award.summary_id])) AS id_groups,
     sum(coalesce(award.total_value, 0.0)) AS amount_total,
     min(coalesce(award.first_date, award.last_date)) AS window_start,
     max(coalesce(award.last_date, award.first_date)) AS window_end
WHERE size(orgs) >= toInteger($pattern_srp_min_orgs)
WITH c,
     amount_total,
     window_start,
     window_end,
     orgs,
     reduce(flat = [], ids IN id_groups | flat + ids) AS evidence_refs
WHERE size(evidence_refs) > 0
RETURN 'srp_multi_org_hitchhiking' AS pattern_id,
       coalesce(c.document_id, c.nit, c.cnpj) AS company_identifier,
       c.razao_social AS company_name,
       toFloat(size(orgs) + size(evidence_refs)) AS risk_signal,
       amount_total AS amount_total,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
