MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
   OR c.cnpj = $company_identifier
   OR c.cnpj = $company_identifier_formatted
MATCH (buyer:Company)-[award:CONTRATOU]->(c)
WHERE coalesce(award.buyer_name, buyer.razao_social, buyer.name) IS NOT NULL
  AND trim(coalesce(award.buyer_name, buyer.razao_social, buyer.name)) <> ''
  AND award.total_value IS NOT NULL
WITH c,
     coalesce(award.buyer_name, buyer.razao_social, buyer.name) AS contracting_org,
     sum(coalesce(award.total_value, 0.0)) AS company_org_total
CALL {
  WITH contracting_org
  MATCH (org:Company)-[org_award:CONTRATOU]->(:Company)
  WHERE coalesce(org_award.buyer_name, org.razao_social, org.name) = contracting_org
    AND org_award.total_value IS NOT NULL
  RETURN sum(coalesce(org_award.total_value, 0.0)) AS org_total
}
WITH c, contracting_org, company_org_total, org_total
WHERE org_total > 0
  AND (company_org_total / org_total) >= toFloat($pattern_share_threshold)
WITH c, collect(DISTINCT contracting_org) AS risky_orgs
WHERE size(risky_orgs) > 0
MATCH (buyer:Company)-[risk_award:CONTRATOU]->(c)
WHERE coalesce(risk_award.buyer_name, buyer.razao_social, buyer.name) IN risky_orgs
WITH c,
     risky_orgs,
     reduce(
       refs = [],
       ref_list IN collect(coalesce(risk_award.evidence_refs, [risk_award.summary_id])) |
         refs + ref_list
     ) AS contract_ids,
     sum(coalesce(risk_award.total_value, 0.0)) AS amount_total,
     min(coalesce(risk_award.first_date, risk_award.last_date)) AS window_start,
     max(coalesce(risk_award.last_date, risk_award.first_date)) AS window_end
WITH c,
     risky_orgs,
     amount_total,
     window_start,
     window_end,
     [x IN contract_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE size(evidence_refs) > 0
RETURN 'contract_concentration' AS pattern_id,
       coalesce(c.document_id, c.nit, c.cnpj) AS company_identifier,
       c.razao_social AS company_name,
       toFloat(size(risky_orgs) + size(evidence_refs)) AS risk_signal,
       amount_total AS amount_total,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
