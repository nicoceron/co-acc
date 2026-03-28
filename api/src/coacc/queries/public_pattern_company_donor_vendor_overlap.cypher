MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
OPTIONAL MATCH (c)-[d:DONO_A]->(e:Election)
OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
WITH c,
     count(DISTINCT d) AS donation_count,
     coalesce(sum(coalesce(d.value, d.valor, 0.0)), 0.0) AS donation_value,
     collect(DISTINCT e.election_id) AS election_ids,
     count(DISTINCT award.summary_id) AS contract_count,
     coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS amount_total,
     collect(DISTINCT award.summary_id) AS summary_ids,
     min(coalesce(award.first_date, award.last_date)) AS window_start,
     max(coalesce(award.last_date, award.first_date)) AS window_end
WITH c,
     donation_count,
     donation_value,
     contract_count,
     amount_total,
     window_start,
     window_end,
     [x IN election_ids + summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE donation_count >= 1
  AND contract_count >= 1
RETURN 'company_donor_vendor_overlap' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(donation_count + contract_count) AS risk_signal,
       amount_total AS amount_total,
       toInteger(contract_count) AS contract_count,
       toInteger(donation_count) AS donation_count,
       donation_value AS donation_value,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
