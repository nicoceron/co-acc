MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
MATCH (buyer:Company)-[award:CONTRATOU]->(c)
WHERE award.average_value IS NOT NULL
  AND award.average_value >= toFloat($pattern_split_min_average_value)
  AND award.average_value <= toFloat($pattern_split_threshold_value)
  AND coalesce(award.contract_count, 0) >= toInteger($pattern_split_min_count)
  AND coalesce(award.total_value, 0.0) >= toFloat($pattern_split_min_total_value)
WITH c,
     coalesce(award.buyer_name, buyer.razon_social, buyer.name) AS contracting_org,
     collect(coalesce(award.evidence_refs, [award.summary_id])) AS id_groups,
     sum(coalesce(award.total_value, 0.0)) AS amount_total,
     min(coalesce(award.first_date, award.last_date)) AS window_start,
     max(coalesce(award.last_date, award.first_date)) AS window_end,
     count(*) AS grouped_occurrences
WITH c,
     amount_total,
     window_start,
     window_end,
     grouped_occurrences,
     reduce(flat = [], ids IN id_groups | flat + ids) AS evidence_refs
WHERE size(evidence_refs) > 0
RETURN 'split_contracts_below_threshold' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       c.razon_social AS company_name,
       toFloat(grouped_occurrences + size(evidence_refs)) AS risk_signal,
       amount_total AS amount_total,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
