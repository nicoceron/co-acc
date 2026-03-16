MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
   OR c.cnpj = $company_identifier
   OR c.cnpj = $company_identifier_formatted
CALL {
  WITH c
  MATCH (c)-[:SANCIONADA]->(s:Sanction)
  WHERE s.date_start IS NOT NULL
    AND trim(s.date_start) <> ''
  RETURN collect(DISTINCT {
    sanction_id: s.sanction_id,
    date_start: s.date_start,
    date_end: s.date_end
  }) AS sanctions
}
WITH c, sanctions
WHERE size(sanctions) > 0
MATCH (:Company)-[award:CONTRATOU]->(c)
WHERE coalesce(award.last_date, award.first_date) IS NOT NULL
  AND any(s IN sanctions WHERE
    coalesce(award.last_date, award.first_date) >= s.date_start
    AND (
      s.date_end IS NULL
      OR trim(coalesce(s.date_end, '')) = ''
      OR coalesce(award.last_date, award.first_date) <= s.date_end
    )
  )
WITH c,
     [s IN sanctions WHERE s.sanction_id IS NOT NULL AND s.sanction_id <> '' | s.sanction_id] AS sanction_ids,
     reduce(
       refs = [],
       ref_list IN collect(coalesce(award.evidence_refs, [award.summary_id])) |
         refs + ref_list
     ) AS contract_ids,
     sum(coalesce(award.total_value, 0.0)) AS amount_total,
     min(coalesce(award.first_date, award.last_date)) AS window_start,
     max(coalesce(award.last_date, award.first_date)) AS window_end
WITH c,
     sanction_ids,
     [x IN contract_ids WHERE x IS NOT NULL AND x <> ''] AS contract_ids,
     amount_total,
     window_start,
     window_end,
     [x IN sanction_ids + contract_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE size(sanction_ids) > 0
  AND size(contract_ids) > 0
  AND size(evidence_refs) > 0
RETURN 'sanctioned_still_receiving' AS pattern_id,
       coalesce(c.document_id, c.nit, c.cnpj) AS company_identifier,
       c.razon_social AS company_name,
       toFloat(size(sanction_ids) + size(contract_ids)) AS risk_signal,
       amount_total AS amount_total,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
