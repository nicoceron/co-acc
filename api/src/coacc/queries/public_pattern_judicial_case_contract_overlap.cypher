MATCH (c:Company)
WHERE (
    elementId(c) = $company_id
    OR c.document_id = $company_identifier
    OR c.document_id = $company_identifier_formatted
    OR c.nit = $company_identifier
    OR c.nit = $company_identifier_formatted
  )
WITH c,
     toUpper(trim(coalesce(c.razon_social, c.name, ''))) AS company_name,
     coalesce(c.document_id, c.nit, '') AS company_identifier
CALL {
  WITH c
  MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_count,
         sum(coalesce(award.total_value, 0.0)) AS amount_total,
         min(coalesce(award.first_date, award.last_date)) AS window_start,
         max(coalesce(award.last_date, award.first_date)) AS window_end,
         collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_refs
}
CALL {
  WITH company_name, company_identifier
  MATCH (j:JudicialCase)
  WHERE (
      company_identifier <> ''
      AND toUpper(coalesce(j.search_text, '')) CONTAINS company_identifier
    ) OR (
      size(company_name) >= 12
      AND company_name CONTAINS ' '
      AND toUpper(coalesce(j.search_text, '')) CONTAINS company_name
    )
  RETURN count(DISTINCT j) AS case_count,
         min(j.publication_date) AS case_start,
         max(j.publication_date) AS case_end,
         collect(DISTINCT coalesce(j.radicado, j.case_id)) AS case_refs
}
WITH c,
     contract_count,
     amount_total,
     [x IN contract_refs WHERE x IS NOT NULL AND x <> ''] AS contract_refs,
     case_count,
     case_start,
     case_end,
     [x IN case_refs WHERE x IS NOT NULL AND x <> ''] AS case_refs,
     window_start,
     window_end
WITH c,
     contract_count,
     case_count,
     amount_total,
     CASE
       WHEN case_start IS NULL THEN window_start
       WHEN window_start IS NULL OR case_start < window_start THEN case_start
       ELSE window_start
     END AS first_date,
     CASE
       WHEN case_end IS NULL THEN window_end
       WHEN window_end IS NULL OR case_end > window_end THEN case_end
       ELSE window_end
     END AS last_date,
     contract_refs + case_refs AS evidence_refs
WHERE contract_count >= toInteger($pattern_min_contract_count)
  AND case_count > 0
RETURN 'judicial_case_contract_overlap' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(contract_count + case_count) AS risk_signal,
       toInteger(contract_count) AS contract_count,
       toInteger(case_count) AS case_count,
       amount_total AS amount_total,
       first_date AS window_start,
       last_date AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
