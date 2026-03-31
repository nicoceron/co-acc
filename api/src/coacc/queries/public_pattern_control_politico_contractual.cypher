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
  MATCH (r:InquiryRequirement)
  WHERE (
      company_identifier <> ''
      AND toUpper(coalesce(r.search_text, '')) CONTAINS company_identifier
    ) OR (
      size(company_name) >= 12
      AND company_name CONTAINS ' '
      AND toUpper(coalesce(r.search_text, '')) CONTAINS company_name
    )
  RETURN count(DISTINCT r) AS requirement_count,
         min(r.approval_date) AS requirement_start,
         max(r.approval_date) AS requirement_end,
         collect(DISTINCT coalesce(r.code, r.requirement_id)) AS requirement_refs
}
CALL {
  WITH company_name, company_identifier
  MATCH (s:InquirySession)
  WHERE (
      company_identifier <> ''
      AND toUpper(coalesce(s.search_text, '')) CONTAINS company_identifier
    ) OR (
      size(company_name) >= 12
      AND company_name CONTAINS ' '
      AND toUpper(coalesce(s.search_text, '')) CONTAINS company_name
    )
  RETURN count(DISTINCT s) AS session_count,
         min(s.date) AS session_start,
         max(s.date) AS session_end,
         collect(DISTINCT coalesce(s.session_no, s.session_id)) AS session_refs
}
WITH c,
     contract_count,
     amount_total,
     [x IN contract_refs WHERE x IS NOT NULL AND x <> ''] AS contract_refs,
     requirement_count,
     session_count,
     requirement_start,
     requirement_end,
     session_start,
     session_end,
     [x IN requirement_refs WHERE x IS NOT NULL AND x <> ''] AS requirement_refs,
     [x IN session_refs WHERE x IS NOT NULL AND x <> ''] AS session_refs,
     window_start,
     window_end
WITH c,
     contract_count,
     requirement_count + session_count AS oversight_count,
     amount_total,
     CASE
       WHEN requirement_start IS NULL THEN coalesce(session_start, window_start)
       WHEN session_start IS NULL OR requirement_start < session_start THEN
         CASE WHEN window_start IS NULL OR requirement_start < window_start THEN requirement_start ELSE window_start END
       ELSE
         CASE WHEN window_start IS NULL OR session_start < window_start THEN session_start ELSE window_start END
     END AS first_date,
     CASE
       WHEN requirement_end IS NULL THEN coalesce(session_end, window_end)
       WHEN session_end IS NULL OR requirement_end > session_end THEN
         CASE WHEN window_end IS NULL OR requirement_end > window_end THEN requirement_end ELSE window_end END
       ELSE
         CASE WHEN window_end IS NULL OR session_end > window_end THEN session_end ELSE window_end END
     END AS last_date,
     contract_refs + requirement_refs + session_refs AS evidence_refs
WHERE contract_count >= toInteger($pattern_min_contract_count)
  AND oversight_count > 0
RETURN 'control_politico_contractual' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(contract_count + oversight_count) AS risk_signal,
       toInteger(contract_count) AS contract_count,
       toInteger(oversight_count) AS oversight_count,
       amount_total AS amount_total,
       first_date AS window_start,
       last_date AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
