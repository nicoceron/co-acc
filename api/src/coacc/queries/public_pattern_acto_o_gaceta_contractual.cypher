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
  MATCH (a:ActoAdministrativo)
  WHERE (
      company_identifier <> ''
      AND toUpper(coalesce(a.search_text, '')) CONTAINS company_identifier
    ) OR (
      size(company_name) >= 12
      AND company_name CONTAINS ' '
      AND toUpper(coalesce(a.search_text, '')) CONTAINS company_name
    )
  RETURN count(DISTINCT a) AS act_count,
         min(a.publication_date) AS act_start,
         max(a.publication_date) AS act_end,
         collect(DISTINCT coalesce(a.number, a.act_id)) AS act_refs
}
CALL {
  WITH company_name, company_identifier
  MATCH (g:GacetaTerritorial)
  WHERE (
      company_identifier <> ''
      AND toUpper(coalesce(g.search_text, '')) CONTAINS company_identifier
    ) OR (
      size(company_name) >= 12
      AND company_name CONTAINS ' '
      AND toUpper(coalesce(g.search_text, '')) CONTAINS company_name
    )
  RETURN count(DISTINCT g) AS gazette_count,
         min(g.publication_date) AS gazette_start,
         max(g.publication_date) AS gazette_end,
         collect(DISTINCT coalesce(g.number, g.gaceta_id)) AS gazette_refs
}
WITH c,
     contract_count,
     amount_total,
     [x IN contract_refs WHERE x IS NOT NULL AND x <> ''] AS contract_refs,
     act_count,
     gazette_count,
     act_start,
     gazette_start,
     act_end,
     gazette_end,
     [x IN act_refs WHERE x IS NOT NULL AND x <> ''] AS act_refs,
     [x IN gazette_refs WHERE x IS NOT NULL AND x <> ''] AS gazette_refs,
     window_start,
     window_end
WITH c,
     contract_count,
     act_count + gazette_count AS document_count,
     amount_total,
     CASE
       WHEN act_start IS NULL THEN coalesce(gazette_start, window_start)
       WHEN gazette_start IS NULL OR act_start < gazette_start THEN
         CASE WHEN window_start IS NULL OR act_start < window_start THEN act_start ELSE window_start END
       ELSE
         CASE WHEN window_start IS NULL OR gazette_start < window_start THEN gazette_start ELSE window_start END
     END AS first_date,
     CASE
       WHEN act_end IS NULL THEN coalesce(gazette_end, window_end)
       WHEN gazette_end IS NULL OR act_end > gazette_end THEN
         CASE WHEN window_end IS NULL OR act_end > window_end THEN act_end ELSE window_end END
       ELSE
         CASE WHEN window_end IS NULL OR gazette_end > window_end THEN gazette_end ELSE window_end END
     END AS last_date,
     contract_refs + act_refs + gazette_refs AS evidence_refs
WHERE contract_count >= toInteger($pattern_min_contract_count)
  AND document_count > 0
RETURN 'acto_o_gaceta_contractual' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(contract_count + document_count) AS risk_signal,
       toInteger(contract_count) AS contract_count,
       toInteger(document_count) AS document_count,
       amount_total AS amount_total,
       first_date AS window_start,
       last_date AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
