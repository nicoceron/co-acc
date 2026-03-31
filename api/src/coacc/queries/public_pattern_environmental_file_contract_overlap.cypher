MATCH (c:Company)
WHERE (
    elementId(c) = $company_id
    OR c.document_id = $company_identifier
    OR c.document_id = $company_identifier_formatted
    OR c.nit = $company_identifier
    OR c.nit = $company_identifier_formatted
  )
MATCH (c)-[:REGISTRO_AMBIENTAL]->(e:EnvironmentalFile)
CALL {
  WITH c
  MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_count,
         sum(coalesce(award.total_value, 0.0)) AS amount_total,
         min(coalesce(award.first_date, award.last_date)) AS window_start,
         max(coalesce(award.last_date, award.first_date)) AS window_end,
         collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_refs
}
WITH c,
     contract_count,
     amount_total,
     window_start,
     window_end,
     [x IN contract_refs WHERE x IS NOT NULL AND x <> ''] AS contract_refs,
     count(DISTINCT e) AS environmental_count,
     min(e.date) AS env_start,
     max(e.date) AS env_end,
     collect(DISTINCT coalesce(e.expediente, e.file_id)) AS environmental_refs
WHERE contract_count >= toInteger($pattern_min_contract_count)
  AND environmental_count > 0
WITH c,
     contract_count,
     amount_total,
     environmental_count,
     CASE
       WHEN env_start IS NULL THEN window_start
       WHEN window_start IS NULL OR env_start < window_start THEN env_start
       ELSE window_start
     END AS first_date,
     CASE
       WHEN env_end IS NULL THEN window_end
       WHEN window_end IS NULL OR env_end > window_end THEN env_end
       ELSE window_end
     END AS last_date,
     contract_refs + [x IN environmental_refs WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
RETURN 'environmental_file_contract_overlap' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(contract_count + environmental_count) AS risk_signal,
       toInteger(contract_count) AS contract_count,
       toInteger(environmental_count) AS environmental_count,
       amount_total AS amount_total,
       first_date AS window_start,
       last_date AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
