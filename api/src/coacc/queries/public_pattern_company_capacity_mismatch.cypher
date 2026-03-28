MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
MATCH (c)-[:DECLARO_FINANZAS]->(f:Finance {type: 'SUPERSOC_TOP_COMPANY'})
CALL {
  WITH c
  MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT award.summary_id) AS contract_count,
         sum(coalesce(award.total_value, 0.0)) AS amount_total,
         collect(DISTINCT award.summary_id) AS summary_ids,
         min(coalesce(award.first_date, award.last_date)) AS window_start,
         max(coalesce(award.last_date, award.first_date)) AS window_end
}
WITH c,
     f,
     contract_count,
     amount_total,
     window_start,
     window_end,
     CASE
       WHEN coalesce(f.operating_revenue_current, 0.0) > 0.0
       THEN amount_total / toFloat(f.operating_revenue_current)
       ELSE 0.0
     END AS revenue_ratio,
     CASE
       WHEN coalesce(f.total_assets_current, 0.0) > 0.0
       THEN amount_total / toFloat(f.total_assets_current)
       ELSE 0.0
     END AS asset_ratio,
     coalesce(f.operating_revenue_current, 0.0) AS reported_revenue,
     coalesce(f.total_assets_current, 0.0) AS reported_assets,
     [x IN summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE amount_total >= toFloat($pattern_min_contract_value)
  AND (
    revenue_ratio >= 2.0
    OR asset_ratio >= 1.0
  )
RETURN 'company_capacity_mismatch' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(
         contract_count
         + CASE
             WHEN revenue_ratio >= 10.0 THEN 4
             WHEN revenue_ratio >= 5.0 THEN 3
             WHEN revenue_ratio >= 2.0 THEN 2
             ELSE 0
           END
         + CASE
             WHEN asset_ratio >= 5.0 THEN 4
             WHEN asset_ratio >= 2.0 THEN 3
             WHEN asset_ratio >= 1.0 THEN 2
             ELSE 0
           END
       ) AS risk_signal,
       toInteger(contract_count) AS contract_count,
       amount_total AS amount_total,
       reported_revenue AS reported_revenue,
       reported_assets AS reported_assets,
       toFloat(revenue_ratio) AS revenue_ratio,
       toFloat(asset_ratio) AS asset_ratio,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
