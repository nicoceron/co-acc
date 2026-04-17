WITH awards AS (
  SELECT
    coalesce(nullif(municipality, ''), 'NACIONAL') AS municipality,
    coalesce(nullif(department, ''), 'NACIONAL') AS department,
    coalesce(nullif(contract_id, ''), nullif(process_id, '')) AS contract_id,
    coalesce(try_cast(contract_value AS DOUBLE), 0) AS contract_value
  FROM src_secop_integrado
),
territory_awards AS (
  SELECT municipality, department, sum(contract_value) AS total_award_value, count(*) AS contract_count
  FROM awards
  GROUP BY municipality, department
)
SELECT
  'territory:' || t.municipality AS entity_id,
  'territory:' || t.municipality AS entity_key,
  'Territory' AS entity_label,
  'dane_pobreza_vs_award_ratio:' || t.municipality AS scope_key,
  t.total_award_value / greatest(try_cast(p.population AS DOUBLE), 1) AS risk_signal,
  t.contract_count AS evidence_count,
  ['secop_integrado', 'dane_pobreza_monetaria', 'dane_ipm'] AS evidence_refs,
  t.municipality,
  t.department,
  t.total_award_value,
  try_cast(p.poverty_rate AS DOUBLE) AS poverty_rate,
  try_cast(ipm.ipm AS DOUBLE) AS ipm
FROM territory_awards t
JOIN src_dane_pobreza_monetaria p ON t.municipality = p.municipality
LEFT JOIN src_dane_ipm ipm ON t.municipality = ipm.municipality
WHERE coalesce(try_cast(ipm.ipm AS DOUBLE), 0) >= 40
   OR coalesce(try_cast(p.poverty_rate AS DOUBLE), 0) >= 0.4;
