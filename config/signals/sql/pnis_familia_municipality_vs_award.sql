WITH awards AS (
  SELECT
    coalesce(nullif(municipality, ''), 'NACIONAL') AS municipality,
    coalesce(nullif(department, ''), 'NACIONAL') AS department,
    supplier_document_id,
    coalesce(nullif(contract_id, ''), nullif(process_id, '')) AS contract_id,
    coalesce(try_cast(contract_value AS DOUBLE), 0) AS contract_value
  FROM src_secop_integrado
),
pnis AS (
  SELECT
    coalesce(nullif(municipality, ''), 'NACIONAL') AS municipality,
    sum(coalesce(try_cast(family_count AS DOUBLE), 0)) AS family_count
  FROM src_pnis_beneficiarios
  GROUP BY municipality
)
SELECT
  'territory:' || a.municipality AS entity_id,
  'territory:' || a.municipality AS entity_key,
  'Territory' AS entity_label,
  'pnis_familia_municipality_vs_award:' || a.municipality AS scope_key,
  sum(a.contract_value) / greatest(count(DISTINCT a.supplier_document_id), 1) AS risk_signal,
  count(DISTINCT a.contract_id) AS evidence_count,
  list(DISTINCT a.contract_id) FILTER (WHERE a.contract_id IS NOT NULL) AS evidence_refs,
  a.municipality,
  a.department,
  max(p.family_count) AS pnis_family_count,
  count(DISTINCT a.supplier_document_id) AS supplier_count
FROM awards a
JOIN pnis p ON a.municipality = p.municipality
JOIN src_pdet_municipios pdet ON a.municipality = pdet.municipality
GROUP BY a.municipality, a.department
HAVING sum(a.contract_value) > 0;
