WITH categorized AS (
  SELECT
    coalesce(nullif(municipality, ''), 'NACIONAL') AS municipality,
    coalesce(nullif(department, ''), 'NACIONAL') AS department,
    nullif(pida_category, '') AS pida_category,
    coalesce(nullif(contract_id, ''), nullif(process_id, '')) AS evidence_ref
  FROM src_secop_integrado
)
SELECT
  'territory:' || municipality AS entity_id,
  'territory:' || municipality AS entity_key,
  'Territory' AS entity_label,
  'pida_full30_meta:' || municipality AS scope_key,
  count(DISTINCT pida_category) AS risk_signal,
  count(*) AS evidence_count,
  list(DISTINCT evidence_ref) FILTER (WHERE evidence_ref IS NOT NULL) AS evidence_refs,
  municipality,
  department,
  count(DISTINCT pida_category) AS pida_category_count
FROM categorized
WHERE pida_category IS NOT NULL
GROUP BY municipality, department
HAVING count(DISTINCT pida_category) >= 3;
