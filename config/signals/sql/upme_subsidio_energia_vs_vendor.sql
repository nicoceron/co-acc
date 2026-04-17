SELECT
  'territory:' || coalesce(nullif(s.municipality, ''), nullif(u.municipality, ''), 'NACIONAL') AS entity_id,
  'territory:' || coalesce(nullif(s.municipality, ''), nullif(u.municipality, ''), 'NACIONAL') AS entity_key,
  'Territory' AS entity_label,
  'upme_subsidio_energia_vs_vendor:' || coalesce(nullif(s.municipality, ''), nullif(u.municipality, ''), 'NACIONAL') AS scope_key,
  count(DISTINCT s.supplier_document_id) AS risk_signal,
  count(DISTINCT u.subsidy_id) AS evidence_count,
  list(DISTINCT u.subsidy_id) FILTER (WHERE u.subsidy_id IS NOT NULL) AS evidence_refs,
  coalesce(nullif(s.municipality, ''), nullif(u.municipality, ''), 'NACIONAL') AS municipality,
  coalesce(nullif(s.department, ''), 'NACIONAL') AS department,
  count(DISTINCT s.supplier_document_id) AS matched_supplier_count
FROM src_secop_suppliers s
JOIN src_upme_subsidios u ON s.supplier_document_id = u.supplier_document_id
GROUP BY
  coalesce(nullif(s.municipality, ''), nullif(u.municipality, ''), 'NACIONAL'),
  coalesce(nullif(s.department, ''), 'NACIONAL')
HAVING count(DISTINCT u.subsidy_id) > 0;
