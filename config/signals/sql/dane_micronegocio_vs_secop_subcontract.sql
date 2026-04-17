SELECT
  'territory:' || coalesce(nullif(e.municipality, ''), nullif(m.municipality, ''), 'NACIONAL') AS entity_id,
  'territory:' || coalesce(nullif(e.municipality, ''), nullif(m.municipality, ''), 'NACIONAL') AS entity_key,
  'Territory' AS entity_label,
  'dane_micronegocio_vs_secop_subcontract:' || coalesce(nullif(e.municipality, ''), nullif(m.municipality, ''), 'NACIONAL') AS scope_key,
  sum(coalesce(try_cast(e.award_value AS DOUBLE), 0)) AS risk_signal,
  count(DISTINCT e.contract_id) AS evidence_count,
  list(DISTINCT e.contract_id) FILTER (WHERE e.contract_id IS NOT NULL) AS evidence_refs,
  coalesce(nullif(e.municipality, ''), nullif(m.municipality, ''), 'NACIONAL') AS municipality,
  coalesce(nullif(e.department, ''), 'NACIONAL') AS department,
  count(DISTINCT e.subcontractor_document_id) AS microbusiness_subcontractor_count
FROM src_secop_contract_execution e
JOIN src_dane_micronegocios m
  ON e.subcontractor_document_id = m.supplier_document_id
GROUP BY
  coalesce(nullif(e.municipality, ''), nullif(m.municipality, ''), 'NACIONAL'),
  coalesce(nullif(e.department, ''), 'NACIONAL')
HAVING sum(coalesce(try_cast(e.award_value AS DOUBLE), 0)) > 1000000;
