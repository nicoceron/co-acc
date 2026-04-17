SELECT
  'territory:' || coalesce(nullif(p.municipality, ''), nullif(o.municipality, ''), 'NACIONAL') AS entity_id,
  'territory:' || coalesce(nullif(p.municipality, ''), nullif(o.municipality, ''), 'NACIONAL') AS entity_key,
  'Territory' AS entity_label,
  'bpin_dnp_vs_pida27_obras_prioritarias:' || p.bpin_code AS scope_key,
  count(DISTINCT p.supplier_document_id) AS risk_signal,
  count(DISTINCT p.contract_id) AS evidence_count,
  list(DISTINCT coalesce(p.contract_id, o.priority_id)) AS evidence_refs,
  coalesce(nullif(p.municipality, ''), nullif(o.municipality, ''), 'NACIONAL') AS municipality,
  coalesce(nullif(p.department, ''), 'NACIONAL') AS department,
  p.bpin_code,
  count(DISTINCT o.priority_id) AS priority_work_count
FROM src_secop_process_bpin p
JOIN src_dnp_obras_prioritarias o
  ON p.bpin_code = o.bpin_code
LEFT JOIN src_secop_integrado si
  ON p.contract_id = si.contract_id
GROUP BY
  p.bpin_code,
  coalesce(nullif(p.municipality, ''), nullif(o.municipality, ''), 'NACIONAL'),
  coalesce(nullif(p.department, ''), 'NACIONAL')
HAVING count(DISTINCT o.priority_id) > 0;
