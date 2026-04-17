WITH chain AS (
  SELECT
    coalesce(nullif(a.municipality, ''), nullif(t.municipality, ''), nullif(l.municipality, ''), 'NACIONAL') AS municipality,
    coalesce(nullif(a.department, ''), 'NACIONAL') AS department,
    a.supplier_document_id,
    a.contract_id,
    t.mining_title_id,
    l.license_id,
    coalesce(try_cast(a.contract_value AS DOUBLE), 0) AS contract_value
  FROM src_secop_integrado a
  JOIN src_anm_titulos t ON a.supplier_document_id = t.supplier_document_id
  JOIN src_anla_licencias l ON a.supplier_document_id = l.supplier_document_id
)
SELECT
  'territory:' || municipality AS entity_id,
  'territory:' || municipality AS entity_key,
  'Territory' AS entity_label,
  'mineria_titulo_vs_anla_license_vs_contractor:' || municipality AS scope_key,
  sum(contract_value) AS risk_signal,
  count(DISTINCT contract_id) AS evidence_count,
  list(DISTINCT contract_id) FILTER (WHERE contract_id IS NOT NULL) AS evidence_refs,
  municipality,
  department,
  count(DISTINCT mining_title_id) AS mining_title_count,
  count(DISTINCT license_id) AS license_count
FROM chain
GROUP BY municipality, department
HAVING count(DISTINCT supplier_document_id) > 0;
