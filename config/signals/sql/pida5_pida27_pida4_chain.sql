WITH sanctioned_awards AS (
  SELECT
    coalesce(nullif(a.municipality, ''), nullif(o.municipality, ''), 'NACIONAL') AS municipality,
    coalesce(nullif(a.department, ''), 'NACIONAL') AS department,
    a.supplier_document_id,
    a.contract_id,
    a.bpin_code,
    s.sanction_id,
    coalesce(try_cast(a.contract_value AS DOUBLE), 0) AS contract_value
  FROM src_secop_integrado a
  JOIN src_secop_sanctions s
    ON a.supplier_document_id = s.supplier_document_id
  LEFT JOIN src_dnp_obras_prioritarias o
    ON a.bpin_code = o.bpin_code
)
SELECT
  'territory:' || municipality AS entity_id,
  'territory:' || municipality AS entity_key,
  'Territory' AS entity_label,
  'pida5_pida27_pida4_chain:' || municipality AS scope_key,
  sum(contract_value) AS risk_signal,
  count(DISTINCT contract_id) AS evidence_count,
  list(DISTINCT coalesce(contract_id, sanction_id)) AS evidence_refs,
  municipality,
  department,
  count(DISTINCT sanction_id) AS sanction_count,
  count(DISTINCT bpin_code) AS priority_bpin_count
FROM sanctioned_awards
GROUP BY municipality, department
HAVING count(DISTINCT sanction_id) > 0;
