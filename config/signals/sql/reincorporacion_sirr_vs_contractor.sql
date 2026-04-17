WITH overlap AS (
  SELECT
    coalesce(nullif(s.municipality, ''), nullif(r.municipality, ''), 'NACIONAL') AS municipality,
    coalesce(nullif(s.department, ''), 'NACIONAL') AS department,
    s.supplier_document_id,
    coalesce(nullif(r.participant_count, ''), '1') AS participant_count
  FROM src_secop_suppliers s
  JOIN src_sirr_reincorporacion r
    ON s.supplier_document_id = r.supplier_document_id
)
SELECT
  'territory:' || municipality AS entity_id,
  'territory:' || municipality AS entity_key,
  'Territory' AS entity_label,
  'reincorporacion_sirr_vs_contractor:' || municipality AS scope_key,
  count(DISTINCT supplier_document_id) AS risk_signal,
  count(*) AS evidence_count,
  ['sirr_reincorporacion', 'secop_suppliers'] AS evidence_refs,
  municipality,
  department,
  count(DISTINCT supplier_document_id) AS matched_supplier_count,
  sum(try_cast(participant_count AS DOUBLE)) AS surfaced_participant_count
FROM overlap
GROUP BY municipality, department
HAVING count(DISTINCT supplier_document_id) > 0;
