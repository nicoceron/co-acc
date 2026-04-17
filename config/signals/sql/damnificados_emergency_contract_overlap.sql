WITH emergency_awards AS (
  SELECT
    coalesce(nullif(municipality, ''), 'NACIONAL') AS municipality,
    coalesce(nullif(department, ''), 'NACIONAL') AS department,
    supplier_document_id,
    coalesce(nullif(contract_id, ''), nullif(process_id, '')) AS contract_id,
    coalesce(try_cast(contract_value AS DOUBLE), 0) AS contract_value
  FROM src_secop_integrado
  WHERE lower(coalesce(modality, '') || ' ' || coalesce(object, '')) LIKE '%urgencia%'
     OR lower(coalesce(modality, '') || ' ' || coalesce(object, '')) LIKE '%emergenc%'
),
affected AS (
  SELECT
    coalesce(nullif(municipality, ''), 'NACIONAL') AS municipality,
    sum(coalesce(try_cast(affected_count AS DOUBLE), 0)) AS affected_count
  FROM src_ungrd_damnificados
  GROUP BY municipality
),
joined AS (
  SELECT
    e.municipality,
    e.department,
    e.supplier_document_id,
    e.contract_id,
    e.contract_value,
    coalesce(a.affected_count, 0) AS affected_count
  FROM emergency_awards e
  JOIN src_secop_contract_additions addn
    ON e.contract_id = addn.contract_id
  LEFT JOIN affected a
    ON e.municipality = a.municipality
  LEFT JOIN src_secop_budget_commitments bc
    ON e.contract_id = bc.contract_id
)
SELECT
  'territory:' || municipality AS entity_id,
  'territory:' || municipality AS entity_key,
  'Territory' AS entity_label,
  'damnificados_emergency_contract_overlap:' || municipality AS scope_key,
  sum(contract_value) / greatest(count(DISTINCT supplier_document_id), 1) AS risk_signal,
  count(DISTINCT contract_id) AS evidence_count,
  list(DISTINCT contract_id) FILTER (WHERE contract_id IS NOT NULL) AS evidence_refs,
  municipality,
  department,
  sum(contract_value) AS emergency_award_value,
  max(affected_count) AS affected_count
FROM joined
GROUP BY municipality, department
HAVING count(DISTINCT contract_id) > 0;
