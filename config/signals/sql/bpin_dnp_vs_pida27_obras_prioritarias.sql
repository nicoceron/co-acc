WITH bpin_links AS (
  SELECT DISTINCT
    NULLIF(TRIM(codigo_bpin), '') AS bpin_code,
    NULLIF(TRIM(anno_bpin), '') AS bpin_year,
    NULLIF(TRIM(id_contracto), '') AS contract_id
  FROM src_secop_process_bpin
  WHERE NULLIF(TRIM(codigo_bpin), '') IS NOT NULL
    AND NULLIF(TRIM(id_contracto), '') IS NOT NULL
),
integrated_contracts AS (
  SELECT
    NULLIF(TRIM(contract_number), '') AS contract_id,
    COALESCE(NULLIF(TRIM(entity_municipality), ''), 'NACIONAL') AS municipality,
    COALESCE(NULLIF(TRIM(entity_department), ''), 'NACIONAL') AS department,
    COALESCE(
      TRY_CAST(
        REGEXP_REPLACE(COALESCE(CAST(contract_value AS VARCHAR), ''), '[^0-9]', '', 'g')
        AS DOUBLE
      ),
      0
    ) AS contract_value,
    COALESCE(NULLIF(TRIM(contract_url), ''), 'secop_integrado:' || contract_number)
      AS evidence_ref,
    LOWER(
      COALESCE(contract_object, '') || ' ' ||
      COALESCE(process_object, '') || ' ' ||
      COALESCE(contract_type, '') || ' ' ||
      COALESCE(procurement_modality, '')
    ) AS text_blob
  FROM src_secop_integrado
  WHERE NULLIF(TRIM(contract_number), '') IS NOT NULL
),
priority_contracts AS (
  SELECT
    b.bpin_code,
    b.bpin_year,
    c.contract_id,
    c.municipality,
    c.department,
    c.contract_value,
    c.evidence_ref
  FROM bpin_links b
  JOIN integrated_contracts c ON c.contract_id = b.contract_id
  WHERE REGEXP_MATCHES(
    c.text_blob,
    '(obra|infraestructura|construcci[oó]n|mejoramiento|vial|acueducto|hospital)'
  )
),
rollup AS (
  SELECT
    bpin_code,
    MIN(bpin_year) AS bpin_year,
    ANY_VALUE(municipality) AS municipality,
    ANY_VALUE(department) AS department,
    COUNT(DISTINCT contract_id) AS priority_contract_count,
    SUM(contract_value) AS priority_contract_value,
    LIST(DISTINCT evidence_ref ORDER BY evidence_ref) AS evidence_refs
  FROM priority_contracts
  GROUP BY bpin_code
)
SELECT
  'project:' || bpin_code AS entity_id,
  bpin_code AS entity_key,
  'Project' AS entity_label,
  'bpin_priority_work:' || bpin_code AS scope_key,
  LEAST(1.0, priority_contract_value / 25000000000.0) AS risk_signal,
  priority_contract_count AS evidence_count,
  evidence_refs,
  municipality,
  department,
  bpin_code,
  bpin_year,
  priority_contract_count,
  priority_contract_value
FROM rollup
WHERE priority_contract_count > 0;
