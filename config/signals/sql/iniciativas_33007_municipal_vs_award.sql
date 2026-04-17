SELECT
  'territory:' || coalesce(nullif(a.municipality, ''), 'NACIONAL') AS entity_id,
  'territory:' || coalesce(nullif(a.municipality, ''), 'NACIONAL') AS entity_key,
  'Territory' AS entity_label,
  'iniciativas_33007_municipal_vs_award:' || coalesce(nullif(a.municipality, ''), 'NACIONAL') AS scope_key,
  sum(coalesce(try_cast(a.contract_value AS DOUBLE), 0)) AS risk_signal,
  count(DISTINCT a.contract_id) AS evidence_count,
  list(DISTINCT coalesce(a.contract_id, i.initiative_id)) AS evidence_refs,
  coalesce(nullif(a.municipality, ''), 'NACIONAL') AS municipality,
  coalesce(nullif(a.department, ''), 'NACIONAL') AS department,
  count(DISTINCT i.initiative_id) AS initiative_count,
  count(DISTINCT a.supplier_document_id) AS supplier_count
FROM src_secop_integrado a
JOIN src_presidencia_iniciativas_33007 i
  ON a.municipality = i.municipality
GROUP BY
  coalesce(nullif(a.municipality, ''), 'NACIONAL'),
  coalesce(nullif(a.department, ''), 'NACIONAL')
HAVING count(DISTINCT i.initiative_id) > 0;
