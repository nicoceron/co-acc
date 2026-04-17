SELECT
  'territory:' || coalesce(nullif(s.municipality, ''), nullif(m.municipality, ''), 'NACIONAL') AS entity_id,
  'territory:' || coalesce(nullif(s.municipality, ''), nullif(m.municipality, ''), 'NACIONAL') AS entity_key,
  'Territory' AS entity_label,
  'registro_actores_deporte_vs_funcionario_pdet:' || coalesce(nullif(s.municipality, ''), nullif(m.municipality, ''), 'NACIONAL') AS scope_key,
  count(DISTINCT s.person_document_id) AS risk_signal,
  count(DISTINCT m.actor_id) AS evidence_count,
  list(DISTINCT m.actor_id) FILTER (WHERE m.actor_id IS NOT NULL) AS evidence_refs,
  coalesce(nullif(s.municipality, ''), nullif(m.municipality, ''), 'NACIONAL') AS municipality,
  coalesce(nullif(s.department, ''), 'NACIONAL') AS department,
  count(DISTINCT s.person_document_id) AS matched_actor_count
FROM src_sigep_public_servants s
JOIN src_mindeporte_actores m
  ON s.person_document_id = m.person_document_id
 AND s.municipality = m.municipality
GROUP BY
  coalesce(nullif(s.municipality, ''), nullif(m.municipality, ''), 'NACIONAL'),
  coalesce(nullif(s.department, ''), 'NACIONAL')
HAVING count(DISTINCT s.person_document_id) > 0;
