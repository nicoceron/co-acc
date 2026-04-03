MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
OPTIONAL MATCH (c)-[:SANCIONADA]->(s:Sanction)
OPTIONAL MATCH (c)-[:TIENE_HALLAZGO]->(f:Finding)
OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
WITH c,
     [x IN collect(DISTINCT coalesce(s.sanction_id, s.reference, elementId(s))) +
            collect(DISTINCT coalesce(f.finding_id, f.reference, elementId(f))) +
            collect(DISTINCT coalesce(award.summary_id, award.contract_id))
      WHERE x IS NOT NULL AND x <> ''][0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     count(DISTINCT s) + count(DISTINCT f) AS sanction_count,
     count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_count
WHERE sanction_count > 0
  AND contract_count > 0
RETURN coalesce(evidence_refs[0], $entity_key) AS scope_key,
       toFloat((sanction_count * 2) + contract_count) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Sanction:' + coalesce(evidence_refs[0], $entity_key) AS node_ref
