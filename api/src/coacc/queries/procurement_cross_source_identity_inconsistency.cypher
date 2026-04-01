MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
MATCH (a:Alias)-[:ALIAS_OF]->(c)
WITH collect(DISTINCT a.alias_id)[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     count(DISTINCT a) AS alias_count
WHERE alias_count >= 5
RETURN $entity_key AS scope_key,
       toFloat(alias_count) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Company:' + $entity_key AS node_ref
