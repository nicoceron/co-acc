MATCH (p:Person)
WHERE elementId(p) = $entity_id
   OR coalesce(p.document_key, p.document_id, p.cedula) = $entity_key
MATCH (p)-[:OFFICER_OF]->(c:Company)
MATCH (c)-[:ADMINISTRA]->(f:Finance {type: 'IGAC_PROPERTY_ACTIVITY'})
WITH collect(DISTINCT f.finance_id)[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     count(DISTINCT f) AS property_count
WHERE property_count > 0
RETURN coalesce(evidence_refs[0], $entity_key) AS scope_key,
       toFloat(property_count + size(evidence_refs)) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_PERSON_DOCUMENT' AS identity_match_type,
       'exact' AS identity_quality,
       'Finance:' + coalesce(evidence_refs[0], $entity_key) AS node_ref
