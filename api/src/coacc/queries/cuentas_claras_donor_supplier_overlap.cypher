MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
MATCH (c)-[:DONO_A]->(e:Election)
OPTIONAL MATCH (:Company)-[award:CONTRATOU]->(c)
WITH collect(DISTINCT e.election_id) AS election_refs,
     collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS award_refs,
     count(DISTINCT e) AS donation_count,
     count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS contract_count
WHERE donation_count > 0
  AND contract_count > 0
RETURN $entity_key AS scope_key,
       toFloat(donation_count + contract_count) AS risk_signal,
       [x IN election_refs + award_refs WHERE x IS NOT NULL AND x <> ''][0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size([x IN election_refs + award_refs WHERE x IS NOT NULL AND x <> '']) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Election:' + coalesce(election_refs[0], $entity_key) AS node_ref
