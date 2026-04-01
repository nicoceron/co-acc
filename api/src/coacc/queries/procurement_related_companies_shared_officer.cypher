MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
MATCH (officer:Person)-[:OFFICER_OF]->(c)
MATCH (officer)-[:OFFICER_OF]->(peer:Company)
WHERE peer <> c
OPTIONAL MATCH (buyer:Company)-[award:CONTRATOU]->(peer)
WITH c,
     officer,
     collect(DISTINCT coalesce(peer.document_id, peer.nit, elementId(peer)))[0..10] AS peer_ids,
     collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     count(DISTINCT peer) AS peer_count,
     count(DISTINCT buyer) AS buyer_overlap_count
WHERE peer_count > 0
RETURN coalesce(officer.document_id, officer.cedula, elementId(officer)) AS scope_key,
       toFloat(peer_count + buyer_overlap_count + size(evidence_refs)) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Person:' + coalesce(officer.document_id, officer.cedula, elementId(officer)) AS node_ref
