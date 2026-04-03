MATCH (c1:Company)
WHERE elementId(c1) = $entity_id
   OR coalesce(c1.nit, c1.document_id) = $entity_key
MATCH (c1)-[:SUMINISTRO_LICITACAO]->(b:Bid)<-[:SUMINISTRO_LICITACAO]-(c2:Company)
WHERE c2 <> c1
WITH c2,
     collect(DISTINCT coalesce(b.bid_id, elementId(b)))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     count(DISTINCT b) AS shared_bid_count
WHERE shared_bid_count >= toInteger($pattern_min_contract_count)
RETURN coalesce(c2.document_id, c2.nit, elementId(c2)) AS scope_key,
       toFloat(shared_bid_count + size(evidence_refs)) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Company:' + coalesce(c2.document_id, c2.nit, elementId(c2)) AS node_ref
