MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
MATCH (c)-[offer:SUMINISTRO_LICITACAO]->(b:Bid)
WHERE coalesce(b.offer_count, 0) <= 1
   OR coalesce(b.direct_invitation, false) = true
WITH c,
     collect(DISTINCT coalesce(b.bid_id, elementId(b)))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     count(DISTINCT b) AS bid_count,
     sum(coalesce(offer.offer_value_total, b.total_offer_value, 0.0)) AS amount_total
WHERE bid_count >= 1
  AND amount_total >= toFloat($pattern_min_contract_value)
RETURN coalesce(evidence_refs[0], $entity_key) AS scope_key,
       toFloat(bid_count + size(evidence_refs)) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Bid:' + coalesce(evidence_refs[0], $entity_key) AS node_ref
