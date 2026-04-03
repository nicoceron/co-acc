MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
MATCH (c)-[:PROVEYO_TVEC]->(o:TVECOrder)
WITH c,
     collect(DISTINCT o.order_id)[0..toInteger($pattern_max_evidence_refs)] AS order_refs,
     collect(DISTINCT o.buyer_document_id) AS buyer_ids,
     count(DISTINCT o) AS order_count,
     sum(coalesce(o.total, 0.0)) AS amount_total
WHERE size([x IN buyer_ids WHERE x IS NOT NULL AND x <> '']) >= toInteger($pattern_srp_min_orgs)
RETURN coalesce(order_refs[0], $entity_key) AS scope_key,
       toFloat(order_count + size(buyer_ids)) + (amount_total / 1000000000.0) AS risk_signal,
       order_refs AS evidence_refs,
       size(order_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'TVECOrder:' + coalesce(order_refs[0], $entity_key) AS node_ref
