MATCH (c:Company)
WHERE elementId(c) = $entity_id
   OR coalesce(c.nit, c.document_id) = $entity_key
MATCH (c)-[:SUMINISTRO_LICITACAO]->(b:Bid)
WHERE b.first_offer_date IS NOT NULL
  AND b.last_offer_date IS NOT NULL
WITH collect(DISTINCT coalesce(b.bid_id, elementId(b)))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     count(
       DISTINCT CASE
         WHEN duration.inDays(date(b.first_offer_date), date(b.last_offer_date)).days <= 3
         THEN coalesce(b.bid_id, elementId(b))
         ELSE NULL
       END
     ) AS short_window_count
WHERE short_window_count > 0
RETURN coalesce(evidence_refs[0], $entity_key) AS scope_key,
       toFloat(short_window_count + size(evidence_refs)) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Bid:' + coalesce(evidence_refs[0], $entity_key) AS node_ref
