MATCH (c:Company)
WHERE (
    elementId(c) = $company_id
    OR c.document_id = $company_identifier
    OR c.document_id = $company_identifier_formatted
    OR c.nit = $company_identifier
    OR c.nit = $company_identifier_formatted
  )
MATCH (c)-[:PROVEYO_TVEC]->(o:TVECOrder)
WITH c, coalesce(o.aggregation, 'SIN_AGREGACION') AS aggregation, o
WITH c,
     aggregation,
     collect(DISTINCT o.buyer_document_id) AS buyer_ids,
     collect(DISTINCT o.order_id) AS order_ids,
     sum(coalesce(o.total, 0.0)) AS amount_total,
     min(coalesce(o.date, o.valid_until)) AS window_start,
     max(coalesce(o.date, o.valid_until)) AS window_end
WHERE size([x IN buyer_ids WHERE x IS NOT NULL AND x <> '']) >= toInteger($pattern_srp_min_orgs)
WITH c,
     aggregation,
     [x IN buyer_ids WHERE x IS NOT NULL AND x <> ''] AS buyer_ids,
     [x IN order_ids WHERE x IS NOT NULL AND x <> ''] AS order_ids,
     amount_total,
     window_start,
     window_end
ORDER BY size(buyer_ids) DESC, amount_total DESC
LIMIT 1
RETURN 'tvec_multi_entity_capture' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(size(buyer_ids) + size(order_ids)) AS risk_signal,
       aggregation AS aggregation,
       toInteger(size(buyer_ids)) AS entity_count,
       toInteger(size(order_ids)) AS order_count,
       amount_total AS amount_total,
       window_start AS window_start,
       window_end AS window_end,
       order_ids[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(order_ids) AS evidence_count
