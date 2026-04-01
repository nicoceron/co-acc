MATCH (ct:Contract)
WHERE elementId(ct) = $entity_id
   OR coalesce(ct.contract_id, ct.summary_id) = $entity_key
WITH ct
WHERE ct.category IS NOT NULL
CALL {
  WITH ct
  MATCH (peer:Contract)
  WHERE peer.category = ct.category
  RETURN percentileCont(peer.value, 0.95) AS p95
}
WITH ct, p95
WHERE p95 IS NOT NULL
  AND coalesce(ct.value, 0.0) >= p95
RETURN coalesce(ct.contract_id, ct.summary_id, $entity_key) AS scope_key,
       toFloat(coalesce(ct.value, 0.0) / CASE WHEN p95 = 0 THEN 1 ELSE p95 END) AS risk_signal,
       [coalesce(ct.contract_id, ct.summary_id, $entity_key)] AS evidence_refs,
       1 AS evidence_count,
       'EXACT_CONTRACT_KEY' AS identity_match_type,
       'exact' AS identity_quality,
       'Contract:' + coalesce(ct.contract_id, ct.summary_id, $entity_key) AS node_ref
