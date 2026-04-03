MATCH (buyer:Company)
WHERE elementId(buyer) = $entity_id
   OR coalesce(buyer.nit, buyer.document_id) = $entity_key
MATCH (buyer)-[award:CONTRATOU]->(:Company)
WITH collect(DISTINCT coalesce(award.summary_id, award.contract_id))[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
     count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS total_count,
     count(
       DISTINCT CASE
         WHEN coalesce(award.offer_count, 0) <= 1 OR coalesce(award.direct_invitation, false)
         THEN coalesce(award.summary_id, award.contract_id)
         ELSE NULL
       END
     ) AS low_competition_count
WHERE total_count >= toInteger($pattern_min_contract_count)
  AND toFloat(low_competition_count) / toFloat(total_count) >= 0.4
RETURN $entity_key AS scope_key,
       toFloat(low_competition_count) + (toFloat(low_competition_count) / toFloat(total_count)) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_COMPANY_NIT' AS identity_match_type,
       'exact' AS identity_quality,
       'Company:' + $entity_key AS node_ref
