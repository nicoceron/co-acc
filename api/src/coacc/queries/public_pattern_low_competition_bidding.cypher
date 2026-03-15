MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
   OR c.cnpj = $company_identifier
   OR c.cnpj = $company_identifier_formatted
MATCH (c)-[offer:FORNECEU_LICITACAO]->(b:Bid)
WHERE coalesce(b.offer_count, 0) <= 2
   OR coalesce(b.direct_invitation, false) = true
WITH c,
     collect(DISTINCT b.bid_id) AS bid_ids,
     count(DISTINCT b) AS bid_count,
     count(DISTINCT CASE WHEN coalesce(b.direct_invitation, false) THEN b END) AS direct_invitation_count,
     sum(coalesce(offer.offer_value_total, 0.0)) AS amount_total,
     min(coalesce(b.first_offer_date, b.last_offer_date)) AS window_start,
     max(coalesce(b.last_offer_date, b.first_offer_date)) AS window_end
WITH c,
     bid_count,
     direct_invitation_count,
     amount_total,
     window_start,
     window_end,
     [x IN bid_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE bid_count >= toInteger($pattern_min_contract_count)
   OR amount_total >= toFloat($pattern_min_contract_value)
RETURN 'low_competition_bidding' AS pattern_id,
       coalesce(c.document_id, c.nit, c.cnpj) AS company_identifier,
       coalesce(c.razao_social, c.name) AS company_name,
       toFloat(bid_count + direct_invitation_count + size(evidence_refs)) AS risk_signal,
       amount_total AS amount_total,
       toInteger(bid_count) AS bid_count,
       toInteger(direct_invitation_count) AS direct_invitation_count,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
