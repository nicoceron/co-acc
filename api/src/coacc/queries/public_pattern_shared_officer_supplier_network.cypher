MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
CALL {
  WITH c
  MATCH (officer:Person)-[:OFFICER_OF]->(c)
  MATCH (officer)-[:OFFICER_OF]->(peer:Company)
  WHERE peer <> c
  CALL {
    WITH c, peer
    OPTIONAL MATCH (buyer:Company)-[company_award:CONTRATOU]->(c)
    OPTIONAL MATCH (buyer)-[peer_award:CONTRATOU]->(peer)
    RETURN count(DISTINCT buyer) AS shared_buyer_count,
           collect(DISTINCT company_award.summary_id) AS company_summary_ids,
           collect(DISTINCT peer_award.summary_id) AS peer_summary_ids
  }
  RETURN count(DISTINCT officer) AS shared_officer_count,
         count(DISTINCT peer) AS peer_supplier_count,
         sum(shared_buyer_count) AS shared_buyer_count,
         collect(DISTINCT coalesce(peer.razon_social, peer.name, peer.document_id))[0..5] AS peer_company_names,
         reduce(
           refs = [],
           ref_list IN collect(company_summary_ids + peer_summary_ids) |
             refs + ref_list
         ) AS summary_ids
}
WITH c,
     shared_officer_count,
     peer_supplier_count,
     shared_buyer_count,
     peer_company_names,
     [x IN summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE peer_supplier_count >= 1
  AND (
    shared_buyer_count > 0
    OR size(evidence_refs) >= toInteger($pattern_min_contract_count)
  )
RETURN 'shared_officer_supplier_network' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(shared_officer_count + peer_supplier_count + shared_buyer_count) AS risk_signal,
       toInteger(shared_officer_count) AS shared_officer_count,
       toInteger(peer_supplier_count) AS peer_supplier_count,
       toInteger(shared_buyer_count) AS shared_buyer_count,
       peer_company_names AS peer_company_names,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
