MATCH (p:Person)
WHERE elementId(p) = $entity_id
   OR coalesce(p.document_key, p.document_id, p.cedula) = $entity_key
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:OFFICER_OF]->(supplier:Company)
  OPTIONAL MATCH (supplier)<-[award:CONTRATOU]-(:Company)
  RETURN count(DISTINCT supplier) AS linked_company_count,
         count(DISTINCT coalesce(award.summary_id, award.contract_id)) AS supplier_contract_count,
         collect(DISTINCT coalesce(award.summary_id, award.contract_id)) AS summary_ids
}
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:RECIBIO_SALARIO]->(o:PublicOffice)
  RETURN count(DISTINCT o) AS office_count
}
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DECLARO_FINANZAS]->(f:Finance {type: 'CONFLICT_DISCLOSURE'})
  RETURN count(DISTINCT f) AS disclosure_count,
         collect(DISTINCT f.finance_id) AS finance_ids
}
WITH p,
     linked_company_count,
     supplier_contract_count,
     office_count,
     disclosure_count,
     [x IN finance_ids + summary_ids WHERE x IS NOT NULL AND x <> ''][0..toInteger($pattern_max_evidence_refs)] AS evidence_refs
WHERE supplier_contract_count > 0
  AND disclosure_count > 0
RETURN coalesce(evidence_refs[0], $entity_key) AS scope_key,
       toFloat(linked_company_count + supplier_contract_count + office_count + disclosure_count) AS risk_signal,
       evidence_refs,
       size(evidence_refs) AS evidence_count,
       'EXACT_PERSON_DOCUMENT' AS identity_match_type,
       'exact' AS identity_quality,
       'Person:' + coalesce(p.document_id, p.cedula, $entity_key) AS node_ref
