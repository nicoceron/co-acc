MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
   OR c.cnpj = $company_identifier
   OR c.cnpj = $company_identifier_formatted
MATCH (c)-[:SANCIONADA]->(s:Sanction)
OPTIONAL MATCH ()-[award:CONTRATOU]->(c)
WITH c,
     count(DISTINCT s) AS sanction_count,
     count(DISTINCT award.summary_id) AS contract_count,
     coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS amount_total,
     min(s.date_start) AS window_start,
     max(coalesce(s.date_end, s.date_start)) AS window_end,
     collect(
       DISTINCT coalesce(
         s.sanction_id,
         s.reference,
         s.name,
         elementId(s)
       )
     ) AS sanction_refs
WITH c,
     sanction_count,
     contract_count,
     amount_total,
     window_start,
     window_end,
     [x IN sanction_refs WHERE x IS NOT NULL AND x <> ""] AS evidence_refs
RETURN 'sanctioned_supplier_record' AS pattern_id,
       coalesce(c.document_id, c.nit, c.cnpj) AS company_identifier,
       coalesce(c.razao_social, c.name) AS company_name,
       toFloat((sanction_count * 2) + CASE WHEN contract_count > 0 THEN 1 ELSE 0 END) AS risk_signal,
       amount_total AS amount_total,
       toInteger(contract_count) AS contract_count,
       toInteger(sanction_count) AS sanction_count,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
