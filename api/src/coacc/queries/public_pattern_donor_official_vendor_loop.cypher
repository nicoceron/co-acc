MATCH (p)
WHERE (elementId(p) = $person_id
       OR p.document_id = $person_document_id
       OR p.cedula = $person_document_id)
MATCH (p)-[:RECEBEU_SALARIO]->(o:PublicOffice)
MATCH (c:Company {document_id: coalesce(p.document_id, p.cedula)})
MATCH ()-[award:CONTRATOU]->(c)
OPTIONAL MATCH (p)-[d:DOOU]->(:Election)
OPTIONAL MATCH (p)-[:DECLAROU_FINANCA]->(f:Finance {type: 'CONFLICT_DISCLOSURE'})
WITH p,
     collect(DISTINCT coalesce(o.role, o.cargo, o.name, o.org))[0..5] AS office_names,
     count(DISTINCT award.summary_id) AS supplier_contract_count,
     sum(coalesce(award.total_value, 0.0)) AS supplier_contract_value,
     count(DISTINCT d) AS donation_count,
     sum(coalesce(d.value, 0.0)) AS donation_value,
     count(DISTINCT f) AS conflict_disclosure_count,
     max(
       CASE
         WHEN coalesce(f.family_conflicts, false)
           OR coalesce(f.partner_involved, false)
           OR coalesce(f.direct_interests, false)
           OR coalesce(f.other_possible_conflicts, false)
         THEN 1
         ELSE 0
       END
     ) AS conflict_flag,
     collect(DISTINCT award.summary_id) AS summary_ids,
     min(coalesce(award.first_date, award.last_date)) AS window_start,
     max(coalesce(award.last_date, award.first_date)) AS window_end
WITH p,
     office_names,
     supplier_contract_count,
     supplier_contract_value,
     donation_count,
     donation_value,
     conflict_disclosure_count,
     conflict_flag,
     window_start,
     window_end,
     [x IN summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE supplier_contract_count >= 1
  AND donation_count >= 1
RETURN 'donor_official_vendor_loop' AS pattern_id,
       coalesce(p.name, p.nome, p.full_name, p.document_id) AS subject_name,
       p.document_id AS document_id,
       toFloat(
         supplier_contract_count
         + donation_count
         + CASE WHEN conflict_disclosure_count > 0 THEN 2 ELSE 0 END
         + CASE WHEN conflict_flag = 1 THEN 1 ELSE 0 END
       ) AS risk_signal,
       office_names AS office_names,
       toInteger(supplier_contract_count) AS supplier_contract_count,
       supplier_contract_value AS supplier_contract_value,
       toInteger(donation_count) AS donation_count,
       donation_value AS donation_value,
       toInteger(conflict_disclosure_count) AS conflict_disclosure_count,
       toBoolean(conflict_flag = 1) AS conflict_flag,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
