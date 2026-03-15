MATCH (p:Person)
WHERE elementId(p) = $person_id
   OR p.document_id = $person_document_id
   OR p.cedula = $person_document_id
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:RECEBEU_SALARIO]->(o:PublicOffice)
  RETURN count(DISTINCT o) AS office_count,
         collect(DISTINCT coalesce(o.role, o.cargo, o.name, o.org))[0..5] AS office_names
}
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DECLAROU_FINANCA]->(f:Finance {type: 'CONFLICT_DISCLOSURE'})
  RETURN count(DISTINCT f) AS conflict_disclosure_count,
         sum(
           CASE
             WHEN coalesce(f.family_conflicts, false)
               OR coalesce(f.partner_involved, false)
               OR coalesce(f.direct_interests, false)
               OR coalesce(f.other_possible_conflicts, false)
             THEN 1
             ELSE 0
           END
         ) AS explicit_conflict_count,
         sum(coalesce(f.company_document_mention_count, 0)) AS company_document_mention_count,
         sum(coalesce(f.company_name_mention_count, 0)) AS company_name_mention_count,
         sum(coalesce(f.process_reference_count, 0)) AS process_reference_count,
         sum(coalesce(f.family_term_count, 0)) AS family_term_count,
         sum(coalesce(f.legal_role_term_count, 0)) AS legal_role_term_count,
         sum(coalesce(f.litigation_term_count, 0)) AS litigation_term_count,
         collect(DISTINCT f.finance_id) AS finance_ids
}
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DECLAROU_BEM]->(a:DeclaredAsset)
  RETURN max(
           CASE
             WHEN coalesce(a.has_board_roles, false)
               OR coalesce(a.has_corporate_interests, false)
               OR coalesce(a.has_private_activities, false)
             THEN 1
             ELSE 0
           END
         ) AS corporate_activity_flag,
         collect(
           DISTINCT CASE
             WHEN coalesce(a.has_board_roles, false)
               OR coalesce(a.has_corporate_interests, false)
               OR coalesce(a.has_private_activities, false)
             THEN a.asset_id
             ELSE NULL
           END
         ) AS asset_ids
}
CALL {
  WITH p
  OPTIONAL MATCH (c:Company {document_id: coalesce(p.document_id, p.cedula)})
  OPTIONAL MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT award.summary_id) AS supplier_contract_count,
         sum(coalesce(award.total_value, 0.0)) AS supplier_contract_value,
         collect(DISTINCT award.summary_id) AS summary_ids
}
WITH p,
     office_count,
     office_names,
     conflict_disclosure_count,
     explicit_conflict_count,
     company_document_mention_count,
     company_name_mention_count,
     process_reference_count,
     family_term_count,
     legal_role_term_count,
     litigation_term_count,
     corporate_activity_flag,
     supplier_contract_count,
     supplier_contract_value,
     [x IN finance_ids WHERE x IS NOT NULL AND x <> ''] AS finance_ids,
     [x IN asset_ids WHERE x IS NOT NULL AND x <> ''] AS asset_ids,
     [x IN summary_ids WHERE x IS NOT NULL AND x <> ''] AS summary_ids
WITH p,
     office_count,
     office_names,
     conflict_disclosure_count,
     explicit_conflict_count,
     company_document_mention_count,
     company_name_mention_count,
     process_reference_count,
     family_term_count,
     legal_role_term_count,
     litigation_term_count,
     corporate_activity_flag,
     supplier_contract_count,
     supplier_contract_value,
     finance_ids,
     asset_ids,
     summary_ids,
     (company_document_mention_count + company_name_mention_count + process_reference_count) AS disclosure_reference_count
WHERE supplier_contract_count >= 1
  AND (
    conflict_disclosure_count >= 1
    OR disclosure_reference_count >= 1
    OR office_count >= 1
    OR corporate_activity_flag = 1
  )
WITH p,
     office_count,
     office_names,
     conflict_disclosure_count,
     explicit_conflict_count,
     company_document_mention_count,
     company_name_mention_count,
     process_reference_count,
     family_term_count,
     legal_role_term_count,
     litigation_term_count,
     disclosure_reference_count,
     corporate_activity_flag,
     supplier_contract_count,
     supplier_contract_value,
     finance_ids,
     asset_ids,
     summary_ids,
     reduce(
       refs = [],
       ref IN finance_ids + asset_ids + summary_ids |
         CASE
           WHEN ref IS NULL OR ref = '' OR ref IN refs THEN refs
           ELSE refs + ref
         END
     ) AS evidence_refs
RETURN 'disclosure_risk_stack' AS pattern_id,
       coalesce(p.name, p.nome, p.full_name, p.document_id) AS subject_name,
       p.document_id AS document_id,
       toFloat(
         supplier_contract_count
         + CASE WHEN office_count > 0 THEN 2 ELSE 0 END
         + explicit_conflict_count
         + disclosure_reference_count
         + family_term_count
         + legal_role_term_count
         + litigation_term_count
         + CASE WHEN corporate_activity_flag = 1 THEN 2 ELSE 0 END
       ) AS risk_signal,
       office_names AS office_names,
       toInteger(office_count) AS office_count,
       toInteger(supplier_contract_count) AS supplier_contract_count,
       supplier_contract_value AS supplier_contract_value,
       toInteger(conflict_disclosure_count) AS conflict_disclosure_count,
       toInteger(explicit_conflict_count) AS explicit_conflict_count,
       toInteger(company_document_mention_count) AS company_document_mention_count,
       toInteger(company_name_mention_count) AS company_name_mention_count,
       toInteger(process_reference_count) AS process_reference_count,
       toInteger(disclosure_reference_count) AS disclosure_reference_count,
       toInteger(family_term_count) AS kinship_term_count,
       toInteger(legal_role_term_count) AS legal_role_term_count,
       toInteger(litigation_term_count) AS litigation_term_count,
       toBoolean(corporate_activity_flag = 1) AS corporate_activity_flag,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
