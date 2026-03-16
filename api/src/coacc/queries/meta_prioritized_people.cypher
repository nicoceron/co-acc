MATCH (p:Person)
WHERE exists(p.document_id) OR exists(p.cedula)
WITH p,
     trim(coalesce(p.document_id, p.cedula, '')) AS person_document_id
WHERE person_document_id <> ''

CALL {
  WITH p, person_document_id
  OPTIONAL MATCH (c:Company)
  WHERE c.document_id = person_document_id
     OR EXISTS { MATCH (p)-[:OFFICER_OF]->(c) }
  WITH collect(DISTINCT c) AS linked_companies
  UNWIND CASE
           WHEN size(linked_companies) = 0 THEN [null]
           ELSE linked_companies
         END AS linked_company
  OPTIONAL MATCH ()-[award:CONTRATOU]->(linked_company)
  RETURN count(DISTINCT linked_company) AS linked_supplier_company_count,
         count(DISTINCT award.summary_id) AS supplier_contract_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS supplier_contract_value
}
WITH p,
     person_document_id,
     linked_supplier_company_count,
     supplier_contract_count,
     supplier_contract_value
WHERE supplier_contract_count > 0

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:RECIBIO_SALARIO]->(o:PublicOffice)
  RETURN count(DISTINCT o) AS office_count,
         collect(DISTINCT coalesce(o.role, o.cargo, o.name, o.org))[0..3] AS offices
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[d:DONO_A]->(:Election)
  RETURN count(DISTINCT d) AS donation_count,
         coalesce(sum(coalesce(d.value, d.valor, 0.0)), 0.0) AS donation_value
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:CANDIDATO_EM]->(e:Election)
  RETURN count(DISTINCT e) AS candidacy_count
}

CALL {
  WITH p, person_document_id
  OPTIONAL MATCH (p)-[:SANCIONADA]->(ps:Sanction)
  WITH p,
       person_document_id,
       count(DISTINCT ps) AS person_sanction_count
  OPTIONAL MATCH (c:Company)
  WHERE c.document_id = person_document_id
     OR EXISTS { MATCH (p)-[:OFFICER_OF]->(c) }
  WITH person_sanction_count,
       collect(DISTINCT c) AS linked_companies
  UNWIND CASE
           WHEN size(linked_companies) = 0 THEN [null]
           ELSE linked_companies
         END AS linked_company
  OPTIONAL MATCH (linked_company)-[:SANCIONADA]->(cs:Sanction)
  RETURN person_sanction_count + count(DISTINCT cs) AS sanction_count
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DECLARO_BIEN]->(a:DeclaredAsset)
  RETURN count(DISTINCT a) AS asset_count,
         count(
           DISTINCT CASE
             WHEN coalesce(a.has_board_roles, false)
               OR coalesce(a.has_corporate_interests, false)
               OR coalesce(a.has_private_activities, false)
             THEN a
             ELSE null
           END
         ) AS corporate_activity_disclosure_count
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DECLARO_FINANZAS]->(f:Finance {type: 'CONFLICT_DISCLOSURE'})
  RETURN count(DISTINCT f) AS conflict_disclosure_count,
         coalesce(
           sum(
             CASE
               WHEN coalesce(f.family_conflicts, false)
                 OR coalesce(f.partner_involved, false)
                 OR coalesce(f.direct_interests, false)
                 OR coalesce(f.other_possible_conflicts, false)
               THEN 1
               ELSE 0
             END
           ),
           0
         ) AS explicit_conflict_count,
         coalesce(
           sum(
             coalesce(f.company_document_mention_count, 0)
             + coalesce(f.company_name_mention_count, 0)
             + coalesce(f.process_reference_count, 0)
           ),
           0
         ) AS disclosure_reference_count,
         coalesce(sum(coalesce(f.family_term_count, 0)), 0) AS family_term_count,
         coalesce(sum(coalesce(f.legal_role_term_count, 0)), 0) AS legal_role_term_count,
         coalesce(sum(coalesce(f.litigation_term_count, 0)), 0) AS litigation_term_count
}

WITH p,
     person_document_id,
     offices,
     office_count,
     linked_supplier_company_count,
     supplier_contract_count,
     supplier_contract_value,
     sanction_count,
     donation_count,
     donation_value,
     candidacy_count,
     asset_count,
     conflict_disclosure_count,
     explicit_conflict_count,
     disclosure_reference_count,
     family_term_count,
     legal_role_term_count,
     litigation_term_count,
     corporate_activity_disclosure_count,
     CASE
       WHEN office_count > 0 AND donation_count > 0 AND supplier_contract_count > 0
       THEN CASE
              WHEN donation_count < supplier_contract_count
              THEN donation_count
              ELSE supplier_contract_count
            END
       ELSE 0
     END AS donor_vendor_loop_count,
     CASE
       WHEN office_count > 0 AND supplier_contract_count > 0
       THEN 1
       ELSE 0
     END AS office_supplier_overlap,
     CASE
       WHEN donation_count > 0 AND supplier_contract_count > 0
       THEN 1
       ELSE 0
     END AS donor_supplier_overlap,
     CASE
       WHEN candidacy_count > 0 AND supplier_contract_count > 0
       THEN 1
       ELSE 0
     END AS candidate_supplier_overlap,
     CASE
       WHEN supplier_contract_count > 0 AND (
         conflict_disclosure_count > 0
         OR explicit_conflict_count > 0
         OR disclosure_reference_count > 0
         OR family_term_count > 0
         OR legal_role_term_count > 0
         OR litigation_term_count > 0
         OR corporate_activity_disclosure_count > 0
       )
       THEN 1
       ELSE 0
     END AS disclosure_contract_overlap
WHERE office_supplier_overlap > 0
   OR donor_supplier_overlap > 0
   OR candidate_supplier_overlap > 0
   OR disclosure_contract_overlap > 0

WITH p,
     person_document_id,
     offices,
     office_count,
     linked_supplier_company_count,
     supplier_contract_count,
     supplier_contract_value,
     sanction_count,
     donation_count,
     donation_value,
     candidacy_count,
     asset_count,
     conflict_disclosure_count,
     explicit_conflict_count,
     disclosure_reference_count,
     family_term_count,
     legal_role_term_count,
     litigation_term_count,
     corporate_activity_disclosure_count,
     donor_vendor_loop_count,
     (
       office_supplier_overlap +
       donor_supplier_overlap +
       candidate_supplier_overlap +
       disclosure_contract_overlap +
       CASE WHEN sanction_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN asset_count > 0 THEN 1 ELSE 0 END
     ) AS signal_types,
     (
       CASE WHEN office_supplier_overlap > 0 THEN 35 ELSE 0 END +
       CASE
         WHEN donor_vendor_loop_count >= 5 THEN 30
         WHEN donor_vendor_loop_count >= 1 THEN 22
         ELSE 0
       END +
       CASE
         WHEN donation_count >= 10 THEN 12
         WHEN donation_count >= 1 THEN 6
         ELSE 0
       END +
       CASE WHEN candidacy_count > 0 THEN 8 ELSE 0 END +
       CASE
         WHEN explicit_conflict_count >= 3 THEN 20
         WHEN explicit_conflict_count >= 1 THEN 14
         WHEN conflict_disclosure_count >= 1 THEN 10
         ELSE 0
       END +
       CASE
         WHEN disclosure_reference_count >= 10 THEN 18
         WHEN disclosure_reference_count >= 3 THEN 12
         WHEN disclosure_reference_count >= 1 THEN 6
         ELSE 0
       END +
       CASE WHEN family_term_count > 0 THEN 8 ELSE 0 END +
       CASE WHEN legal_role_term_count > 0 THEN 6 ELSE 0 END +
       CASE WHEN litigation_term_count > 0 THEN 4 ELSE 0 END +
       CASE WHEN corporate_activity_disclosure_count > 0 THEN 12 ELSE 0 END +
       CASE
         WHEN linked_supplier_company_count >= 2 THEN 6
         WHEN linked_supplier_company_count = 1 THEN 3
         ELSE 0
       END +
       CASE
         WHEN supplier_contract_count >= 10 THEN 12
         WHEN supplier_contract_count >= 3 THEN 8
         WHEN supplier_contract_count >= 1 THEN 4
         ELSE 0
       END +
       CASE
         WHEN supplier_contract_value >= 1000000000 THEN 12
         WHEN supplier_contract_value >= 100000000 THEN 8
         WHEN supplier_contract_value > 0 THEN 4
         ELSE 0
       END +
       CASE WHEN sanction_count > 0 THEN 4 ELSE 0 END
     ) AS suspicion_score

RETURN elementId(p) AS entity_id,
       coalesce(p.name, p.nome, p.full_name, person_document_id) AS name,
       person_document_id AS document_id,
       toInteger(suspicion_score) AS suspicion_score,
       toInteger(signal_types) AS signal_types,
       toInteger(office_count) AS office_count,
       toInteger(donation_count) AS donation_count,
       toFloat(donation_value) AS donation_value,
       toInteger(candidacy_count) AS candidacy_count,
       toInteger(asset_count) AS asset_count,
       toFloat(0.0) AS asset_value,
       toInteger(conflict_disclosure_count) AS finance_count,
       toFloat(0.0) AS finance_value,
       toInteger(supplier_contract_count) AS supplier_contract_count,
       toFloat(supplier_contract_value) AS supplier_contract_value,
       toInteger(conflict_disclosure_count) AS conflict_disclosure_count,
       toInteger(
         disclosure_reference_count
         + family_term_count
         + legal_role_term_count
         + litigation_term_count
       ) AS disclosure_reference_count,
       toInteger(corporate_activity_disclosure_count) AS corporate_activity_disclosure_count,
       toInteger(donor_vendor_loop_count) AS donor_vendor_loop_count,
       offices AS offices
ORDER BY suspicion_score DESC,
         donor_vendor_loop_count DESC,
         disclosure_reference_count DESC,
         supplier_contract_value DESC,
         name ASC
LIMIT toInteger($limit)
