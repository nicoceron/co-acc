CALL {
  MATCH (p:Person)-[:RECIBIO_SALARIO]->(:PublicOffice)
  RETURN DISTINCT p
  UNION
  MATCH (p:Person)-[:SANCIONADA]->(:Sanction)
  RETURN DISTINCT p
  UNION
  MATCH (p:Person)-[:DONO_A]->(:Election)
  RETURN DISTINCT p
  UNION
  MATCH (p:Person)-[:CANDIDATO_EM]->(:Election)
  RETURN DISTINCT p
  UNION
  MATCH (p:Person)-[:DECLARO_BIEN]->(:DeclaredAsset)
  RETURN DISTINCT p
  UNION
  MATCH (p:Person)-[:DECLARO_FINANZAS]->(:Finance {type: 'CONFLICT_DISCLOSURE'})
  RETURN DISTINCT p
  UNION
  MATCH (p:Person)-[:OFFICER_OF]->(:Company)
  RETURN DISTINCT p
  UNION
  MATCH (p:Person)-[:REFERENTE_A]->(:Inquiry {source: 'official_case_bulletins'})
  RETURN DISTINCT p
}
WITH DISTINCT p
WITH p,
     trim(coalesce(p.document_id, '')) AS person_document_id,
     coalesce(trim(p.document_id), trim(p.case_person_id)) AS person_key,
     COUNT {
       (p)-[:RECIBIO_SALARIO|DONO_A|CANDIDATO_EM|DECLARO_BIEN|DECLARO_FINANZAS|OFFICER_OF|REFERENTE_A]->()
     } AS signal_degree
WHERE person_key <> ''
ORDER BY signal_degree DESC,
         person_key ASC
LIMIT CASE
        WHEN toInteger($limit) * 20 > 2500
        THEN 2500
        ELSE toInteger($limit) * 20
      END

CALL {
  WITH p, person_document_id
  OPTIONAL MATCH (p)-[:POSSIBLY_SAME_AS]->(probable_person:Person)
  OPTIONAL MATCH (p)-[:POSSIBLY_SAME_AS]->(probable_company:Company)
  WITH p,
       collect(DISTINCT probable_person) AS probable_people,
       collect(DISTINCT probable_company) AS probable_companies
  WITH [person IN [p] + probable_people WHERE person IS NOT NULL] AS person_candidates,
       probable_companies
  UNWIND person_candidates AS person_candidate
  OPTIONAL MATCH (person_candidate)-[:OFFICER_OF]->(officer_company:Company)
  OPTIONAL MATCH (same_document_company:Company {document_id: person_candidate.document_id})
  OPTIONAL MATCH (same_nit_company:Company {nit: person_candidate.document_id})
  WITH probable_companies,
       collect(DISTINCT officer_company) AS officer_companies,
       collect(DISTINCT same_document_company) AS same_document_companies,
       collect(DISTINCT same_nit_company) AS same_nit_companies
  WITH [company IN probable_companies
                  + officer_companies
                  + same_document_companies
                  + same_nit_companies
        WHERE company IS NOT NULL] AS linked_companies
  UNWIND CASE
           WHEN size(linked_companies) = 0 THEN [null]
           ELSE linked_companies
         END AS linked_company
  WITH DISTINCT linked_company
  OPTIONAL MATCH ()-[award:CONTRATOU]->(linked_company)
  RETURN count(DISTINCT linked_company) AS linked_supplier_company_count,
         count(DISTINCT award.summary_id) AS supplier_contract_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS supplier_contract_value
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[salary:RECIBIO_SALARIO]->(o:PublicOffice)
  RETURN count(DISTINCT o) AS office_count,
         count(
           DISTINCT CASE
             WHEN coalesce(salary.sensitive_position, false)
               OR coalesce(o.sensitive_position, false)
             THEN o
             ELSE null
           END
         ) AS sensitive_office_count,
         collect(DISTINCT coalesce(o.role, o.cargo, o.name, o.org))[0..3] AS offices
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[sp:SUPERVISA_PAGO]->(supervised_company:Company)
  OPTIONAL MATCH (:Company)-[award:CONTRATOU {summary_id: sp.summary_id}]->(supervised_company)
  WITH sp, supervised_company, award
  WHERE sp IS NOT NULL
  WITH DISTINCT sp.summary_id AS summary_id,
       supervised_company,
       award
  RETURN count(DISTINCT summary_id) AS payment_supervision_count,
         count(DISTINCT supervised_company) AS payment_supervision_company_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS payment_supervision_contract_value,
         count(
           DISTINCT CASE
             WHEN coalesce(award.archive_document_count, 0) > 0
             THEN summary_id
             ELSE NULL
           END
         ) AS payment_supervision_archive_contract_count,
         coalesce(sum(coalesce(award.archive_document_count, 0)), 0) AS archive_document_total,
         coalesce(sum(coalesce(award.archive_supervision_document_count, 0)), 0) AS archive_supervision_document_total,
         coalesce(sum(coalesce(award.archive_payment_document_count, 0)), 0) AS archive_payment_document_total,
         coalesce(sum(coalesce(award.archive_assignment_document_count, 0)), 0) AS archive_assignment_document_total,
         count(
           DISTINCT CASE
             WHEN award.summary_id IS NOT NULL
              AND (
                (
                  coalesce(award.invoice_total_value, 0.0) > 0.0
                  AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
                )
                OR (
                  coalesce(award.commitment_total_value, 0.0) > 0.0
                  AND coalesce(award.invoice_total_value, 0.0) >
                      coalesce(award.commitment_total_value, 0.0) * (1.0 + toFloat($pattern_min_discrepancy_ratio))
                )
                OR coalesce(award.suspension_event_count, 0) > 0
                OR (
                  coalesce(award.payment_pending_count, 0) > 0
                  AND coalesce(award.payment_actual_count, 0) = 0
                )
              )
             THEN summary_id
             ELSE NULL
           END
         ) AS payment_supervision_risk_contract_count,
         count(
           DISTINCT CASE
             WHEN award.summary_id IS NOT NULL
              AND (
                (
                  coalesce(award.invoice_total_value, 0.0) > 0.0
                  AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
                )
                OR (
                  coalesce(award.commitment_total_value, 0.0) > 0.0
                  AND coalesce(award.invoice_total_value, 0.0) >
                      coalesce(award.commitment_total_value, 0.0) * (1.0 + toFloat($pattern_min_discrepancy_ratio))
                )
              )
             THEN summary_id
             ELSE NULL
           END
         ) AS payment_supervision_discrepancy_contract_count,
         count(
           DISTINCT CASE
             WHEN coalesce(award.suspension_event_count, 0) > 0
             THEN summary_id
             ELSE NULL
           END
         ) AS payment_supervision_suspension_contract_count,
         count(
           DISTINCT CASE
             WHEN coalesce(award.payment_pending_count, 0) > 0
              AND coalesce(award.payment_actual_count, 0) = 0
             THEN summary_id
             ELSE NULL
           END
         ) AS payment_supervision_pending_contract_count
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
  WITH p
  OPTIONAL MATCH (p)-[:SANCIONADA]->(ps:Sanction)
  RETURN count(DISTINCT ps) AS person_sanction_count,
         count(
           DISTINCT CASE
             WHEN ps.type = 'PACO_DISCIPLINARY_SANCTION'
               OR (
                 ps.type = 'SIRI_ANTECEDENT'
                 AND toUpper(coalesce(ps.sanction_domain, '')) = 'DISCIPLINARIO'
               )
             THEN ps
             ELSE NULL
           END
         ) AS disciplinary_sanction_count,
         count(
           DISTINCT CASE
             WHEN ps.type = 'PACO_FISCAL_RESPONSIBILITY'
               OR ps.type = 'CO_FISCAL_RESPONSIBILITY'
               OR (
                 ps.type = 'SIRI_ANTECEDENT'
                 AND toUpper(coalesce(ps.sanction_domain, '')) = 'FISCAL'
               )
             THEN ps
             ELSE NULL
           END
         ) AS fiscal_responsibility_count
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:REFERENTE_A]->(inq:Inquiry {source: 'official_case_bulletins'})
  RETURN count(DISTINCT inq) AS official_case_bulletin_count,
         collect(DISTINCT coalesce(inq.title, inq.name))[0..3] AS official_case_bulletin_titles
}

CALL {
  WITH p, person_document_id
  OPTIONAL MATCH (p)-[:POSSIBLY_SAME_AS]->(probable_person:Person)
  OPTIONAL MATCH (p)-[:POSSIBLY_SAME_AS]->(probable_company:Company)
  WITH p,
       collect(DISTINCT probable_person) AS probable_people,
       collect(DISTINCT probable_company) AS probable_companies
  WITH [person IN [p] + probable_people WHERE person IS NOT NULL] AS person_candidates,
       probable_companies
  UNWIND person_candidates AS person_candidate
  OPTIONAL MATCH (person_candidate)-[:OFFICER_OF]->(linked_company:Company)
  OPTIONAL MATCH (same_document_company:Company {document_id: person_candidate.document_id})
  OPTIONAL MATCH (same_nit_company:Company {nit: person_candidate.document_id})
  WITH probable_companies,
       collect(DISTINCT linked_company) AS linked_officer_companies,
       collect(DISTINCT same_document_company) AS same_document_companies,
       collect(DISTINCT same_nit_company) AS same_nit_companies
  WITH [company IN probable_companies
                  + linked_officer_companies
                  + same_document_companies
                  + same_nit_companies
        WHERE company IS NOT NULL] AS linked_companies
  UNWIND CASE
           WHEN size(linked_companies) = 0 THEN [null]
           ELSE linked_companies
         END AS linked_company
  WITH DISTINCT linked_company
  OPTIONAL MATCH (linked_company)-[:SANCIONADA]->(cs:Sanction)
  RETURN count(DISTINCT cs) AS linked_company_sanction_count
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
     sensitive_office_count,
     payment_supervision_count,
     payment_supervision_company_count,
     payment_supervision_contract_value,
     payment_supervision_archive_contract_count,
     archive_document_total,
     archive_supervision_document_total,
     archive_payment_document_total,
     archive_assignment_document_total,
     payment_supervision_risk_contract_count,
     payment_supervision_discrepancy_contract_count,
     payment_supervision_suspension_contract_count,
     payment_supervision_pending_contract_count,
     linked_supplier_company_count,
     supplier_contract_count,
     supplier_contract_value,
     person_sanction_count,
     disciplinary_sanction_count,
     fiscal_responsibility_count,
     official_case_bulletin_count,
     official_case_bulletin_titles,
     person_sanction_count + linked_company_sanction_count AS sanction_count,
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
       WHEN office_count > 0 AND supplier_contract_count > 0 THEN 1
       ELSE 0
     END AS office_supplier_overlap,
     CASE
       WHEN sensitive_office_count > 0 AND supplier_contract_count > 0 THEN 1
       ELSE 0
     END AS sensitive_supplier_overlap,
     CASE
       WHEN donation_count > 0 AND supplier_contract_count > 0 THEN 1
       ELSE 0
     END AS donor_supplier_overlap,
     CASE
       WHEN candidacy_count > 0 AND supplier_contract_count > 0 THEN 1
       ELSE 0
     END AS candidate_supplier_overlap,
     CASE
       WHEN linked_supplier_company_count >= 2 AND supplier_contract_count > 0 THEN 1
       ELSE 0
     END AS shared_supplier_network_overlap,
     CASE
       WHEN payment_supervision_count > 0
        AND payment_supervision_risk_contract_count > 0
        AND (office_count > 0 OR donation_count > 0 OR candidacy_count > 0)
       THEN 1
       ELSE 0
     END AS payment_supervision_overlap,
     CASE
       WHEN person_sanction_count > 0
        AND (
          office_count > 0
          OR supplier_contract_count > 0
          OR donation_count > 0
          OR candidacy_count > 0
          OR payment_supervision_count > 0
        )
       THEN 1
       ELSE 0
     END AS sanctioned_person_exposure_overlap,
     CASE
       WHEN official_case_bulletin_count > 0
        AND (
          office_count > 0
          OR supplier_contract_count > 0
          OR payment_supervision_count > 0
          OR person_sanction_count > 0
        )
       THEN 1
       ELSE 0
     END AS official_case_bulletin_overlap,
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
WHERE office_count > 0
   OR donation_count > 0
   OR candidacy_count > 0
   OR conflict_disclosure_count > 0
   OR asset_count > 0
   OR supplier_contract_count > 0
   OR official_case_bulletin_count > 0

WITH p,
     person_document_id,
     offices,
     office_count,
     sensitive_office_count,
     payment_supervision_count,
     payment_supervision_company_count,
     payment_supervision_contract_value,
     payment_supervision_archive_contract_count,
     archive_document_total,
     archive_supervision_document_total,
     archive_payment_document_total,
     archive_assignment_document_total,
     payment_supervision_risk_contract_count,
     payment_supervision_discrepancy_contract_count,
     payment_supervision_suspension_contract_count,
     payment_supervision_pending_contract_count,
     linked_supplier_company_count,
     supplier_contract_count,
     supplier_contract_value,
     person_sanction_count,
     disciplinary_sanction_count,
     fiscal_responsibility_count,
     official_case_bulletin_count,
     official_case_bulletin_titles,
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
     office_supplier_overlap,
     sensitive_supplier_overlap,
     donor_supplier_overlap,
     candidate_supplier_overlap,
     shared_supplier_network_overlap,
     payment_supervision_overlap,
     sanctioned_person_exposure_overlap,
     official_case_bulletin_overlap,
     disclosure_contract_overlap,
     CASE
       WHEN office_count > 0 AND donation_count > 0 AND supplier_contract_count > 0
       THEN CASE
              WHEN donation_count < supplier_contract_count
              THEN donation_count
              ELSE supplier_contract_count
            END
       ELSE 0
     END AS donor_vendor_loop_count,
     (
       office_supplier_overlap +
       donor_supplier_overlap +
       candidate_supplier_overlap +
       shared_supplier_network_overlap +
       payment_supervision_overlap +
       sanctioned_person_exposure_overlap +
       official_case_bulletin_overlap +
       disclosure_contract_overlap +
       CASE WHEN sanction_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN asset_count > 0 THEN 1 ELSE 0 END
     ) AS signal_types

WITH p,
     person_document_id,
     offices,
     office_count,
     sensitive_office_count,
     payment_supervision_count,
     payment_supervision_company_count,
     payment_supervision_contract_value,
     payment_supervision_archive_contract_count,
     archive_document_total,
     archive_supervision_document_total,
     archive_payment_document_total,
     archive_assignment_document_total,
     payment_supervision_risk_contract_count,
     payment_supervision_discrepancy_contract_count,
     payment_supervision_suspension_contract_count,
     payment_supervision_pending_contract_count,
     linked_supplier_company_count,
     supplier_contract_count,
     supplier_contract_value,
     person_sanction_count,
     disciplinary_sanction_count,
     fiscal_responsibility_count,
     official_case_bulletin_count,
     official_case_bulletin_titles,
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
     office_supplier_overlap,
     sensitive_supplier_overlap,
     donor_supplier_overlap,
     candidate_supplier_overlap,
     shared_supplier_network_overlap,
     payment_supervision_overlap,
     sanctioned_person_exposure_overlap,
     official_case_bulletin_overlap,
     disclosure_contract_overlap,
     donor_vendor_loop_count,
     signal_types,
     (
       CASE WHEN office_supplier_overlap > 0 THEN 24 ELSE 0 END +
       CASE WHEN sensitive_supplier_overlap > 0 THEN 12 ELSE 0 END +
       CASE WHEN shared_supplier_network_overlap > 0 THEN 12 ELSE 0 END +
       CASE
         WHEN person_sanction_count >= 5 AND sanctioned_person_exposure_overlap > 0 THEN 22
         WHEN person_sanction_count >= 2 AND sanctioned_person_exposure_overlap > 0 THEN 18
         WHEN person_sanction_count >= 1 AND sanctioned_person_exposure_overlap > 0 THEN 14
         WHEN person_sanction_count > 0 THEN 6
         ELSE 0
       END +
       CASE
         WHEN fiscal_responsibility_count > 0 THEN 4
         ELSE 0
       END +
       CASE
         WHEN official_case_bulletin_overlap > 0 THEN 20
         WHEN official_case_bulletin_count > 0 THEN 10
         ELSE 0
       END +
       CASE
         WHEN payment_supervision_risk_contract_count >= 10 THEN 44
         WHEN payment_supervision_risk_contract_count >= 5 THEN 36
         WHEN payment_supervision_risk_contract_count >= 3 THEN 30
         WHEN payment_supervision_overlap > 0 THEN 24
         ELSE 0
       END +
       CASE
         WHEN payment_supervision_overlap > 0 AND payment_supervision_archive_contract_count >= 10 THEN 10
         WHEN payment_supervision_overlap > 0 AND payment_supervision_archive_contract_count >= 3 THEN 8
         WHEN payment_supervision_overlap > 0 AND payment_supervision_archive_contract_count > 0 THEN 6
         ELSE 0
       END +
       CASE
         WHEN archive_supervision_document_total >= 10 THEN 6
         WHEN archive_supervision_document_total >= 3 THEN 4
         WHEN archive_supervision_document_total > 0 THEN 2
         ELSE 0
       END +
       CASE
         WHEN archive_payment_document_total >= 5 THEN 5
         WHEN archive_payment_document_total >= 1 THEN 3
         ELSE 0
       END +
       CASE
         WHEN archive_assignment_document_total >= 3 THEN 4
         WHEN archive_assignment_document_total >= 1 THEN 2
         ELSE 0
       END +
       CASE
         WHEN payment_supervision_company_count >= 10 THEN 12
         WHEN payment_supervision_company_count >= 5 THEN 8
         WHEN payment_supervision_company_count >= 1 THEN 3
         ELSE 0
       END +
       CASE
         WHEN donation_count >= 10 THEN 12
         WHEN donation_count >= 1 THEN 6
         ELSE 0
       END +
       CASE
         WHEN candidacy_count > 0 AND supplier_contract_count > 0 THEN 10
         WHEN candidacy_count > 0 THEN 4
         ELSE 0
       END +
       CASE
         WHEN disclosure_contract_overlap > 0 THEN 18
         WHEN conflict_disclosure_count >= 1 THEN 10
         ELSE 0
       END +
       CASE
         WHEN donor_vendor_loop_count >= 5 THEN 20
         WHEN donor_vendor_loop_count >= 1 THEN 14
         ELSE 0
       END +
       CASE
         WHEN explicit_conflict_count >= 3 THEN 12
         WHEN explicit_conflict_count >= 1 THEN 8
         ELSE 0
       END +
       CASE
         WHEN disclosure_reference_count >= 10 THEN 14
         WHEN disclosure_reference_count >= 3 THEN 10
         WHEN disclosure_reference_count >= 1 THEN 6
         ELSE 0
       END +
       CASE WHEN family_term_count > 0 THEN 6 ELSE 0 END +
       CASE WHEN legal_role_term_count > 0 THEN 4 ELSE 0 END +
       CASE WHEN litigation_term_count > 0 THEN 3 ELSE 0 END +
       CASE WHEN corporate_activity_disclosure_count > 0 THEN 8 ELSE 0 END +
       CASE
         WHEN linked_supplier_company_count >= 2 THEN 8
         WHEN linked_supplier_company_count = 1 THEN 4
         ELSE 0
       END +
       CASE
         WHEN supplier_contract_count >= 10 THEN 14
         WHEN supplier_contract_count >= 3 THEN 10
         WHEN supplier_contract_count >= 1 THEN 5
         ELSE 0
       END +
       CASE
       WHEN supplier_contract_value >= 1000000000 THEN 12
        WHEN supplier_contract_value >= 100000000 THEN 8
        WHEN supplier_contract_value > 0 THEN 4
        ELSE 0
      END +
       CASE
         WHEN payment_supervision_contract_value >= 500000000 THEN 8
         WHEN payment_supervision_contract_value >= 100000000 THEN 5
         WHEN payment_supervision_contract_value > 0 THEN 2
         ELSE 0
       END +
       CASE WHEN asset_count > 0 THEN 3 ELSE 0 END +
       CASE WHEN sanction_count > 0 THEN 4 ELSE 0 END
     ) AS suspicion_score

RETURN elementId(p) AS entity_id,
       coalesce(p.name, p.nome, p.full_name, p.case_person_id, person_document_id) AS name,
       CASE
         WHEN person_document_id <> '' THEN person_document_id
         ELSE NULL
       END AS document_id,
       CASE
         WHEN coalesce(trim(p.case_person_id), '') <> '' THEN trim(p.case_person_id)
         ELSE NULL
       END AS case_person_id,
       toInteger(suspicion_score) AS suspicion_score,
       toInteger(signal_types) AS signal_types,
       toInteger(office_count) AS office_count,
       toInteger(sensitive_office_count) AS sensitive_office_count,
       toInteger(donation_count) AS donation_count,
       toFloat(donation_value) AS donation_value,
       toInteger(candidacy_count) AS candidacy_count,
       toInteger(asset_count) AS asset_count,
       toFloat(0.0) AS asset_value,
       toInteger(conflict_disclosure_count) AS finance_count,
       toFloat(0.0) AS finance_value,
       toInteger(linked_supplier_company_count) AS linked_supplier_company_count,
       toInteger(supplier_contract_count) AS supplier_contract_count,
       toFloat(supplier_contract_value) AS supplier_contract_value,
       toInteger(person_sanction_count) AS person_sanction_count,
       toInteger(disciplinary_sanction_count) AS disciplinary_sanction_count,
       toInteger(fiscal_responsibility_count) AS fiscal_responsibility_count,
       toInteger(conflict_disclosure_count) AS conflict_disclosure_count,
       toInteger(
         disclosure_reference_count
         + family_term_count
         + legal_role_term_count
       + litigation_term_count
       ) AS disclosure_reference_count,
       toInteger(corporate_activity_disclosure_count) AS corporate_activity_disclosure_count,
       toInteger(donor_vendor_loop_count) AS donor_vendor_loop_count,
       toInteger(payment_supervision_count) AS payment_supervision_count,
       toInteger(payment_supervision_company_count) AS payment_supervision_company_count,
       toInteger(payment_supervision_risk_contract_count) AS payment_supervision_risk_contract_count,
       toInteger(payment_supervision_discrepancy_contract_count) AS payment_supervision_discrepancy_contract_count,
       toInteger(payment_supervision_suspension_contract_count) AS payment_supervision_suspension_contract_count,
       toInteger(payment_supervision_pending_contract_count) AS payment_supervision_pending_contract_count,
       toFloat(payment_supervision_contract_value) AS payment_supervision_contract_value,
       toInteger(payment_supervision_archive_contract_count) AS payment_supervision_archive_contract_count,
       toInteger(archive_document_total) AS archive_document_total,
       toInteger(archive_supervision_document_total) AS archive_supervision_document_total,
       toInteger(archive_payment_document_total) AS archive_payment_document_total,
       toInteger(archive_assignment_document_total) AS archive_assignment_document_total,
       toInteger(official_case_bulletin_count) AS official_case_bulletin_count,
       official_case_bulletin_titles AS official_case_bulletin_titles,
       offices AS offices
ORDER BY suspicion_score DESC,
         person_sanction_count DESC,
         payment_supervision_risk_contract_count DESC,
         archive_supervision_document_total DESC,
         archive_payment_document_total DESC,
         donor_vendor_loop_count DESC,
         supplier_contract_value DESC,
         disclosure_reference_count DESC,
         name ASC
LIMIT toInteger($limit)
