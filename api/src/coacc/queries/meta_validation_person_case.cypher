MATCH (p:Person)
WHERE p.document_id = $person_ref
   OR p.cedula = $person_ref
   OR p.case_person_id = $person_ref
WITH DISTINCT p
LIMIT 1

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:POSSIBLY_SAME_AS]->(probable_person:Person)
  WITH p, collect(DISTINCT probable_person) AS probable_people
  WITH [person IN [p] + probable_people WHERE person IS NOT NULL] AS person_candidates
  UNWIND person_candidates AS person_candidate
  OPTIONAL MATCH (person_candidate)-[salary:RECIBIO_SALARIO]->(office:PublicOffice)
  RETURN count(DISTINCT office) AS office_count,
         count(
           DISTINCT CASE
             WHEN coalesce(salary.sensitive_position, false)
               OR coalesce(office.sensitive_position, false)
             THEN office
             ELSE NULL
           END
         ) AS sensitive_office_count,
         collect(DISTINCT coalesce(office.role, office.cargo, office.name, office.org))[0..3]
           AS offices
}

CALL {
  WITH p
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
  OPTIONAL MATCH (p)-[:POSSIBLY_SAME_AS]->(probable_person:Person)
  WITH p, collect(DISTINCT probable_person) AS probable_people
  WITH [person IN [p] + probable_people WHERE person IS NOT NULL] AS person_candidates
  UNWIND person_candidates AS person_candidate
  OPTIONAL MATCH (person_candidate)-[donation:DONO_A]->(:Election)
  RETURN count(DISTINCT donation) AS donation_count,
         coalesce(sum(coalesce(donation.value, donation.valor, 0.0)), 0.0) AS donation_value
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:POSSIBLY_SAME_AS]->(probable_person:Person)
  WITH p, collect(DISTINCT probable_person) AS probable_people
  WITH [person IN [p] + probable_people WHERE person IS NOT NULL] AS person_candidates
  UNWIND person_candidates AS person_candidate
  OPTIONAL MATCH (person_candidate)-[:CANDIDATO_EM]->(election:Election)
  RETURN count(DISTINCT election) AS candidacy_count
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:POSSIBLY_SAME_AS]->(probable_person:Person)
  WITH p, collect(DISTINCT probable_person) AS probable_people
  WITH [person IN [p] + probable_people WHERE person IS NOT NULL] AS person_candidates
  UNWIND person_candidates AS person_candidate
  OPTIONAL MATCH (person_candidate)-[:DECLARO_BIEN]->(asset:DeclaredAsset)
  RETURN count(DISTINCT asset) AS asset_count,
         count(
           DISTINCT CASE
             WHEN coalesce(asset.has_board_roles, false)
               OR coalesce(asset.has_corporate_interests, false)
               OR coalesce(asset.has_private_activities, false)
             THEN asset
             ELSE NULL
           END
         ) AS corporate_activity_disclosure_count
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:POSSIBLY_SAME_AS]->(probable_person:Person)
  WITH p, collect(DISTINCT probable_person) AS probable_people
  WITH [person IN [p] + probable_people WHERE person IS NOT NULL] AS person_candidates
  UNWIND person_candidates AS person_candidate
  OPTIONAL MATCH (person_candidate)-[:DECLARO_FINANZAS]->(finance:Finance {type: 'CONFLICT_DISCLOSURE'})
  RETURN count(DISTINCT finance) AS conflict_disclosure_count,
         coalesce(
           sum(
             coalesce(finance.company_document_mention_count, 0)
             + coalesce(finance.company_name_mention_count, 0)
             + coalesce(finance.process_reference_count, 0)
           ),
           0
         ) AS disclosure_reference_count
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:POSSIBLY_SAME_AS]->(probable_person:Person)
  WITH p, collect(DISTINCT probable_person) AS probable_people
  WITH [person IN [p] + probable_people WHERE person IS NOT NULL] AS person_candidates
  UNWIND person_candidates AS person_candidate
  OPTIONAL MATCH (person_candidate)-[:SANCIONADA]->(ps:Sanction)
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

RETURN elementId(p) AS entity_id,
       coalesce(p.name, p.nombre, p.full_name, p.document_id, p.case_person_id, p.cedula) AS name,
       coalesce(p.document_id, p.cedula) AS document_id,
       p.case_person_id AS case_person_id,
       toInteger(office_count) AS office_count,
       toInteger(sensitive_office_count) AS sensitive_office_count,
       offices AS offices,
       toInteger(linked_supplier_company_count) AS linked_supplier_company_count,
       toInteger(supplier_contract_count) AS supplier_contract_count,
       supplier_contract_value AS supplier_contract_value,
       toInteger(donation_count) AS donation_count,
       donation_value AS donation_value,
       toInteger(candidacy_count) AS candidacy_count,
       toInteger(asset_count) AS asset_count,
       toInteger(conflict_disclosure_count) AS conflict_disclosure_count,
       toInteger(disclosure_reference_count) AS disclosure_reference_count,
       toInteger(corporate_activity_disclosure_count) AS corporate_activity_disclosure_count,
       toInteger(person_sanction_count) AS person_sanction_count,
       toInteger(disciplinary_sanction_count) AS disciplinary_sanction_count,
       toInteger(fiscal_responsibility_count) AS fiscal_responsibility_count,
       toInteger(official_case_bulletin_count) AS official_case_bulletin_count,
       official_case_bulletin_titles AS official_case_bulletin_titles,
       CASE
         WHEN office_count > 0 AND donation_count > 0 AND supplier_contract_count > 0
         THEN CASE
                WHEN donation_count < supplier_contract_count
                THEN donation_count
                ELSE supplier_contract_count
              END
         ELSE 0
       END AS donor_vendor_loop_count
