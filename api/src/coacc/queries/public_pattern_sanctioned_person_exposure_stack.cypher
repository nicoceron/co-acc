MATCH (p)
WHERE (elementId(p) = $person_id
       OR p.document_id = $person_document_id
       OR p.cedula = $person_document_id)

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:SANCIONADA]->(s:Sanction)
  WHERE s.type IN [
    'PACO_DISCIPLINARY_SANCTION',
    'PACO_FISCAL_RESPONSIBILITY',
    'PACO_SECOP_FINE',
    'SIRI_ANTECEDENT',
    'CO_FISCAL_RESPONSIBILITY'
  ]
  RETURN collect(DISTINCT s.type)[0..5] AS sanction_types,
         collect(DISTINCT coalesce(s.document_id, s.name))[0..toInteger($pattern_max_evidence_refs)] AS sanction_refs,
         count(DISTINCT s) AS sanction_count,
         count(
           DISTINCT CASE
             WHEN s.type = 'PACO_DISCIPLINARY_SANCTION'
               OR (
                 s.type = 'SIRI_ANTECEDENT'
                 AND toUpper(coalesce(s.sanction_domain, '')) = 'DISCIPLINARIO'
               )
             THEN s
             ELSE NULL
           END
         ) AS disciplinary_sanction_count,
         count(
           DISTINCT CASE
             WHEN s.type = 'PACO_FISCAL_RESPONSIBILITY'
               OR s.type = 'CO_FISCAL_RESPONSIBILITY'
               OR (
                 s.type = 'SIRI_ANTECEDENT'
                 AND toUpper(coalesce(s.sanction_domain, '')) = 'FISCAL'
               )
             THEN s
             ELSE NULL
           END
         ) AS fiscal_responsibility_count
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:RECIBIO_SALARIO]->(o:PublicOffice)
  RETURN collect(DISTINCT coalesce(o.role, o.cargo, o.name, o.org))[0..5] AS office_names,
         count(DISTINCT o) AS office_count
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DONO_A|CANDIDATO_EM]->(e:Election)
  RETURN count(DISTINCT e) AS political_event_count
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[:OFFICER_OF]->(officer_company:Company)
  OPTIONAL MATCH (same_document_company:Company {document_id: $person_document_id})
  OPTIONAL MATCH (same_nit_company:Company {nit: $person_document_id})
  WITH [company IN collect(DISTINCT officer_company)
                  + collect(DISTINCT same_document_company)
                  + collect(DISTINCT same_nit_company)
        WHERE company IS NOT NULL] AS linked_companies
  UNWIND CASE
           WHEN size(linked_companies) = 0 THEN [null]
           ELSE linked_companies
         END AS linked_company
  WITH DISTINCT linked_company
  OPTIONAL MATCH ()-[award:CONTRATOU]->(linked_company)
  RETURN collect(
           DISTINCT coalesce(
             linked_company.razon_social,
             linked_company.name,
             linked_company.document_id
           )
         )[0..5] AS supplier_names,
         count(DISTINCT linked_company) AS linked_supplier_company_count,
         count(DISTINCT award.summary_id) AS supplier_contract_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS supplier_contract_value,
         collect(DISTINCT award.summary_id)[0..toInteger($pattern_max_evidence_refs)] AS contract_refs
}

CALL {
  WITH p
  OPTIONAL MATCH (p)-[sp:SUPERVISA_PAGO]->(supervised_company:Company)
  OPTIONAL MATCH (:Company)-[award:CONTRATOU {summary_id: sp.summary_id}]->(supervised_company)
  WITH DISTINCT sp.summary_id AS summary_id, award
  RETURN count(DISTINCT summary_id) AS payment_supervision_count,
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
         ) AS risky_payment_contract_count
}

WITH p,
     sanction_types,
     sanction_refs,
     sanction_count,
     disciplinary_sanction_count,
     fiscal_responsibility_count,
     office_names,
     office_count,
     political_event_count,
     supplier_names,
     linked_supplier_company_count,
     supplier_contract_count,
     supplier_contract_value,
     payment_supervision_count,
     risky_payment_contract_count,
     [x IN contract_refs WHERE x IS NOT NULL AND x <> ''] AS contract_refs
WHERE sanction_count > 0
  AND (
    office_count > 0
    OR supplier_contract_count > 0
    OR political_event_count > 0
    OR risky_payment_contract_count > 0
  )

WITH p,
     sanction_types,
     office_names,
     supplier_names,
     sanction_count,
     disciplinary_sanction_count,
     fiscal_responsibility_count,
     office_count,
     political_event_count,
     linked_supplier_company_count,
     supplier_contract_count,
     supplier_contract_value,
     payment_supervision_count,
     risky_payment_contract_count,
     [x IN sanction_refs + contract_refs WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
RETURN 'sanctioned_person_exposure_stack' AS pattern_id,
       coalesce(p.name, p.nombre, p.full_name, p.document_id) AS subject_name,
       coalesce(p.document_id, p.cedula) AS document_id,
       toFloat(
         sanction_count
         + disciplinary_sanction_count
         + fiscal_responsibility_count
         + office_count
         + political_event_count
         + linked_supplier_company_count
         + supplier_contract_count
         + risky_payment_contract_count
       ) AS risk_signal,
       sanction_types AS sanction_types,
       office_names AS office_names,
       supplier_names AS supplier_names,
       toInteger(sanction_count) AS sanction_count,
       toInteger(disciplinary_sanction_count) AS disciplinary_sanction_count,
       toInteger(fiscal_responsibility_count) AS fiscal_responsibility_count,
       toInteger(office_count) AS office_count,
       toInteger(political_event_count) AS political_event_count,
       toInteger(linked_supplier_company_count) AS linked_supplier_company_count,
       toInteger(supplier_contract_count) AS supplier_contract_count,
       supplier_contract_value AS supplier_contract_value,
       toInteger(payment_supervision_count) AS payment_supervision_count,
       toInteger(risky_payment_contract_count) AS risky_payment_contract_count,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
