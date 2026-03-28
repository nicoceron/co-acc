MATCH (p)
WHERE (elementId(p) = $person_id
       OR p.document_id = $person_document_id
       OR p.cedula = $person_document_id)
MATCH (p)-[sp:SUPERVISA_PAGO]->(supervised_company:Company)
OPTIONAL MATCH (:Company)-[award:CONTRATOU {summary_id: sp.summary_id}]->(supervised_company)
OPTIONAL MATCH (p)-[:RECIBIO_SALARIO]->(o:PublicOffice)
OPTIONAL MATCH (p)-[:DONO_A|CANDIDATO_EM]->(e:Election)
WITH p,
     collect(DISTINCT coalesce(o.role, o.cargo, o.name, o.org))[0..5] AS office_names,
     count(DISTINCT o) AS office_count,
     count(DISTINCT e) AS political_event_count,
     collect(DISTINCT coalesce(supervised_company.razon_social, supervised_company.name, supervised_company.document_id))[0..5] AS supervised_company_names,
     collect(DISTINCT sp.summary_id) AS summary_ids,
     count(DISTINCT supervised_company) AS supervised_company_count,
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
         THEN sp.summary_id
         ELSE NULL
       END
     ) AS risky_contract_count,
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
         THEN sp.summary_id
         ELSE NULL
       END
     ) AS discrepancy_contract_count,
     count(
       DISTINCT CASE
         WHEN coalesce(award.suspension_event_count, 0) > 0
         THEN sp.summary_id
         ELSE NULL
       END
     ) AS suspension_contract_count,
     count(
       DISTINCT CASE
         WHEN coalesce(award.payment_pending_count, 0) > 0
          AND coalesce(award.payment_actual_count, 0) = 0
         THEN sp.summary_id
         ELSE NULL
       END
     ) AS pending_payment_contract_count,
     coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS amount_total
WITH p,
     office_names,
     office_count,
     political_event_count,
     supervised_company_names,
     supervised_company_count,
     risky_contract_count,
     discrepancy_contract_count,
     suspension_contract_count,
     pending_payment_contract_count,
     amount_total,
     [x IN summary_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs
WHERE risky_contract_count >= 1
  AND (office_count > 0 OR political_event_count > 0)
RETURN 'payment_supervision_risk_stack' AS pattern_id,
       coalesce(p.name, p.nombre, p.full_name, p.document_id) AS subject_name,
       coalesce(p.document_id, p.cedula) AS document_id,
       toFloat(
         risky_contract_count
         + supervised_company_count
         + office_count
         + political_event_count
         + discrepancy_contract_count
         + suspension_contract_count
         + pending_payment_contract_count
       ) AS risk_signal,
       office_names AS office_names,
       supervised_company_names AS supervised_company_names,
       toInteger(supervised_company_count) AS supervised_company_count,
       toInteger(risky_contract_count) AS risky_contract_count,
       toInteger(discrepancy_contract_count) AS discrepancy_contract_count,
       toInteger(suspension_contract_count) AS suspension_contract_count,
       toInteger(pending_payment_contract_count) AS pending_payment_contract_count,
       amount_total AS amount_total,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
