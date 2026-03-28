MATCH (c:Company)
WHERE c.document_id = $document_id
   OR c.nit = $document_id
WITH DISTINCT c
LIMIT 1

CALL {
  WITH c
  OPTIONAL MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT award.summary_id) AS contract_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS contract_value,
         count(
           DISTINCT CASE
             WHEN coalesce(award.invoice_total_value, 0.0) > 0.0
              AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
             THEN award.summary_id
             ELSE NULL
           END
         ) AS execution_gap_contract_count,
         coalesce(
           sum(
             CASE
               WHEN coalesce(award.invoice_total_value, 0.0) > 0.0
                AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
               THEN coalesce(award.invoice_total_value, 0.0)
               ELSE 0.0
             END
           ),
           0.0
         ) AS execution_gap_invoice_total,
         count(
           DISTINCT CASE
             WHEN coalesce(award.commitment_total_value, 0.0) > 0.0
              AND coalesce(award.invoice_total_value, 0.0) >
                  coalesce(award.commitment_total_value, 0.0)
             THEN award.summary_id
             ELSE NULL
           END
         ) AS commitment_gap_contract_count,
         coalesce(
           sum(
             CASE
               WHEN coalesce(award.commitment_total_value, 0.0) > 0.0
                AND coalesce(award.invoice_total_value, 0.0) >
                    coalesce(award.commitment_total_value, 0.0)
               THEN coalesce(award.invoice_total_value, 0.0) -
                    coalesce(award.commitment_total_value, 0.0)
               ELSE 0.0
             END
           ),
           0.0
         ) AS commitment_gap_total,
         count(
           DISTINCT CASE
             WHEN coalesce(award.suspension_event_count, 0) > 0
               OR (
                 coalesce(award.invoice_total_value, 0.0) > 0.0
                 AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
               ) OR (
                 coalesce(award.commitment_total_value, 0.0) > 0.0
                 AND coalesce(award.invoice_total_value, 0.0) >
                     coalesce(award.commitment_total_value, 0.0)
               )
             THEN award.summary_id
             ELSE NULL
           END
         ) AS interadmin_risk_contract_count,
         count(
           DISTINCT CASE
             WHEN coalesce(award.suspension_event_count, 0) > 0
             THEN award.summary_id
             ELSE NULL
           END
         ) AS suspension_contract_count,
         coalesce(sum(coalesce(award.suspension_event_count, 0)), 0) AS suspension_event_count,
         count(
           DISTINCT CASE
             WHEN coalesce(award.last_date, award.first_date) IS NOT NULL
              AND EXISTS {
                MATCH (c)-[:SANCIONADA]->(active:Sanction)
                WHERE active.date_start IS NOT NULL
                  AND trim(active.date_start) <> ''
                  AND coalesce(award.last_date, award.first_date) >= active.date_start
                  AND (
                    active.date_end IS NULL
                    OR trim(coalesce(active.date_end, '')) = ''
                    OR coalesce(award.last_date, award.first_date) <= active.date_end
                  )
              }
             THEN award.summary_id
             ELSE NULL
           END
         ) AS sanctioned_still_receiving_contract_count,
         coalesce(
           sum(
             CASE
               WHEN coalesce(award.last_date, award.first_date) IS NOT NULL
                AND EXISTS {
                  MATCH (c)-[:SANCIONADA]->(active:Sanction)
                  WHERE active.date_start IS NOT NULL
                    AND trim(active.date_start) <> ''
                    AND coalesce(award.last_date, award.first_date) >= active.date_start
                    AND (
                      active.date_end IS NULL
                      OR trim(coalesce(active.date_end, '')) = ''
                      OR coalesce(award.last_date, award.first_date) <= active.date_end
                    )
                }
               THEN coalesce(award.total_value, 0.0)
               ELSE 0.0
             END
           ),
           0.0
         ) AS sanctioned_still_receiving_total,
         count(
           DISTINCT CASE
             WHEN award.average_value IS NOT NULL
              AND award.average_value >= toFloat($pattern_split_min_average_value)
              AND award.average_value <= toFloat($pattern_split_threshold_value)
              AND coalesce(award.contract_count, 0) >= toInteger($pattern_split_min_count)
              AND coalesce(award.total_value, 0.0) >= toFloat($pattern_split_min_total_value)
             THEN award.summary_id
             ELSE NULL
           END
         ) AS split_contract_group_count,
         coalesce(
           sum(
             CASE
               WHEN award.average_value IS NOT NULL
                AND award.average_value >= toFloat($pattern_split_min_average_value)
                AND award.average_value <= toFloat($pattern_split_threshold_value)
                AND coalesce(award.contract_count, 0) >= toInteger($pattern_split_min_count)
                AND coalesce(award.total_value, 0.0) >= toFloat($pattern_split_min_total_value)
               THEN coalesce(award.total_value, 0.0)
               ELSE 0.0
             END
           ),
           0.0
         ) AS split_contract_total
}

CALL {
  WITH c
  OPTIONAL MATCH (c)-[:SANCIONADA]->(s:Sanction)
  RETURN count(DISTINCT s) AS sanction_count
}

CALL {
  WITH c
  OPTIONAL MATCH (:Company)-[ia:CELEBRO_CONVENIO_INTERADMIN]->(c)
  RETURN count(DISTINCT ia.summary_id) AS interadmin_agreement_count,
         coalesce(sum(coalesce(ia.total_value, 0.0)), 0.0) AS interadmin_total
}

CALL {
  WITH c
  OPTIONAL MATCH (director:Person)-[:ADMINISTRA]->(c)
  RETURN count(DISTINCT director) AS education_director_count
}

CALL {
  WITH c
  OPTIONAL MATCH (left:Person)-[:ADMINISTRA]->(c)
  OPTIONAL MATCH (left)-[rel:POSSIBLE_FAMILY_TIE]-(:Person)-[:ADMINISTRA]->(c)
  RETURN count(DISTINCT rel) AS education_family_tie_count
}

CALL {
  WITH c
  OPTIONAL MATCH (c)-[:SAME_AS]-(alias:Company)
  RETURN count(DISTINCT alias) AS education_alias_count
}

CALL {
  WITH c
  OPTIONAL MATCH (c)-[:SAME_AS]-(alias:Company)
  WITH c, collect(DISTINCT alias) AS aliases
  WITH [entity IN aliases + [c] WHERE entity IS NOT NULL] AS targets
  UNWIND targets AS target
  OPTIONAL MATCH ()-[ia:CELEBRO_CONVENIO_INTERADMIN]->(target)
  WITH collect(DISTINCT ia) AS agreements
  RETURN size(agreements) AS education_procurement_link_count,
         reduce(
           total = 0.0,
           agreement IN agreements |
             total + coalesce(agreement.total_value, 0.0)
         ) AS education_procurement_total
}

CALL {
  WITH c
  OPTIONAL MATCH (person:Person)-[:OFFICER_OF]->(c)
  OPTIONAL MATCH (person)-[salary:RECIBIO_SALARIO]->(office:PublicOffice)
  RETURN count(
           DISTINCT CASE
             WHEN office IS NOT NULL
             THEN person
             ELSE NULL
           END
         ) AS official_officer_count,
         count(
           DISTINCT CASE
             WHEN office IS NOT NULL
             THEN office
             ELSE NULL
           END
         ) AS official_role_count,
         count(
           DISTINCT CASE
             WHEN office IS NOT NULL
              AND (
                coalesce(salary.sensitive_position, false)
                OR coalesce(office.sensitive_position, false)
              )
             THEN person
             ELSE NULL
           END
         ) AS sensitive_officer_count,
         count(
           DISTINCT CASE
             WHEN office IS NOT NULL
              AND (
                coalesce(salary.sensitive_position, false)
                OR coalesce(office.sensitive_position, false)
              )
             THEN office
             ELSE NULL
           END
         ) AS sensitive_role_count,
         [name IN collect(
           DISTINCT CASE
             WHEN office IS NOT NULL
             THEN coalesce(person.name, person.nombre, person.full_name, person.document_id)
             ELSE NULL
           END
         )
         WHERE name IS NOT NULL][0..5] AS official_names
}

RETURN elementId(c) AS entity_id,
       coalesce(c.razon_social, c.name, c.document_id, c.nit) AS name,
       coalesce(c.document_id, c.nit) AS document_id,
       coalesce(c.company_type, '') AS company_type,
       toInteger(contract_count) AS contract_count,
       contract_value AS contract_value,
       toInteger(sanction_count) AS sanction_count,
       toInteger(official_officer_count) AS official_officer_count,
       toInteger(official_role_count) AS official_role_count,
       toInteger(sensitive_officer_count) AS sensitive_officer_count,
       toInteger(sensitive_role_count) AS sensitive_role_count,
       official_names AS official_names,
       0 AS funding_overlap_event_count,
       0.0 AS funding_overlap_total,
       0 AS capacity_mismatch_contract_count,
       0.0 AS capacity_mismatch_contract_value,
       0.0 AS capacity_mismatch_revenue_ratio,
       0.0 AS capacity_mismatch_asset_ratio,
       toInteger(execution_gap_contract_count) AS execution_gap_contract_count,
       execution_gap_invoice_total AS execution_gap_invoice_total,
       toInteger(commitment_gap_contract_count) AS commitment_gap_contract_count,
       commitment_gap_total AS commitment_gap_total,
       toInteger(interadmin_agreement_count) AS interadmin_agreement_count,
       interadmin_total AS interadmin_total,
       toInteger(education_director_count) AS education_director_count,
       toInteger(education_family_tie_count) AS education_family_tie_count,
       toInteger(education_alias_count) AS education_alias_count,
       toInteger(education_procurement_link_count) AS education_procurement_link_count,
       education_procurement_total AS education_procurement_total,
       toInteger(interadmin_risk_contract_count) AS interadmin_risk_contract_count,
       toInteger(suspension_contract_count) AS suspension_contract_count,
       toInteger(suspension_event_count) AS suspension_event_count,
       toInteger(sanctioned_still_receiving_contract_count) AS sanctioned_still_receiving_contract_count,
       sanctioned_still_receiving_total AS sanctioned_still_receiving_total,
       toInteger(split_contract_group_count) AS split_contract_group_count,
       split_contract_total AS split_contract_total
