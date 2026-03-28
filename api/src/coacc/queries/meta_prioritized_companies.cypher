CALL {
  MATCH (c:Company)-[:SANCIONADA]->(s:Sanction)
  RETURN elementId(c) AS company_id,
         c AS company,
         count(DISTINCT s) AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         0 AS sensitive_officer_count,
         0 AS sensitive_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total,
         0 AS suspension_contract_count,
         0 AS suspension_event_count,
         0 AS sanctioned_still_receiving_contract_count,
         0.0 AS sanctioned_still_receiving_total,
         0 AS split_contract_group_count,
         0.0 AS split_contract_total
  UNION ALL
  MATCH (p:Person)-[:OFFICER_OF]->(c:Company)
  WHERE EXISTS { MATCH (p)-[:RECIBIO_SALARIO]->(:PublicOffice) }
  OPTIONAL MATCH (p)-[:RECIBIO_SALARIO]->(o:PublicOffice)
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         count(DISTINCT p) AS official_officer_count,
         count(DISTINCT o) AS official_role_count,
         count(
           DISTINCT CASE
             WHEN coalesce(o.sensitive_position, false)
             THEN p
             ELSE NULL
           END
         ) AS sensitive_officer_count,
         count(
           DISTINCT CASE
             WHEN coalesce(o.sensitive_position, false)
             THEN o
             ELSE NULL
           END
         ) AS sensitive_role_count,
         collect(DISTINCT coalesce(p.name, p.nombre, p.document_id))[0..3] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total,
         0 AS suspension_contract_count,
         0 AS suspension_event_count,
         0 AS sanctioned_still_receiving_contract_count,
         0.0 AS sanctioned_still_receiving_total,
         0 AS split_contract_group_count,
         0.0 AS split_contract_total
  UNION ALL
  MATCH (c:Company)-[offer:SUMINISTRO_LICITACAO]->(b:Bid)
  WHERE coalesce(b.offer_count, 0) <= 2
     OR coalesce(b.direct_invitation, false) = true
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         0 AS sensitive_officer_count,
         0 AS sensitive_role_count,
         [] AS official_names,
         count(DISTINCT b) AS low_competition_bid_count,
         coalesce(sum(coalesce(offer.offer_value_total, 0.0)), 0.0) AS low_competition_bid_value,
         count(DISTINCT CASE WHEN coalesce(b.direct_invitation, false) THEN b END) AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total,
         0 AS suspension_contract_count,
         0 AS suspension_event_count,
         0 AS sanctioned_still_receiving_contract_count,
         0.0 AS sanctioned_still_receiving_total,
         0 AS split_contract_group_count,
         0.0 AS split_contract_total
  UNION ALL
  MATCH ()-[award:CONTRATOU]->(c:Company)
  WHERE coalesce(award.invoice_total_value, 0.0) > 0
    AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         0 AS sensitive_officer_count,
         0 AS sensitive_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         toInteger(sum(coalesce(award.contract_count, 1))) AS execution_gap_contract_count,
         coalesce(sum(coalesce(award.invoice_total_value, 0.0)), 0.0) AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total,
         0 AS suspension_contract_count,
         0 AS suspension_event_count,
         0 AS sanctioned_still_receiving_contract_count,
         0.0 AS sanctioned_still_receiving_total,
         0 AS split_contract_group_count,
         0.0 AS split_contract_total
  UNION ALL
  MATCH ()-[award:CONTRATOU]->(c:Company)
  WHERE coalesce(award.commitment_total_value, 0.0) > 0
    AND coalesce(award.invoice_total_value, 0.0) >
        coalesce(award.commitment_total_value, 0.0) * (1.0 + toFloat($pattern_min_discrepancy_ratio))
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         0 AS sensitive_officer_count,
         0 AS sensitive_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         toInteger(sum(coalesce(award.contract_count, 1))) AS commitment_gap_contract_count,
         coalesce(
           sum(
             coalesce(award.invoice_total_value, 0.0) -
             coalesce(award.commitment_total_value, 0.0)
           ),
           0.0
         ) AS commitment_gap_total,
         0 AS suspension_contract_count,
         0 AS suspension_event_count,
         0 AS sanctioned_still_receiving_contract_count,
         0.0 AS sanctioned_still_receiving_total,
         0 AS split_contract_group_count,
         0.0 AS split_contract_total
  UNION ALL
  MATCH ()-[award:CONTRATOU]->(c:Company)
  WHERE coalesce(award.suspension_event_count, 0) > 0
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         0 AS sensitive_officer_count,
         0 AS sensitive_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total,
         toInteger(sum(coalesce(award.contract_count, 1))) AS suspension_contract_count,
         coalesce(sum(coalesce(award.suspension_event_count, 0)), 0) AS suspension_event_count,
         0 AS sanctioned_still_receiving_contract_count,
         0.0 AS sanctioned_still_receiving_total,
         0 AS split_contract_group_count,
         0.0 AS split_contract_total
  UNION ALL
  MATCH (c:Company)-[:SANCIONADA]->(s:Sanction)
  WHERE s.date_start IS NOT NULL
    AND trim(s.date_start) <> ''
  MATCH ()-[award:CONTRATOU]->(c)
  WHERE coalesce(award.last_date, award.first_date) IS NOT NULL
    AND coalesce(award.last_date, award.first_date) >= s.date_start
    AND (
      s.date_end IS NULL
      OR trim(coalesce(s.date_end, '')) = ''
      OR coalesce(award.last_date, award.first_date) <= s.date_end
    )
  WITH DISTINCT c, award
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         0 AS sensitive_officer_count,
         0 AS sensitive_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total,
         0 AS suspension_contract_count,
         0 AS suspension_event_count,
         toInteger(sum(coalesce(award.contract_count, 1))) AS sanctioned_still_receiving_contract_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS sanctioned_still_receiving_total,
         0 AS split_contract_group_count,
         0.0 AS split_contract_total
  UNION ALL
  MATCH (buyer:Company)-[award:CONTRATOU]->(c:Company)
  WHERE award.average_value IS NOT NULL
    AND award.average_value >= toFloat($pattern_split_min_average_value)
    AND award.average_value <= toFloat($pattern_split_threshold_value)
    AND coalesce(award.contract_count, 0) >= toInteger($pattern_split_min_count)
    AND coalesce(award.total_value, 0.0) >= toFloat($pattern_split_min_total_value)
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         0 AS sensitive_officer_count,
         0 AS sensitive_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total,
         0 AS suspension_contract_count,
         0 AS suspension_event_count,
         0 AS sanctioned_still_receiving_contract_count,
         0.0 AS sanctioned_still_receiving_total,
         count(award) AS split_contract_group_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS split_contract_total
  UNION ALL
  MATCH (c:Company)-[:SUMINISTRO]->(f:Finance {type: 'SGR_EXPENSE_EXECUTION'})
  WHERE coalesce(c.document_id, c.nit, '') <> '0'
    AND EXISTS { MATCH ()-[:CONTRATOU]->(c) }
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         0 AS sensitive_officer_count,
         0 AS sensitive_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         count(DISTINCT f) AS funding_overlap_event_count,
         coalesce(sum(coalesce(f.value, 0.0)), 0.0) AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total,
         0 AS suspension_contract_count,
         0 AS suspension_event_count,
         0 AS sanctioned_still_receiving_contract_count,
         0.0 AS sanctioned_still_receiving_total,
         0 AS split_contract_group_count,
         0.0 AS split_contract_total
  UNION ALL
  MATCH (c:Company)-[:DECLARO_FINANZAS]->(f:Finance {type: 'SUPERSOC_TOP_COMPANY'})
  CALL {
    WITH c
    MATCH ()-[award:CONTRATOU]->(c)
    RETURN toInteger(sum(coalesce(award.contract_count, 1))) AS contract_count,
           coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS contract_total
  }
  WITH c,
       contract_count,
       contract_total,
       CASE
         WHEN coalesce(f.operating_revenue_current, 0.0) > 0.0
         THEN contract_total / toFloat(f.operating_revenue_current)
         ELSE 0.0
       END AS revenue_ratio,
       CASE
         WHEN coalesce(f.total_assets_current, 0.0) > 0.0
         THEN contract_total / toFloat(f.total_assets_current)
         ELSE 0.0
       END AS asset_ratio
  WHERE contract_total >= toFloat($pattern_min_discrepancy_ratio) * 1000000000
    AND (
      revenue_ratio >= 2.0
      OR asset_ratio >= 1.0
    )
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         0 AS sensitive_officer_count,
         0 AS sensitive_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         contract_count AS capacity_mismatch_contract_count,
         contract_total AS capacity_mismatch_contract_value,
         toFloat(revenue_ratio) AS capacity_mismatch_revenue_ratio,
         toFloat(asset_ratio) AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total,
         0 AS suspension_contract_count,
         0 AS suspension_event_count,
         0 AS sanctioned_still_receiving_contract_count,
         0.0 AS sanctioned_still_receiving_total,
         0 AS split_contract_group_count,
         0.0 AS split_contract_total
}
WITH company_id,
     head(collect(company)) AS c,
     sum(sanction_count) AS sanction_count,
     sum(official_officer_count) AS official_officer_count,
     sum(official_role_count) AS official_role_count,
     sum(sensitive_officer_count) AS sensitive_officer_count,
     sum(sensitive_role_count) AS sensitive_role_count,
     head([names IN collect(official_names) WHERE size(names) > 0]) AS official_names,
     sum(low_competition_bid_count) AS low_competition_bid_count,
     sum(low_competition_bid_value) AS low_competition_bid_value,
     sum(direct_invitation_bid_count) AS direct_invitation_bid_count,
     sum(funding_overlap_event_count) AS funding_overlap_event_count,
     sum(funding_overlap_total) AS funding_overlap_total,
     max(capacity_mismatch_contract_count) AS capacity_mismatch_contract_count,
     max(capacity_mismatch_contract_value) AS capacity_mismatch_contract_value,
     max(capacity_mismatch_revenue_ratio) AS capacity_mismatch_revenue_ratio,
     max(capacity_mismatch_asset_ratio) AS capacity_mismatch_asset_ratio,
     sum(execution_gap_contract_count) AS execution_gap_contract_count,
     sum(execution_gap_invoice_total) AS execution_gap_invoice_total,
     sum(commitment_gap_contract_count) AS commitment_gap_contract_count,
     sum(commitment_gap_total) AS commitment_gap_total,
     sum(suspension_contract_count) AS suspension_contract_count,
     sum(suspension_event_count) AS suspension_event_count,
     sum(sanctioned_still_receiving_contract_count) AS sanctioned_still_receiving_contract_count,
     sum(sanctioned_still_receiving_total) AS sanctioned_still_receiving_total,
     sum(split_contract_group_count) AS split_contract_group_count,
     sum(split_contract_total) AS split_contract_total,
     count(*) AS signal_types
WHERE signal_types >= 1
CALL {
  WITH c
  MATCH ()-[award:CONTRATOU]->(c)
  RETURN toInteger(sum(coalesce(award.contract_count, 1))) AS contract_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS contract_value,
         count(DISTINCT coalesce(award.buyer_document_id, award.buyer_name)) AS buyer_count
}
CALL {
  WITH c
  OPTIONAL MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(
           DISTINCT CASE
             WHEN coalesce(award.archive_document_count, 0) > 0
             THEN coalesce(award.summary_id, elementId(award))
             ELSE NULL
           END
         ) AS archive_contract_count,
         coalesce(sum(coalesce(award.archive_document_count, 0)), 0) AS archive_document_total,
         count(
           DISTINCT CASE
             WHEN coalesce(award.archive_supervision_document_count, 0) > 0
             THEN coalesce(award.summary_id, elementId(award))
             ELSE NULL
           END
         ) AS archive_supervision_contract_count,
         coalesce(sum(coalesce(award.archive_supervision_document_count, 0)), 0) AS archive_supervision_document_total,
         count(
           DISTINCT CASE
             WHEN coalesce(award.archive_payment_document_count, 0) > 0
             THEN coalesce(award.summary_id, elementId(award))
             ELSE NULL
           END
         ) AS archive_payment_contract_count,
         coalesce(sum(coalesce(award.archive_payment_document_count, 0)), 0) AS archive_payment_document_total,
         count(
           DISTINCT CASE
             WHEN coalesce(award.archive_assignment_document_count, 0) > 0
             THEN coalesce(award.summary_id, elementId(award))
             ELSE NULL
           END
         ) AS archive_assignment_contract_count,
         coalesce(sum(coalesce(award.archive_assignment_document_count, 0)), 0) AS archive_assignment_document_total
}
CALL {
  WITH c
  OPTIONAL MATCH (c)-[:TIENE_HALLAZGO]->(finding:Finding)
  RETURN count(DISTINCT finding) AS fiscal_finding_count,
         coalesce(sum(coalesce(finding.amount, 0.0)), 0.0) AS fiscal_finding_total
}
CALL {
  WITH c
  OPTIONAL MATCH (:Company)-[ia:CELEBRO_CONVENIO_INTERADMIN]->(c)
  WITH c,
       count(DISTINCT ia.summary_id) AS interadmin_agreement_count,
       coalesce(sum(coalesce(ia.total_value, 0.0)), 0.0) AS interadmin_total
  CALL {
    WITH c
    OPTIONAL MATCH ()-[award:CONTRATOU]->(c)
    RETURN toInteger(sum(
             CASE
               WHEN coalesce(award.suspension_event_count, 0) > 0
                 OR (
                   coalesce(award.invoice_total_value, 0.0) > 0.0
                   AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
                 ) OR (
                   coalesce(award.commitment_total_value, 0.0) > 0.0
                   AND coalesce(award.invoice_total_value, 0.0) >
                       coalesce(award.commitment_total_value, 0.0) * (1.0 + toFloat($pattern_min_discrepancy_ratio))
                 )
               THEN coalesce(award.contract_count, 1)
               ELSE 0
             END
           )) AS interadmin_risk_contract_count
  }
  CALL {
    WITH c
    OPTIONAL MATCH (p:Person)-[:OFFICER_OF]->(c)
    WHERE EXISTS {
      MATCH (p)-[:RECIBIO_SALARIO]->(:PublicOffice)
    }
    RETURN count(DISTINCT p) AS interadmin_official_overlap_count
  }
  CALL {
    WITH c
    OPTIONAL MATCH (c)-[:SANCIONADA]->(s:Sanction)
    RETURN count(DISTINCT s) AS interadmin_sanction_count
  }
  RETURN toInteger(interadmin_agreement_count) AS interadmin_agreement_count,
         toFloat(interadmin_total) AS interadmin_total,
         toInteger(
           CASE
             WHEN interadmin_agreement_count > 0
              AND (
                interadmin_risk_contract_count > 0
                OR interadmin_official_overlap_count > 0
                OR interadmin_sanction_count > 0
              )
             THEN interadmin_risk_contract_count
             ELSE 0
           END
         ) AS interadmin_risk_contract_count,
         toInteger(
           CASE
             WHEN interadmin_agreement_count > 0
              AND (
                interadmin_risk_contract_count > 0
                OR interadmin_official_overlap_count > 0
                OR interadmin_sanction_count > 0
              )
             THEN 1
             ELSE 0
           END
         ) AS interadmin_signal_flag
}
WITH c,
     contract_count,
     contract_value,
     buyer_count,
     archive_contract_count,
     archive_document_total,
     archive_supervision_contract_count,
     archive_supervision_document_total,
     archive_payment_contract_count,
     archive_payment_document_total,
     archive_assignment_contract_count,
     archive_assignment_document_total,
     fiscal_finding_count,
     fiscal_finding_total,
     sanction_count,
     official_officer_count,
     official_role_count,
     sensitive_officer_count,
     sensitive_role_count,
     coalesce(official_names, []) AS official_names,
     low_competition_bid_count,
     low_competition_bid_value,
     direct_invitation_bid_count,
     funding_overlap_event_count,
     funding_overlap_total,
     capacity_mismatch_contract_count,
     capacity_mismatch_contract_value,
     capacity_mismatch_revenue_ratio,
     capacity_mismatch_asset_ratio,
     execution_gap_contract_count,
     execution_gap_invoice_total,
     commitment_gap_contract_count,
     commitment_gap_total,
     suspension_contract_count,
     suspension_event_count,
     sanctioned_still_receiving_contract_count,
     sanctioned_still_receiving_total,
     split_contract_group_count,
     split_contract_total,
     interadmin_agreement_count,
     interadmin_total,
     interadmin_risk_contract_count,
     signal_types + interadmin_signal_flag + CASE WHEN fiscal_finding_count > 0 THEN 1 ELSE 0 END AS signal_types,
     (
       CASE
         WHEN fiscal_finding_count >= 5 THEN 4
         WHEN fiscal_finding_count >= 2 THEN 3
         WHEN fiscal_finding_count > 0 THEN 2
         ELSE 0
       END +
       CASE
         WHEN sanction_count >= 10 THEN 6
         WHEN sanction_count >= 3 THEN 5
         WHEN sanction_count > 0 THEN 4
         ELSE 0
       END +
       CASE WHEN official_officer_count > 0 THEN 4 ELSE 0 END +
       CASE WHEN sensitive_officer_count > 0 THEN 2 ELSE 0 END +
       CASE
         WHEN low_competition_bid_count >= 5 THEN 3
         WHEN low_competition_bid_count >= 2 THEN 2
         ELSE 0
       END +
       CASE
         WHEN funding_overlap_event_count >= 20 THEN 3
         WHEN funding_overlap_event_count >= 5 THEN 2
         WHEN funding_overlap_event_count > 0 THEN 1
         ELSE 0
       END +
       CASE
         WHEN capacity_mismatch_revenue_ratio >= 10 OR capacity_mismatch_asset_ratio >= 5 THEN 4
         WHEN capacity_mismatch_revenue_ratio >= 5 OR capacity_mismatch_asset_ratio >= 2 THEN 3
         WHEN capacity_mismatch_revenue_ratio >= 2 OR capacity_mismatch_asset_ratio >= 1 THEN 2
         ELSE 0
       END +
       CASE
         WHEN execution_gap_contract_count >= 5 THEN 3
         WHEN execution_gap_contract_count >= 2 THEN 2
         ELSE 0
       END +
       CASE
         WHEN commitment_gap_contract_count >= 5 THEN 3
         WHEN commitment_gap_contract_count >= 2 THEN 2
         ELSE 0
       END +
       CASE
         WHEN suspension_event_count >= 5 THEN 3
         WHEN suspension_event_count >= 2 THEN 2
         WHEN suspension_event_count > 0 THEN 1
         ELSE 0
       END +
       CASE
         WHEN sanctioned_still_receiving_contract_count > 0 THEN 5
         ELSE 0
       END +
       CASE
         WHEN archive_contract_count >= 10 THEN 3
         WHEN archive_contract_count >= 3 THEN 2
         WHEN archive_contract_count > 0 THEN 1
         ELSE 0
       END +
       CASE WHEN archive_supervision_document_total > 0 THEN 1 ELSE 0 END +
       CASE WHEN archive_payment_document_total > 0 THEN 1 ELSE 0 END +
       CASE WHEN archive_assignment_document_total > 0 THEN 1 ELSE 0 END +
       CASE
         WHEN split_contract_group_count > 0 AND split_contract_total >= 120000000.0 THEN 6
         WHEN split_contract_group_count > 0 THEN 5
         ELSE 0
       END +
       CASE
         WHEN interadmin_signal_flag = 0 THEN 0
         WHEN interadmin_agreement_count >= 10 AND interadmin_risk_contract_count >= 3 THEN 4
         WHEN interadmin_agreement_count >= 3 AND interadmin_risk_contract_count >= 1 THEN 3
         ELSE 2
       END +
       CASE
         WHEN contract_count >= 10 THEN 2
         WHEN contract_count >= 3 THEN 1
         ELSE 0
       END
     ) AS suspicion_score
RETURN elementId(c) AS entity_id,
       coalesce(c.razon_social, c.name, c.document_id, c.nit) AS name,
       coalesce(c.document_id, c.nit) AS document_id,
       toInteger(suspicion_score) AS suspicion_score,
       toInteger(signal_types) AS signal_types,
       toInteger(contract_count) AS contract_count,
       toFloat(contract_value) AS contract_value,
       toInteger(buyer_count) AS buyer_count,
       toInteger(fiscal_finding_count) AS fiscal_finding_count,
       toFloat(fiscal_finding_total) AS fiscal_finding_total,
       toInteger(sanction_count) AS sanction_count,
       toInteger(official_officer_count) AS official_officer_count,
       toInteger(official_role_count) AS official_role_count,
       toInteger(sensitive_officer_count) AS sensitive_officer_count,
       toInteger(sensitive_role_count) AS sensitive_role_count,
       official_names AS official_names,
       toInteger(low_competition_bid_count) AS low_competition_bid_count,
       toFloat(low_competition_bid_value) AS low_competition_bid_value,
       toInteger(direct_invitation_bid_count) AS direct_invitation_bid_count,
       toInteger(funding_overlap_event_count) AS funding_overlap_event_count,
       toFloat(funding_overlap_total) AS funding_overlap_total,
       toInteger(capacity_mismatch_contract_count) AS capacity_mismatch_contract_count,
       toFloat(capacity_mismatch_contract_value) AS capacity_mismatch_contract_value,
       toFloat(capacity_mismatch_revenue_ratio) AS capacity_mismatch_revenue_ratio,
       toFloat(capacity_mismatch_asset_ratio) AS capacity_mismatch_asset_ratio,
       toInteger(execution_gap_contract_count) AS execution_gap_contract_count,
       toFloat(execution_gap_invoice_total) AS execution_gap_invoice_total,
       toInteger(commitment_gap_contract_count) AS commitment_gap_contract_count,
       toFloat(commitment_gap_total) AS commitment_gap_total,
       toInteger(interadmin_agreement_count) AS interadmin_agreement_count,
       toFloat(interadmin_total) AS interadmin_total,
       toInteger(interadmin_risk_contract_count) AS interadmin_risk_contract_count,
       toInteger(suspension_contract_count) AS suspension_contract_count,
       toInteger(suspension_event_count) AS suspension_event_count,
       toInteger(sanctioned_still_receiving_contract_count) AS sanctioned_still_receiving_contract_count,
       toFloat(sanctioned_still_receiving_total) AS sanctioned_still_receiving_total,
       toInteger(split_contract_group_count) AS split_contract_group_count,
       toFloat(split_contract_total) AS split_contract_total,
       toInteger(archive_contract_count) AS archive_contract_count,
       toInteger(archive_document_total) AS archive_document_total,
       toInteger(archive_supervision_contract_count) AS archive_supervision_contract_count,
       toInteger(archive_supervision_document_total) AS archive_supervision_document_total,
       toInteger(archive_payment_contract_count) AS archive_payment_contract_count,
       toInteger(archive_payment_document_total) AS archive_payment_document_total,
       toInteger(archive_assignment_contract_count) AS archive_assignment_contract_count,
       toInteger(archive_assignment_document_total) AS archive_assignment_document_total
ORDER BY suspicion_score DESC,
         fiscal_finding_count DESC,
         archive_supervision_document_total DESC,
         archive_payment_document_total DESC,
         capacity_mismatch_revenue_ratio DESC,
         capacity_mismatch_asset_ratio DESC,
         contract_value DESC,
         sanction_count DESC,
         low_competition_bid_count DESC,
         name ASC
LIMIT toInteger($limit)
