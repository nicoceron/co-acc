// Gather exposure metrics for a given entity
// Aggregates across SAME_AS equivalent Person nodes
MATCH (e)
WHERE elementId(e) = $entity_id
  AND (e:Person OR e:Company OR e:Contract OR e:Sanction OR e:Election
       OR e:Amendment OR e:Finance OR e:Embargo OR e:Health OR e:Education
       OR e:Convenio OR e:LaborStats OR e:PublicOffice OR e:Bid OR e:DeclaredAsset
       OR e:Inquiry)
WITH e, labels(e) AS lbls
// Collect equivalent nodes: self + SAME_AS neighbors (up to 2 hops for chains)
OPTIONAL MATCH (e)-[:SAME_AS*1..2]-(other)
WITH e, lbls, collect(DISTINCT other) AS others
WITH e, lbls, [e] + others AS equivs
// Count all connections from any equivalent (exclude SAME_AS links)
UNWIND equivs AS eq
OPTIONAL MATCH (eq)-[r]-(connected) WHERE type(r) <> 'SAME_AS'
WITH e, lbls, equivs,
     coalesce(sum(
       CASE
         WHEN r IS NULL THEN 0
         WHEN type(r) = 'CONTRATOU' THEN coalesce(r.contract_count, 1)
         WHEN type(r) = 'ADJUDICOU_A' THEN coalesce(r.process_count, 1)
         ELSE 1
       END
     ), 0) AS connection_count,
     collect(DISTINCT
       CASE
         WHEN type(r) IN ['CONTRATOU', 'ADJUDICOU_A'] THEN coalesce(r.source, 'secop_integrado')
         WHEN connected:Contract THEN coalesce(connected.source, 'secop_integrado')
         WHEN connected:Sanction THEN coalesce(connected.source, 'secop_sanctions')
         WHEN connected:Election THEN coalesce(connected.source, 'cuentas_claras_income_2019')
         WHEN connected:Health THEN coalesce(connected.source, 'health_providers')
         WHEN connected:Finance THEN coalesce(connected.source, 'sgr_expense_execution')
         WHEN connected:Embargo THEN coalesce(connected.source, 'secop_sanctions')
         WHEN connected:Education THEN coalesce(connected.source, 'higher_ed_enrollment')
         WHEN connected:Convenio THEN coalesce(connected.source, 'sgr_projects')
         WHEN connected:LaborStats THEN coalesce(connected.source, 'sigep_public_servants')
         WHEN connected:PublicOffice THEN coalesce(connected.source, 'sigep_public_servants')
         WHEN connected:Bid THEN coalesce(connected.source, 'secop_ii_processes')
         WHEN connected:DeclaredAsset THEN coalesce(connected.source, 'asset_disclosures')
         WHEN connected:Amendment THEN coalesce(connected.source, 'sgr_expense_execution')
         WHEN connected:Company THEN coalesce(connected.source, 'secop_integrado')
         WHEN connected:Person THEN coalesce(connected.source, 'sigep_public_servants')
         ELSE 'secop_integrado'
       END
     ) AS source_list
// Contract volume across all equivalents
UNWIND equivs AS eq2
OPTIONAL MATCH (eq2)-[:VENCEU]->(c:Contract)
WITH e, lbls, equivs, connection_count, source_list,
     COALESCE(sum(c.value), 0) AS contract_node_volume
UNWIND equivs AS eq2r
OPTIONAL MATCH ()-[award:CONTRATOU]->(eq2r)
WITH e, lbls, equivs, connection_count, source_list,
     contract_node_volume + COALESCE(sum(award.total_value), 0) AS contract_volume
// Donation volume across all equivalents
UNWIND equivs AS eq3
OPTIONAL MATCH (eq3)-[:DOOU]->(d)
WITH e, lbls, equivs, connection_count, source_list, contract_volume,
     COALESCE(sum(d.valor), 0) AS donation_volume
// Debt/loan volume across all equivalents
UNWIND equivs AS eq4
OPTIONAL MATCH (eq4)-[:RECEBEU_EMPRESTIMO|DEVE]->(f:Finance)
WITH e, lbls, connection_count, source_list, contract_volume, donation_volume,
     COALESCE(sum(f.value), 0) AS debt_loan_volume
RETURN
  elementId(e) AS entity_id,
  lbls AS entity_labels,
  connection_count,
  size(source_list) AS source_count,
  contract_volume + donation_volume + debt_loan_volume AS financial_volume,
  e.cnae_principal AS cnae_principal,
  e.role AS role
