CALL {
  MATCH (n) RETURN count(n) AS total_nodes
}
CALL {
  MATCH ()-[r]->() RETURN count(r) AS total_relationships
}
CALL {
  MATCH (p) WHERE (p:Person OR p:Company) RETURN count(p) AS person_count
}
CALL {
  MATCH (c:Company) RETURN count(c) AS company_count
}
CALL {
  MATCH (h:Health) RETURN count(h) AS health_count
}
CALL {
  MATCH (f:Finance) RETURN count(f) AS finance_count
}
CALL {
  MATCH ()-[r:CONTRATOU]->()
  RETURN toInteger(coalesce(sum(coalesce(r.contract_count, 0)), 0)) AS contract_count
}
CALL {
  MATCH (s:Sanction) RETURN count(s) AS sanction_count
}
CALL {
  MATCH (e:Election) RETURN count(e) AS election_count
}
CALL {
  MATCH (a:Amendment) RETURN count(a) AS amendment_count
}
CALL {
  MATCH (e:Education) RETURN count(e) AS education_count
}
CALL {
  MATCH ()-[r:ADJUDICOU_A]->()
  RETURN toInteger(coalesce(sum(coalesce(r.process_count, 0)), 0)) AS bid_count
}
CALL {
  MATCH (sd:SourceDocument) RETURN count(sd) AS source_document_count
}
CALL {
  MATCH (ir:IngestionRun) RETURN count(ir) AS ingestion_run_count
}
RETURN total_nodes, total_relationships,
       person_count, company_count, health_count,
       finance_count, contract_count, sanction_count,
       election_count, amendment_count,
       education_count, bid_count,
       source_document_count, ingestion_run_count
