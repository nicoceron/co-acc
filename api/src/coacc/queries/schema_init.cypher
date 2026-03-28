// CO-ACC Neo4j Schema — Constraints and Indexes
// Applied on database initialization

// ── Uniqueness Constraints ──────────────────────────────
CREATE CONSTRAINT person_document_id_unique IF NOT EXISTS
  FOR (p:Person) REQUIRE p.document_id IS UNIQUE;

CREATE CONSTRAINT company_document_id_unique IF NOT EXISTS
  FOR (c:Company) REQUIRE c.document_id IS UNIQUE;

CREATE CONSTRAINT contract_contract_id_unique IF NOT EXISTS
  FOR (c:Contract) REQUIRE c.contract_id IS UNIQUE;

CREATE CONSTRAINT bid_id_unique IF NOT EXISTS
  FOR (b:Bid) REQUIRE b.bid_id IS UNIQUE;

CREATE CONSTRAINT sanction_sanction_id_unique IF NOT EXISTS
  FOR (s:Sanction) REQUIRE s.sanction_id IS UNIQUE;

CREATE CONSTRAINT election_id_unique IF NOT EXISTS
  FOR (e:Election) REQUIRE e.election_id IS UNIQUE;

CREATE CONSTRAINT public_office_id_unique IF NOT EXISTS
  FOR (po:PublicOffice) REQUIRE po.office_id IS UNIQUE;

CREATE CONSTRAINT investigation_id_unique IF NOT EXISTS
  FOR (i:Investigation) REQUIRE i.id IS UNIQUE;

CREATE CONSTRAINT amendment_id_unique IF NOT EXISTS
  FOR (a:Amendment) REQUIRE a.amendment_id IS UNIQUE;

CREATE CONSTRAINT finance_id_unique IF NOT EXISTS
  FOR (f:Finance) REQUIRE f.finance_id IS UNIQUE;

CREATE CONSTRAINT declared_asset_id_unique IF NOT EXISTS
  FOR (d:DeclaredAsset) REQUIRE d.asset_id IS UNIQUE;

CREATE CONSTRAINT finding_id_unique IF NOT EXISTS
  FOR (f:Finding) REQUIRE f.finding_id IS UNIQUE;

CREATE CONSTRAINT education_school_id_unique IF NOT EXISTS
  FOR (e:Education) REQUIRE e.school_id IS UNIQUE;

CREATE CONSTRAINT source_document_id_unique IF NOT EXISTS
  FOR (s:SourceDocument) REQUIRE s.doc_id IS UNIQUE;

CREATE CONSTRAINT ingestion_run_id_unique IF NOT EXISTS
  FOR (r:IngestionRun) REQUIRE r.run_id IS UNIQUE;

// ── Indexes ─────────────────────────────────────────────
CREATE INDEX person_name IF NOT EXISTS
  FOR (p:Person) ON (p.name);

CREATE INDEX company_razon_social IF NOT EXISTS
  FOR (c:Company) ON (c.razon_social);

CREATE INDEX company_document_id IF NOT EXISTS
  FOR (c:Company) ON (c.document_id);

CREATE INDEX company_education_institution_code IF NOT EXISTS
  FOR (c:Company) ON (c.education_institution_code);

CREATE INDEX contract_value IF NOT EXISTS
  FOR (c:Contract) ON (c.value);

CREATE INDEX contract_object IF NOT EXISTS
  FOR (c:Contract) ON (c.object);

CREATE INDEX bid_publication_date IF NOT EXISTS
  FOR (b:Bid) ON (b.publication_date);

CREATE INDEX sanction_type IF NOT EXISTS
  FOR (s:Sanction) ON (s.type);

CREATE INDEX procurement_summary_id IF NOT EXISTS
  FOR ()-[r:CONTRATOU]-() ON (r.summary_id);

CREATE INDEX procurement_process_summary_id IF NOT EXISTS
  FOR ()-[r:ADJUDICOU_A]-() ON (r.summary_id);

CREATE INDEX sanction_date_start IF NOT EXISTS
  FOR (s:Sanction) ON (s.date_start);

CREATE INDEX finding_type IF NOT EXISTS
  FOR (f:Finding) ON (f.type);

CREATE INDEX finding_radicado IF NOT EXISTS
  FOR (f:Finding) ON (f.radicado);

// ── Finance Indexes ───────────────────────────────────
CREATE INDEX finance_type IF NOT EXISTS
  FOR (f:Finance) ON (f.type);

CREATE INDEX finance_value IF NOT EXISTS
  FOR (f:Finance) ON (f.value);

CREATE INDEX finance_date IF NOT EXISTS
  FOR (f:Finance) ON (f.date);

CREATE INDEX finance_source IF NOT EXISTS
  FOR (f:Finance) ON (f.source);

// ── Health Indexes ────────────────────────────────────
CREATE INDEX health_name IF NOT EXISTS
  FOR (h:Health) ON (h.name);

// ── Education Indexes ───────────────────────────────────
CREATE INDEX education_name IF NOT EXISTS
  FOR (e:Education) ON (e.name);

// ── Person Servidor ID Index ────────────────────────────
CREATE INDEX person_servidor_id IF NOT EXISTS
  FOR (p:Person) ON (p.servidor_id);

CREATE INDEX person_document_id IF NOT EXISTS
  FOR (p:Person) ON (p.document_id);

// ── PublicOffice Indexes ────────────────────────────────
CREATE INDEX public_office_org IF NOT EXISTS
  FOR (po:PublicOffice) ON (po.org);

CREATE INDEX source_document_source_id IF NOT EXISTS
  FOR (s:SourceDocument) ON (s.source_id);

CREATE INDEX ingestion_run_source_id IF NOT EXISTS
  FOR (r:IngestionRun) ON (r.source_id);

CREATE INDEX ingestion_run_status IF NOT EXISTS
  FOR (r:IngestionRun) ON (r.status);

CREATE INDEX ingestion_run_started_at IF NOT EXISTS
  FOR (r:IngestionRun) ON (r.started_at);

// ── Fulltext Search Index ───────────────────────────────
CREATE FULLTEXT INDEX entity_search IF NOT EXISTS
  FOR (n:Person|Company|Contract|Bid|Sanction|Finding|Election|Amendment|Finance|DeclaredAsset|Health|Education|PublicOffice|SourceDocument)
  ON EACH [n.name, n.razon_social, n.document_id, n.nit, n.cedula, n.numero_documento, n.object, n.contracting_org, n.infraction, n.org, n.function, n.subject, n.text, n.topic, n.url, n.reference, n.bid_id, n.asset_id, n.office_id, n.finance_id, n.election_id, n.finding_id, n.radicado, n.process_name, n.description];

// ── User Constraints ────────────────────────────────────
CREATE CONSTRAINT user_email_unique IF NOT EXISTS
  FOR (u:User) REQUIRE u.email IS UNIQUE;
