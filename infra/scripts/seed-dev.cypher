// CO-ACC Colombia demo seed
// Small synthetic fixture for local exploration

MATCH (n) DETACH DELETE n;

CREATE (p1:Person {
  document_id: '1032456789',
  cedula: '1032456789',
  name: 'ANA MARIA TORRES',
  is_pep: true,
  role: 'ALCALDESA'
});

CREATE (p2:Person {
  document_id: '79865412',
  cedula: '79865412',
  name: 'JUAN CAMILO RESTREPO',
  is_pep: false
});

CREATE (c1:Company {
  document_id: '901234567',
  nit: '901234567',
  name: 'CONSORCIO ANDINO SAS',
  razao_social: 'CONSORCIO ANDINO SAS',
  sector: 'infrastructure',
  city: 'Bogota D.C.',
  country: 'CO',
  source: 'secop_integrado'
});

CREATE (c2:Company {
  document_id: '800765432',
  nit: '800765432',
  name: 'SALUD ABIERTA SAS',
  razao_social: 'SALUD ABIERTA SAS',
  sector: 'health',
  city: 'Medellin',
  country: 'CO',
  source: 'secop_integrado'
});

CREATE (c3:Company {
  document_id: '900123456',
  nit: '900123456',
  name: 'CONSORCIO VIAL DEL NORTE',
  razao_social: 'CONSORCIO VIAL DEL NORTE',
  sector: 'transport',
  city: 'Bogota D.C.',
  country: 'CO',
  source: 'secop_sanctions'
});

CREATE (k1:Contract {
  contract_id: 'CO1.PCCNTR.1001',
  name: 'Interventoria de obra publica',
  object: 'Interventoria de obra publica',
  value: 1500000000.0,
  contracting_org: 'ALCALDIA DE MEDELLIN',
  city: 'Medellin',
  department: 'Antioquia',
  process_id: 'CO1.PCCNTR.5001',
  source: 'secop_integrado'
});

CREATE (k2:Contract {
  contract_id: 'CO1.PCCNTR.1002',
  name: 'Suministro de equipos medicos',
  object: 'Suministro de equipos medicos',
  value: 450000000.0,
  contracting_org: 'GOBERNACION DE CUNDINAMARCA',
  city: 'Bogota D.C.',
  department: 'Cundinamarca',
  process_id: 'CO1.PCCNTR.5002',
  source: 'secop_integrado'
});

CREATE (k3:Contract {
  contract_id: 'ANI-001-2024',
  name: 'Intervencion vial regional',
  object: 'Intervencion vial regional',
  value: 2100000000.0,
  contracting_org: 'AGENCIA NACIONAL DE INFRAESTRUCTURA',
  city: 'Bogota D.C.',
  department: 'Bogota D.C.',
  process_id: 'ANI-PROC-2024-01',
  source: 'secop_integrado'
});

CREATE (s1:Sanction {
  sanction_id: 'secop_ii_CO1.BDOS.5786919_RES-SECOPII-99',
  name: 'Clausula Penal',
  type: 'SECOP_II_SANCTION',
  value: 1080000.0,
  issuing_entity: 'FONDO DE PRESTACIONES ECONOMICAS CESANTIAS Y PENSIONES',
  source: 'secop_sanctions'
});

CREATE (s2:Sanction {
  sanction_id: 'secop_i_ANI-001-2024_RES-2024-001',
  name: 'RES-2024-001',
  type: 'SECOP_I_SANCTION',
  value: 250000000.0,
  issuing_entity: 'AGENCIA NACIONAL DE INFRAESTRUCTURA',
  source: 'secop_sanctions'
});

CREATE (f1:Finance {
  finance_id: 'SGR-001',
  name: 'Proyecto SGR 001',
  value: 980000000.0,
  source: 'sgr_projects'
});

CREATE (p1)-[:SOCIO_DE]->(c1);
CREATE (p2)-[:SOCIO_DE]->(c2);
CREATE (c1)-[:VENCEU {source: 'secop_integrado'}]->(k1);
CREATE (c2)-[:VENCEU {source: 'secop_integrado'}]->(k2);
CREATE (c3)-[:VENCEU {source: 'secop_integrado'}]->(k3);
CREATE (c1)-[:SANCIONADA {source: 'secop_sanctions'}]->(s1);
CREATE (c3)-[:SANCIONADA {source: 'secop_sanctions'}]->(s2);
CREATE (c1)-[:DEVE {source: 'synthetic'}]->(f1);
