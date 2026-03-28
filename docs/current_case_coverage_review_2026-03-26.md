# Current Case Coverage Review

Date: 2026-03-26

## Scope

This pass compares recent or still-relevant public Colombia corruption benchmarks against the current real-data graph, then isolates:

- `caught`: the graph reproduces the public signal directly
- `partial`: the graph contains the actor or part of the signal, but misses the full misconduct pattern
- `missing`: the graph does not yet contain the actor or the required edge family

It also identifies high-confidence non-public leads already surfacing in the graph.

## Public benchmark matrix

### 1. Fundación San José / ICAFT

- Status: `caught`
- Why:
  - current validation route matches `education_control_capture`
  - the graph contains the MEN institution node, ICAFT, the SECOP alias bridge, and Apía interadministrative agreements
  - the investigation dossier is already live
- Current graph evidence:
  - validation `16/16` includes `san_jose_education_control_capture`
  - public metric: `education_alias_count = 1`
  - public metric: `education_procurement_link_count = 2`
  - public metric: `education_procurement_total = COP 872,598,000`
- Solve / next:
  - keep adding only document-backed company-control bridges
  - do not promote family/partner ties unless they close with registry or institutional documents
- Sources:
  - https://www.mineducacion.gov.co/1780/w3-article-426421.html
  - https://www.mineducacion.gov.co/1780/articles-426422_recurso_1.pdf
  - https://caracol.com.co/2026/02/25/jennifer-pedraza-denuncia-que-directivos-de-la-san-jose-tienen-tentaculos-en-una-universidad-fachada/

### 2. Alejandro Ospina Coll, Alcaldía de Pereira

- Status: `caught`
- Why:
  - the graph now matches the Procuraduría bulletin directly
  - the exact person node remains present with document-backed sanctions
  - validation now reproduces the public exposure as `official_case_bulletin_exposure`
- Current graph evidence:
  - exact person node present
  - `document_id = 10136043`
  - `official_case_bulletin_count = 1`
  - `person_sanction_count = 18`
- Remaining gap:
  - the graph still does not close the specific `SUPERVISA_PAGO` or payroll-period edge for the Pereira contract
- Solve / next:
  - keep the bulletin match as proof coverage
  - add more supervision/interventoría sources if we want the exact Pereira operational chain
- Source:
  - https://www.procuraduria.gov.co/Pages/procuraduria-confirmo-sancion-supervisor-contrato-omitio-vigilar-cumplimiento.aspx

### 3. Federico García Arbeláez / SENA / interventoría-NEOGLOBAL style case

- Status: `caught`
- Why:
  - the graph now matches the Procuraduría bulletin directly
  - the bulletin placeholder closes into a very small exact-name natural-person contractor cluster
  - validation now reproduces the public exposure as `official_case_bulletin_exposure`
- Current graph evidence:
  - `official_case_bulletin_count = 1`
  - `linked_supplier_company_count = 2`
  - `supplier_contract_count = 49`
  - `supplier_contract_value = COP 5.53B`
  - observed signal also includes `shared_officer_supplier_network`
- Remaining gap:
  - the graph still does not close a formal `interventoría` role edge from structured procurement data
  - the SENA improper-payment motif is still represented by the official bulletin, not by a separate contract-role dataset
- Solve / next:
  - parse more interventor/supervisor role sources if we want the operational chain, not just the validated public case
- Source:
  - https://www.procuraduria.gov.co/Pages/cargos-a-contratista-del-sena-por-extralimitacion-en-funciones.aspx

### 4. Jaime José Garcés García / Aguas del Cesar

- Status: `caught`
- Why:
  - the graph now contains the official bulletin as a structured case record
  - validation reproduces the public case as `official_case_bulletin_record`
- Current graph evidence:
  - `official_case_bulletin_count = 1`
  - official bulletin title is present in the live validation route
- Remaining gap:
  - there is still no stronger structured procurement/person-role bridge behind the case
- Solve / next:
  - ingest more territorial supervision / payroll / disciplinary layers around Aguas del Cesar if we want operational edges beyond the bulletin itself
- Source:
  - https://www.procuraduria.gov.co/Pages/cargos-funcionario-aguas-cesar-presuntas-irregularidades-supervision-contrato-interventoria.aspx

### 5. FONDECUN

- Status: `caught`
- Why:
  - validation route already matches `budget_execution_discrepancy` and `public_official_supplier_overlap`
  - the national graph widened the exposure instead of weakening it
- Current graph evidence:
  - validation matched
  - current company snapshot shows `815` contracts
  - `official_officer_count = 2`
  - `execution_gap_contract_count = 1`
  - `interadmin_agreement_count = 14`
- Sources:
  - https://www.integracionsocial.gov.co/index.php/noticias/116-otras-noticias/5795-contraloria-distrital-reconoce-a-integracion-social-los-avances-en-la-construccion-del-centro-dia-campo-verde
  - https://www.elespectador.com/bogota/bosa-espera-recuperar-su-elefante-blanco/
  - https://www.procuraduria.gov.co/Pages/procuraduria-alerta-ejecucion-3-billones-de-19-contrataderos.aspx

### 6. EGOBUS / COOBUS

- Status: `caught`
- Why:
  - both exact companies remain in the graph and in the validation route
  - the graph catches the public sanction footprint directly
- Sources:
  - https://bogota.gov.co/mi-ciudad/movilidad/alcaldia-penalosa-empieza-pagarles-propietarios-egobus-y-coobus
  - https://www.transmilenio.gov.co/files/6c7a31a0-0df6-4750-98f5-e1225e1b9583/0e64b507-6aa5-4be4-b90a-f1d3b050fe62/Informe%20de%20gestion%202016-2019.pdf

### 7. SUMINISTROS MAYBE S.A.S.

- Status: `caught`
- Why:
  - the graph still matches both `sanctioned_supplier_record` and `sanctioned_still_receiving`
- Sources:
  - https://sedeelectronica.sic.gov.co/sites/default/files/estados/032022/RELATOR%C3%8DA%20RESOLUCI%C3%93N%2012992%20DEL%2010%20DE%20MAYO%20DE%202019%20-%20SUMINISTROS.pdf
  - https://www.pulzo.com/economia/carrusel-contratacion-estatal-PP476289

### 8. Vivian del Rosario Moreno Pérez

- Status: `caught`
- Why:
  - validation route still matches `candidate_supplier_overlap`
  - the graph keeps the political-exposure + supplier path
- Sources:
  - https://www.procuraduria.gov.co/Pages/cargos-siete-exediles-localidad-bogota-presuntas-irregularidades-conformacion-terna-alcalde-local.aspx

### 9. UNGRD public scandal actors from Fiscalía bulletins

- Names checked:
  - `Olmedo de Jesus Lopez Martinez`
  - `Sneyder Augusto Pinilla Alvarez`
  - `Carlos Ramon Gonzalez Merchan`
  - `Luis Carlos Barreto Gantiva`
  - `Sandra Liliana Ortiz Nova`
  - `Maria Alejandra Benavides Soto`
- Status: `caught`
- Why:
  - the curated official-bulletin layer is now ingested and live
  - all six actors validate as `official_case_bulletin_record` or stronger
  - four of the six already pick up extra live graph exposure beyond the bulletin itself
- Current graph evidence:
  - validation `16/16` includes all six UNGRD actors
  - `Olmedo de Jesus Lopez Martinez` -> `official_case_bulletin_exposure`, `sanctioned_person_exposure_stack`
  - `Sneyder Augusto Pinilla Alvarez` -> `official_case_bulletin_exposure`, `shared_officer_supplier_network`
  - `Luis Carlos Barreto Gantiva` -> `official_case_bulletin_exposure`
  - `Maria Alejandra Benavides Soto` -> `official_case_bulletin_exposure`
  - `Carlos Ramon Gonzalez Merchan` and `Sandra Liliana Ortiz Nova` are currently bulletin-record only
- Solve / next:
  - keep expanding exact official bulletins before trying press-only criminal cases
  - improve natural-person normalization around contractor `Company` nodes so bulletin actors close into cleaner procurement trails
- Sources:
  - https://www.fiscalia.gov.co/colombia/noticias/nueva-imputacion-de-cargos-contra-exdirectivos-de-la-ungrd-olmedo-lopez-y-sneyder-pinilla-por-direccionamiento-irregular-de-la-contratacion-en-la-entidad/
  - https://www.fiscalia.gov.co/colombia/lucha-contra-corrupcion/acusado-exdirector-del-departamento-administrativo-de-presidencia-carlos-ramon-gonzalez-merchan-por-presuntamente-direccionar-dadivas-en-favor-de-congresistas-con-recursos-de-la-ungrd
  - https://www.fiscalia.gov.co/colombia/noticias/exdirector-de-conocimiento-de-la-ungrd-luis-carlos-barreto-gantiva-sera-condenado-mediante-preacuerdo-por-direccionamiento-de-contratos-en-la-entidad/
  - https://www.fiscalia.gov.co/colombia/noticias/acusada-exconsejera-presidencial-para-las-regiones-sandra-ortiz-por-presuntamente-trasladar-dadivas-relacionadas-con-actos-de-corrupcion-en-la-ungrd/
  - https://www.fiscalia.gov.co/colombia/noticias/imputada-exasesora-del-ministerio-de-hacienda-por-su-presunta-intervencion-en-el-direccionamiento-de-contratos-en-la-ungrd-en-favor-de-congresistas/

## Coverage summary

- `caught`: San José / ICAFT, FONDECUN, EGOBUS, COOBUS, SUMINISTROS MAYBE, Vivian Moreno, Alejandro Ospina Coll, Federico García Arbeláez, Jaime José Garcés García, Olmedo López, Sneyder Pinilla, Carlos Ramón González, Luis Carlos Barreto, Sandra Ortiz, María Alejandra Benavides
- `missing`: no major benchmark from this pass remains missing

## What the misses are telling us

The graph is currently strongest when a case has at least one of these:

- exact `NIT` or document number in procurement / sanction / institutional datasets
- direct supplier exposure in SECOP or SECOP I historical
- public-servant overlap in SIGEP
- sanction trace in SIRI / fiscal responsibility / PACO
- institution-control evidence with formal documents

The graph still misses or under-catches cases when the public record is mostly:

- criminal-case reporting without structured IDs
- local supervision cases without open identifiers
- bulletin text with names but no standardized document number
- interventions routed through PDFs or document archives we have not yet parsed

## High-confidence new leads not yet tied to public benchmark cases

These are not presented as proven corruption. They are the strongest non-validated leads currently surfacing from the live graph because they stack multiple hard signals at once.

### 1. TEATRO SUA (`8000947557`)

- Signal stack:
  - sanction window overlap
  - sanctions
  - official overlap
  - sensitive-position overlap
- Current metrics:
  - `94` contracts
  - `1` sanction
  - `1` official overlap
  - `1` sensitive overlap

### 2. SADY ALBERTO SANCHEZ HERNANDEZ (`88160225`)

- Signal stack:
  - sanction window overlap
  - sanctions
  - official overlap
  - sensitive-position overlap
- Current metrics:
  - `22` contracts
  - `8` contracts in sanction window
  - `1` sanction
  - `1` official overlap

### 3. LILIA MARINA CASTELLANOS JAIMES (`63505871`)

- Signal stack:
  - sanction window overlap
  - sanctions
  - official overlap
  - sensitive-position overlap
- Current metrics:
  - `20` contracts
  - `6` contracts in sanction window
  - `1` sanction
  - `1` official overlap

### 4. VIGIAS DE COLOMBIA SRL LTDA (`860050247`)

- Signal stack:
  - sanction window overlap
  - sanctions
  - interadministrative stacking
- Current metrics:
  - `104` contracts
  - `12` contracts in sanction window
  - `4` sanctions
  - `1` interadministrative agreement

### 5. MAQUINAS PROCESOS Y LOGITICA MP&L SAS (`900024808`)

- Signal stack:
  - sanction window overlap
  - sanctions
  - interadministrative stacking
- Current metrics:
  - `254` contracts
  - `18` contracts in sanction window
  - `2` sanctions
  - `1` interadministrative agreement

### 6. EXOGENA LTDA (`830052968`)

- Signal stack:
  - sanction window overlap
  - sanctions
  - interadministrative stacking
- Current metrics:
  - `236` contracts
  - `47` contracts in sanction window
  - `1` sanction
  - `6` interadministrative agreements

## Practical next fixes

### Highest-value data additions

1. Better natural-person normalization  
   The graph still splits some people between `Person` and natural-person `Company` forms.

2. Supervision/interventoría enrichment  
   This is the main blocker for converting `partial` supervision cases into `caught` cases.

3. Fresh people snapshot rebuild  
   The enlarged graph now contains more person-side evidence than the current materialized people queue exposes.

### Precision rule for non-public leads

Only elevate a lead to investigation when at least two hard families stack:

- sanction window + sanctions
- sanctions + official overlap
- official overlap + execution gap
- interadministrative stack + sanctions
- execution gap + commitment gap

That is the current best route to high-confidence non-public leads without pretending they are already proven.
