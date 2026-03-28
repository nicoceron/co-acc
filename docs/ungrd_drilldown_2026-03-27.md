# UNGRD Drilldown

Date: 2026-03-27

## Scope

This memo drills into the six March 26, 2026 Fiscalía UNGRD bulletin actors that are now matched by the live graph, and compares that graph evidence against the ways journalists and civil-society investigators have been building the case in public.

## Current live status

- Live validation: `16/16 matched`
- New UNGRD bulletin cases matched:
  - `Olmedo de Jesus Lopez Martinez`
  - `Sneyder Augusto Pinilla Alvarez`
  - `Carlos Ramon Gonzalez Merchan`
  - `Luis Carlos Barreto Gantiva`
  - `Sandra Liliana Ortiz Nova`
  - `Maria Alejandra Benavides Soto`

## What the graph is actually catching

### 1. Olmedo de Jesus Lopez Martinez

- Bulletin match: `yes`
- Extra graph bridge:
  - `POSSIBLY_SAME_AS` -> `Person(document_id=98538265)`
  - match reason: `unique_exact_full_name_person`
- Current procurement overlap: `none`
- Interpretation:
  - this is useful as an identity lead only
  - no procurement-side evidence should be promoted from this bridge alone

### 2. Sneyder Augusto Pinilla Alvarez

- Bulletin match: `yes`
- Extra graph bridge:
  - `POSSIBLY_SAME_AS` -> natural-person `Company(document_id=1101200853)`
  - `POSSIBLY_SAME_AS` -> natural-person `Company(document_id=11012008531)`
  - match reason: `exact_full_name_company_form_cluster`
- Current procurement overlap on those company-form nodes:
  - `9` contracts total in the live graph
  - top buyers include `INVIAS`, `ALCALDIA MUNICIPIO DE GIRON SANTANDER`, and `ALCALDIA MUNICIPIO DE SABANA DE TORRES`
  - evidence refs include `CO1.PCCNTR.2391534`, `819-2021`, `CO1.PCCNTR.3454219`, `0756-2022`
- Interpretation:
  - this is still a name-based contractor-form bridge
  - it is not enough to state that these contracts belong to the same Sneyder Pinilla from the UNGRD case without an official ID source

### 3. Carlos Ramon Gonzalez Merchan

- Bulletin match: `yes`
- Extra graph bridge: `none`
- Interpretation:
  - official-case record only for now

### 4. Luis Carlos Barreto Gantiva

- Bulletin match: `yes`
- Extra graph bridge:
  - `POSSIBLY_SAME_AS` -> natural-person `Company(document_id=11257105)`
  - match reason: `unique_exact_full_name_company_form`
- Current procurement overlap on that company-form node:
  - `15` contracts
  - `COP 399,185,566`
  - buyers include `ALCALDIA MUNICIPAL FUSAGASUGA`, `MUNICIPIO DE SOACHA`, `MUNICIPIO DE CAQUEZA`, `CONCEJO MUNICIPAL DE FUQUENE`
- Interpretation:
  - again, this is a real exact-name contractor cluster but not an identity-proven bridge yet
  - useful for drill-down, not safe yet as a promoted corruption pattern

### 5. Sandra Liliana Ortiz Nova

- Bulletin match: `yes`
- Extra graph bridge: `none`
- Interpretation:
  - official-case record only for now

### 6. Maria Alejandra Benavides Soto

- Bulletin match: `yes`
- Extra graph bridge:
  - `POSSIBLY_SAME_AS` -> natural-person `Company(document_id=1020785233)`
  - match reason: `unique_exact_full_name_company_form`
- Current procurement overlap on that company-form node:
  - aggregated summary edge to `MINISTERIO DE HACIENDA Y CREDITO PUBLICO`
  - `3` contracts
  - `COP 143,227,700`
  - evidence refs include `CO1.PCCNTR.1451509`, `3.151-2020`, `CO1.PCCNTR.2163423`, `3.006-2021`, `CO1.PCCNTR.3304969`
- Interpretation:
  - this is the strongest contextual overlap in the current drill because the buyer aligns with the institution named in the Fiscalía bulletin
  - even here, the bridge is still name-based and not identity-proven from an official ID document

## What should not be promoted

These four procurement-side overlaps are not yet safe as public corruption findings:

- `Sneyder Augusto Pinilla Alvarez`
- `Luis Carlos Barreto Gantiva`
- `Maria Alejandra Benavides Soto`
- `Olmedo de Jesus Lopez Martinez`

Reason:

- the current bridge comes from exact-name matching into natural-person contractor `Company` nodes or a same-name `Person` node
- the Fiscalía bulletins do not publish the corresponding document numbers
- without an official identity document, these remain drill-down clues, not proven identity joins

## How journalists and investigators have been building the UNGRD case

### 1. Official judicial material first

The most structured public narrative is coming from official bulletins and hearings:

- Fiscalía bulletins and accusation/preacuerdo notes
- public hearing narratives referenced in later reports
- Corte Suprema bulletins for the congressional branch

This matters because the civil-society and journalism pieces are repeatedly leaning on those official proceedings to define the actor network.

### 2. SECOP alone was not enough

Transparencia por Colombia explicitly states that the ordinary SECOP search path is inadequate for the UNGRD case because the main published information was left in a poor-quality dataset that does not expose key identifiers cleanly.

Relevant source:

- “SECOP II – Procesos de Contratación” was described as inadequate because it does not precisely expose contractor name, definitive contract value, signature date, and related identifiers.
- They explicitly contrast it with `SECOP II – Contratos Electrónicos`.

### 3. Curated contract-document collections were critical

Transparencia / Monitor Ciudadano built a dedicated public case page that links:

- order of supply documents
- ratification documents
- direct SECOP links
- contract objects and values

This is much closer to an investigative dataset than a normal news article.

### 4. Right-to-information requests were part of the workflow

Transparencia reports that it filed a `derecho de petición` and then a `recurso de insistencia` to the UNGRD, because the published contracting information was still incomplete for meaningful oversight.

### 5. Real-control / beneficial-control tracing mattered

Transparencia and Monitor Ciudadano do not stop at the nominal contractor. They explicitly describe:

- `Impoamericana Roger SAS`
- `Luket SAS`
- `Brand SAS`

as three legal vehicles whose real controller was the already convicted businessman López Rosero.

That means the investigation method was:

- official case narrative
- procurement document retrieval
- company-control tracing
- then contract aggregation across the controlled legal vehicles

### 6. Campaign-finance and congressional branches matter

The Monitor Ciudadano / Transparencia analysis points to:

- the Comisión Interparlamentaria de Crédito Público branch
- family-campaign hypotheses around `Iván Name` and `Andrés Calle`

That suggests campaign-finance and family-political links are not side stories; they are part of the case architecture.

## Datasets and document sources we should add next

These are the strongest real targets surfaced by the journalism/civil-society research:

1. `Monitor Ciudadano / contratos públicos caso UNGRD`
   - public case page with structured contract documents and SECOP links
   - not a Socrata dataset, but a high-value curated evidence source
   - URL: `https://www.monitorciudadano.co/accion-publica-anticorrupcion/contratos-publicos-caso-ungrd/`

2. `UNGRD transparencia contratos`
   - yearly public contract repositories exposed from the UNGRD transparency page
   - includes links out to public document folders
   - URL: `https://portal.gestiondelriesgo.gov.co/Paginas/Transparencia-Contratos.aspx`

3. `SECOP II – Contratos Electrónicos`
   - for this case, stronger than relying only on the weaker `Procesos de Contratación` publication path

4. `Corte Suprema bulletin layer`
   - needed for the Commission branch and the congressperson side of the case

5. `Cuentas Claras family-campaign branch`
   - needed if we want to reproduce the alleged political-benefit side tied to family campaigns

6. `Company-control / chamber records around Lopez Rosero vehicles`
   - specifically to operationalize the `Impoamericana Roger SAS`, `Luket SAS`, `Brand SAS` cluster and any additional controlled entities

## Local evidence bundle created in this pass

Saved bundle:

- [ungrd-public-evidence-2026-03-27](/Users/ceron/Developer/corruption/audit-results/investigations/ungrd-public-evidence-2026-03-27)
- [manifest.json](/Users/ceron/Developer/corruption/audit-results/investigations/ungrd-public-evidence-2026-03-27/manifest.json)

Collector:

- [collect_ungrd_public_evidence.py](/Users/ceron/Developer/corruption/scripts/collect_ungrd_public_evidence.py)

Collected from public pages:

- `24` SECOP Community links from the Monitor Ciudadano UNGRD case page
- `32` old `contratos.gov.co` links from the same page
- `6` Google Drive folders from the UNGRD transparency contracts portal
- `4` Monitor Ciudadano UNGRD PDFs downloaded locally

Downloaded Monitor PDFs include:

- `1010501-21-01-07.pdf`
- `RATIFICACION-9677-1306-2023.pdf`
- `INSTRUCCION-9677-1461-2023.pdf`
- `PERFECCIONAMIENTO-9677-1461-2023.pdf`

## Immediate next drill actions

1. Pull the public contract-document inventory from the Monitor Ciudadano UNGRD page.
2. Resolve the SECOP evidence refs already visible in the graph for `Maria Alejandra Benavides Soto`, `Luis Carlos Barreto Gantiva`, and `Sneyder Augusto Pinilla Alvarez`.
3. Keep those matches marked as `probable` until an official document number or case annex closes the identity.
4. Add a dedicated `UNGRD evidence` layer for:
   - order numbers
   - ratification numbers
   - SECOP community links
   - public judicial bulletins
   - related political/campaign nodes where official sources exist

## Sources

- Fiscalía UNGRD bulletins:
  - https://www.fiscalia.gov.co/colombia/noticias/nueva-imputacion-de-cargos-contra-exdirectivos-de-la-ungrd-olmedo-lopez-y-sneyder-pinilla-por-direccionamiento-irregular-de-la-contratacion-en-la-entidad/
  - https://www.fiscalia.gov.co/colombia/lucha-contra-corrupcion/acusado-exdirector-del-departamento-administrativo-de-presidencia-carlos-ramon-gonzalez-merchan-por-presuntamente-direccionar-dadivas-en-favor-de-congresistas-con-recursos-de-la-ungrd
  - https://www.fiscalia.gov.co/colombia/noticias/exdirector-de-conocimiento-de-la-ungrd-luis-carlos-barreto-gantiva-sera-condenado-mediante-preacuerdo-por-direccionamiento-de-contratos-en-la-entidad/
  - https://www.fiscalia.gov.co/colombia/noticias/acusada-exconsejera-presidencial-para-las-regiones-sandra-ortiz-por-presuntamente-trasladar-dadivas-relacionadas-con-actos-de-corrupcion-en-la-ungrd/
  - https://www.fiscalia.gov.co/colombia/noticias/imputada-exasesora-del-ministerio-de-hacienda-por-su-presunta-intervencion-en-el-direccionamiento-de-contratos-en-la-ungrd-en-favor-de-congresistas/
- Transparencia / Monitor Ciudadano:
  - https://www.monitorciudadano.co/accion-publica-anticorrupcion/contratos-publicos-caso-ungrd/
  - https://transparenciacolombia.org.co/algo-tiene-que-pasar-quinto-informe-escandalo-de-la-ungrd/
  - https://transparenciacolombia.org.co/wp-content/uploads/2025/10/Algo-tiene-que-pasar_Quinto-informe-UNGRD_Transparencia-por-Colombia.pdf
  - https://www.monitorciudadano.co/documentos/hc-informes/2025/Documento_Enablers_GACC.pdf
- UNGRD transparency portal:
  - https://portal.gestiondelriesgo.gov.co/Paginas/Transparencia-Contratos.aspx
