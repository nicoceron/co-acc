# Colombia Connection Opportunities

Date: 2026-03-14

This report compiles the Colombia data that is currently live in the repo and maps how it can already be connected to find corruption-style signals.

## Live Snapshot

- Sources implemented in repo: `30`
- Sources live-loaded in Neo4j: `23`
- Blocked external sources: `1`
- Nodes: `4,749,820`
- Relationships: `8,410,725`

### Main live node families

- `Company`: `3,138,975`
- `Finance`: `496,600`
- `Person`: `410,274`
- `DeclaredAsset`: `328,799`
- `Election`: `121,321`
- `Convenio`: `97,118`
- `Health`: `76,395`
- `Sanction`: `55,575`
- `PublicOffice`: `24,737`

### Main live relationship families

- `CONTRATOU`: `6,876,732`
- `DECLAROU_FINANCA`: `329,799`
- `DECLAROU_BEM`: `328,799`
- `DOOU`: `182,356`
- `FORNECEU`: `166,669`
- `ADJUDICOU_A`: `149,820`
- `CANDIDATO_EM`: `121,321`
- `ADMINISTRA`: `98,305`
- `OPERA_UNIDADE`: `76,395`
- `SANCIONADA`: `55,592`
- `RECEBEU_SALARIO`: `24,737`

## Loaded Sources That Actually Matter For Detection

- `asset_disclosures`
- `conflict_disclosures`
- `cuentas_claras_income_2019`
- `health_providers`
- `mapa_inversiones_projects`
- `paco_sanctions`
- `pte_sector_commitments`
- `pte_top_contracts`
- `rues_chambers`
- `secop_contract_additions`
- `secop_contract_execution`
- `secop_contract_modifications`
- `secop_ii_contracts`
- `secop_ii_processes`
- `secop_integrado`
- `secop_sanctions`
- `secop_suppliers`
- `sgr_expense_execution`
- `sgr_projects`
- `sigep_public_servants`
- `sigep_sensitive_positions`
- `supersoc_top_companies`

## The Best Join Anchors We Already Have

### Identity joins

- `Person.document_id / cedula`
- `Company.document_id / nit`
- `Company.document_id == Person.document_id`

This same-ID person/company bridge is the single strongest current join in the Colombia graph. It is imperfect and needs review, but it enables many high-yield signals without waiting for a relatives database.

### Procurement joins

- `CONTRATOU.summary_id`
- `ADJUDICOU_A.summary_id`
- `buyer_document_id`
- `supplier_document_id`
- `buyer_name`
- `supplier_name`
- `department`
- `city`

### Money-event joins

- `Finance.type`
- `Finance.value`
- `Finance.date`
- `Convenio.convenio_id`
- `Convenio.department`
- `Convenio.sector`

### Risk joins

- `Company -[:SANCIONADA]-> Sanction`
- `Person -[:DECLAROU_FINANCA]-> Finance(type='CONFLICT_DISCLOSURE')`
- `Person -[:DECLAROU_BEM]-> DeclaredAsset`
- `Company -[:DECLAROU_FINANCA]-> Finance(type='SUPERSOC_TOP_COMPANY')`
- `Company -[:OPERA_UNIDADE]-> Health`

## Corruption Functionality Already Real

These are not ideas. These paths already exist in the live graph and can be scored today.

### 1. Donor-official-vendor loop

Current join:

`Person -> PublicOffice`
`Person -> Election donation`
`Person.document_id == Company.document_id`
`Company <- CONTRATOU`

Live scale:

- public-office holders with donations: `981`
- public-office holders with same-ID supplier contracts: `14,564`
- full office + donation + supplier loop: `981`

Live examples:

- `CARLOS ALFONSO LOPEZ PARRA`
- `RODRIGO ARMANDO CHICUASUQUE FERNANDEZ`
- `NELSON HERNANDO VARGAS COLMENARES`

Interpretation:

This is one of the strongest current person-centric signals. It does not prove corruption, but it does isolate people who sit in public office, move in election-finance space, and also appear as vendors.

### 2. Public-office holder with supplier identity overlap

Current join:

`Person -> PublicOffice`
`Person.document_id == Company.document_id`
`Company <- CONTRATOU`

Live scale:

- `14,564` people

Example names seen in the live graph:

- `CARLOMAN LONDONO LLANO`
- `JOSE FERNANDO MORALES ACUNA`
- `JULIO EDUARDO DELGADO SANCHEZ`

Interpretation:

This is the broadest same-ID incompatibility detector in the graph. It is high yield and also the one that most needs human review, because some overlaps may reflect legitimate independent-contractor or sole-proprietor cases.

### 3. Officeholder with conflict-disclosure and supplier exposure

Current join:

`Person -> PublicOffice`
`Person -> CONFLICT_DISCLOSURE`
`Person.document_id == Company.document_id`
`Company <- CONTRATOU`

Live scale:

- public-office holders with conflict disclosures: `2,463`
- the same office + conflict + supplier pattern is already material enough to score as a separate risk tier

Interpretation:

This is better than plain same-ID matching because the person also has a formal conflict-disclosure trail.

### 4. Company-capacity mismatch

Current join:

`Company <- CONTRATOU`
`Company -> SUPERSOC_TOP_COMPANY`

Live scale:

- companies with Supersociedades financials and public contracts: `655`
- companies with both contracts and sanctions inside that corporate-finance slice: `2`

Live examples:

- `BUREAU VERITAS COLOMBIA LTDA`
- `EXPERTOS SEGURIDAD LTDA`

Interpretation:

This is one of the cleanest current company-side signals. It compares public contract exposure against reported operating revenue and assets.

### 5. Sanctioned contractor

Current join:

`Company -> Sanction`
`Company <- CONTRATOU`

Live scale:

- sanctioned companies that also hold contracts: `875`

Interpretation:

This remains a core baseline detector and should stay in every watchlist.

### 6. Company donor-vendor loop

Current join:

`Company -> Election donation`
`Company <- CONTRATOU`

Live scale:

- company donors that also hold contracts: `642`
- company donors with both sanctions and donation history: `2`

Examples:

- `RCN RADIO S.A.S`
- `BANCO DE OCCIDENTE SA`
- `DAVIVIENDA SA`
- `BANCO BILBAO VIZCAYA ARGENTARIA S.A. (OFICIAL)`

Interpretation:

This is real and useful, but it needs sector baselining. Large banks and media groups will appear here often; they are not automatically suspicious. The rule should rank by recurrence, concentration, and timing, not mere presence.

### 7. Health-operator procurement exposure

Current join:

`Company -> Health`
`Company <- CONTRATOU`

Live scale:

- health operators with procurement exposure: `15,309`
- health operators with sanctions: `10`

Examples:

- `LA PREVISORA S.A. COMPANIA DE SEGUROS`
- `UNIVERSIDAD NACIONAL DE COLOMBIA`
- `CAJA DE COMPENSACION FAMILIAR COMPENSAR`
- `UNIVERSIDAD DE ANTIOQUIA`
- `COLSUBSIDIO`

Sanctioned health-side examples:

- `IPS REGION VIVA SAS`
- `ESE HOSPITAL SAN RAFAEL DE ANDES`
- `SALUD VITAL Y RIESGOS PROFESIONALES IPS EU`

Interpretation:

This is a large and valuable vertical slice. It supports healthcare-sector concentration, sanctioned operator, and vendor-dependence detectors.

### 8. Multi-channel public-money overlap

Current join:

`Company -> SGR_EXPENSE_EXECUTION`
`Company <- CONTRATOU`

Live scale:

- companies present in both SGR expense execution and procurement: `20`

Examples:

- `MINISTERIO DE SALUD`
- `E.S.P. EMPRESA DISTRIBUIDORA DEL PACIFICO S.A.`
- `INSTITUTO COLOMBIANO DE COMERCIO EXTERIOR`
- `DEPARTAMENTO NACIONAL DE PLANEACION`

Interpretation:

This is small but interesting. It identifies organizations that appear in more than one public-money channel. It is not yet the full upstream trigger model, but it is a real overlap surface.

### 9. Project administrator who is also a supplier

Current join:

`Company -> Convenio`
`Company <- CONTRATOU`

Live scale:

- project administrators that also appear as suppliers: `17`

Examples:

- `MINISTERIO DE CIENCIA, TECNOLOGIA E INNOVACION`
- `ECOPETROL SA`
- `MUNICIPIO DE CUMBITARA`
- `DEPARTAMENTO DEL ATLANTICO`

Interpretation:

This is an agency or public-entity self-overlap detector. It is useful for identifying institutions that sit on both the project-administration and supplier sides of public spending.

## Creative Detectors We Can Build Immediately From Current Data

These are not blocked by missing national tax or relatives data. They can be built with the live graph now.

### A. Patrimony vs vendor exposure

Join:

`Person -> DeclaredAsset`
`Person -> PublicOffice`
`Person.document_id == Company.document_id`
`Company <- CONTRATOU`

Detector:

Flag officeholders whose same-ID supplier contract exposure is multiple times higher than declared patrimony or declared finance totals.

Why it matters:

This is the Colombia version of a patrimony-incompatibility detector, and it is available without external data.

### B. Sensitive-position amplification

Join:

`Person -[:RECEBEU_SALARIO {sensitive_position:true}]-> PublicOffice`
`Person.document_id == Company.document_id`
`Company <- CONTRATOU`

Detector:

Take the same-ID supplier loop and raise the risk weight when the public role is explicitly marked as corruption-sensitive in SIGEP.

Why it matters:

The same incompatibility is much more serious when the person sits in a procurement-sensitive or high-discretion post.

### C. Donor-vendor baseline by sector

Join:

`Company -> Election donation`
`Company <- CONTRATOU`
`Company -> SUPERSOC_TOP_COMPANY`

Detector:

Rank donor-vendors by how abnormal their contract volume is relative to their sector and size, not just whether they donated.

Why it matters:

This reduces false positives from large financial institutions and isolates donor-vendors whose state exposure is outsized for their actual scale.

### D. Sanctioned health operator priority queue

Join:

`Company -> Health`
`Company -> Sanction`
`Company <- CONTRATOU`

Detector:

Prioritize sanctioned health operators with recurring contract awards and high public revenue dependence.

Why it matters:

Healthcare is one of the easiest sectors for capture to hide inside service complexity and reimbursement structures.

### E. Public-money channel stacking

Join:

`Company <- CONTRATOU`
`Company -> FORNECEU (SGR expense execution)`
`Company -> ADMINISTRA -> Convenio`
`Company -> OPERA_UNIDADE`

Detector:

Rank companies and entities that appear in multiple public-money channels at once: contracts, SGR execution, project administration, health operation.

Why it matters:

This surfaces organizations that sit at repeated state-spending chokepoints.

### F. Political actor with disclosure flags and vendor income

Join:

`Person -> PublicOffice`
`Person -> Election donation`
`Person -> CONFLICT_DISCLOSURE`
`Person.document_id == Company.document_id`
`Company <- CONTRATOU`

Detector:

Increase risk when the same person has all of:

- a public role
- election-finance activity
- conflict disclosures
- vendor exposure

Why it matters:

This is a stronger version of the donor-official-vendor loop and can be explained clearly on the dashboard.

### G. Supplier concentration by buyer

Join:

`Buyer Company -[CONTRATOU]-> Supplier Company`

Detector:

For each buyer, compute the supplier share of total contract value and flag buyers dominated by a small number of suppliers.

Why it matters:

This works even without family or beneficial-ownership data. Concentration and repetition are often cleaner evidence than kinship.

### H. Municipality or department capture

Join:

`CONTRATOU.department/city`
`Convenio.department`
`Health.department`
`PublicOffice.org / dependency / city-level entities`

Detector:

Score territories where:

- the same suppliers recur
- political actors are present
- health or project operations concentrate
- sanctions or conflict disclosures also appear

Why it matters:

Territory-level capture is often easier to detect than ownership-level capture.

## High-Potential But Weakly Supported Right Now

These need better live backfill or better entity normalization before they become strong.

### Budget-chain leakage

The repo now has code paths for:

- offers
- budget commitments
- CDP requests
- invoices
- execution locations

But these auxiliary SECOP layers are not yet materially visible in the live graph. In the current Neo4j data:

- `FORNECEU_LICITACAO` is not present
- invoice/commitment execution properties are not materially visible on `CONTRATOU`

Meaning:

The budget-chain detector is conceptually right, but it is not the strongest current live surface until those feeds are backfilled.

### PTE trigger layer

`PTE_TOP_CONTRACT` and `PTE_SECTOR_COMMITMENT` are loaded, but they do not yet align cleanly enough with procurement buyers/suppliers to drive a strong `funds arrive -> awards follow` alert model.

Meaning:

Keep the detector family, but do not overstate it yet.

### RUES

`rues_chambers` is loaded, but that public chamber directory is too thin to act as a true corporate-control layer.

Meaning:

RUES/RUP remains important, but the current loaded chamber slice is identity context, not full ownership or legal-representative intelligence.

## What The Current Data Can Honestly Prove

### Confirmed facts

- public contracts
- award recurrence
- public-office records
- election donations and candidacies
- sanctions
- health-operation status
- SGR expense-execution presence
- asset and conflict disclosures
- company financial scale for a subset of companies

### Strong signals

- same-ID officeholder/vendor overlap
- donor-official-vendor loops
- company-capacity mismatch
- donor-vendor overlap
- sanctioned operator/contractor overlap
- project administrator also acting as supplier
- multi-channel public-money overlap

### Weak or review-heavy signals

- same-ID person/company links when the person may really be an independent contractor
- territorial funding-to-award timing without cleaner upstream IDs
- any inference that sounds like kinship or beneficial ownership

## Highest-Value Next Detector Queue Using Only Current Data

1. `patrimony_vs_contract_exposure`
2. `sensitive_position_vendor_loop`
3. `company_donor_vendor_baselined_by_sector`
4. `sanctioned_health_operator_with_contracts`
5. `supplier_concentration_by_buyer`
6. `multi_channel_public_money_stack`
7. `territorial_capture_score`
8. `officeholder_conflict_disclosure_vendor_loop`

## Bottom Line

The current graph is already enough to do much more than sanctions and generic “suspicious company” lists.

The most important insight is this:

The strongest live Colombia join is not relatives. It is `same document ID + public role + political finance + contracting + disclosures`.

That means the system can already move beyond passive search and into concrete corruption-style triage, even before a full relatives or beneficial-ownership layer exists.
