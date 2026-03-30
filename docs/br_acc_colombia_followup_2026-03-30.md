# br-acc Colombia Follow-up

Date: 2026-03-30

## What Was Actually Missing

The repo already had more `br-acc`-style structure than it looked:

- `Bid` nodes already existed via `secop_offers`
- `Inquiry` nodes already existed via `official_case_bulletins`
- public pattern queries already existed for:
  - `split_contracts_below_threshold`
  - `contract_concentration`
  - `inexigibility_recurrence`
  - `amendment_beneficiary_contracts`
- baseline queries already existed, but one of them was still using Brazil-style sector logic

The real gaps were:

- `SourceDocument` existed in schema but had effectively no public document layer
- `baseline_sector` was looking at the wrong company field for Colombia
- several existing patterns were not included in the default community analysis subset
- public graph/API did not expose `SourceDocument`
- document archives were being modeled in a way that exploded a single file into many redundant document nodes

## What Was Changed

### 1. Colombia baselines

- `baseline_sector` now uses Colombian CIIU fields:
  - `primary_ciiu_code`
  - `ciiu4`
  - `secondary_ciiu_code`
- baseline endpoints now accept public identifiers like `NIT/document_id`, not only Neo4j `elementId`
- `baseline_region` was optimized to use the actual `buyer` node instead of grouping by buyer name string

### 2. Default community pattern bundle

The community analysis subset now includes the existing procurement patterns that were already in the repo but were not being run by default:

- `amendment_beneficiary_contracts`
- `split_contracts_below_threshold`
- `contract_concentration`
- `inexigibility_recurrence`

### 3. Generic `SourceDocument` layer

The SECOP archive-doc ETL was refactored so that a public file becomes:

`Company -> Bid/Proceso -> SourceDocument`

instead of:

`Company -> SourceDocument duplicated once per contract-summary edge`

This matters because the original attempt expanded about `340k` archive rows into about `6.1M` document-summary combinations, which was enough to crash Neo4j.

The new model creates:

- one `SourceDocument` per real archive file within a procurement process
- one `Bid`/process node per process
- company-to-process links (`LICITO` / `GANO`)
- process-to-document links (`REFERENTE_A`)

## Live Graph State After Reload

Verified directly in Neo4j after the new `secop_document_archives` run:

- `SourceDocument`: `340,156`
- `Bid`: `20,903`

That means the repo now has a real public document graph, not just archive counters on relationships.

## Actual Findings

### ICONTEC (`860012336`)

Verified through live API and Neo4j.

#### Split contracts below threshold

Live API result:

- pattern: `split_contracts_below_threshold`
- total hits: `157`

The first returned cluster shows:

- `risk_signal`: `6.0`
- `amount_total`: `80,430,289`
- evidence refs:
  - `20-4-11098734`
  - `CD-CDMC-ESESY-053-2020`
  - `19-4-9910302`
  - `CD-CDMC-ESESY-051-2019`
  - `21-4-12406584`

This is a real, document-backed recurrence signal, not just a score artifact.

#### Baseline by sector

Live API result for `/api/v1/baseline/860012336?dimension=sector`:

- CIIU: `7210`
- `value_ratio`: `1.7750732538777725`
- `contract_ratio`: `19.534443603330807`
- `total_value`: `261,481,734,221`

Interpretation:

- ICONTEC is not just active; it is materially outsized relative to its Colombian peer sector by contract count and meaningfully above peer value as well.

#### Buyer concentration check

Direct Neo4j check for top ICONTEC buyers by total awarded value:

- `LA NACION CONSEJO SUPERIOR DE LA JUDICATURA-DIRECCION EJECUTIVA SECCIONAL ADMINISTRACION JUDICIAL DE`: `64.6B`
- `Rama Judicial - Dirección Ejecutiva de Administración Judicial`: `22.9B`
- `FIDUCIARIA COLOMBIANA DE COMERCIO EXTERIOR S.A. - FIDUCOLDEX`: `16.4B`
- `AGENCIA LOGISTICA DE LAS FUERZAS MILITARES`: `15.5B`
- `CVC`: `9.5B`

Direct Neo4j share check against each buyer's total contracting:

- highest observed share for ICONTEC in this quick check: `21.7%`
- buyer: `CONTRALORIA DISTRITAL DE SANTA MARTA`

That explains why `contract_concentration` currently returns `0` for ICONTEC at the current public threshold of `25%`:

- this is not a bug
- it means ICONTEC is a stronger `split + outsized peer activity` finding than a pure `single-buyer capture` finding

#### Public document reach

Direct Neo4j path check:

- `ICONTEC -> Bid/Proceso -> SourceDocument`
- distinct `Bid`: `4`
- distinct `SourceDocument`: `47`

The public API graph now resolves ICONTEC by NIT and returns `Bid` nodes successfully. One current UX gap remains:

- for very high-degree companies, the public graph caps depth aggressively
- that means some `SourceDocument` nodes can still be hidden in the default company graph response unless the graph flow is tuned for document-first navigation

## Official Datasets Found For Next Node Types

These are concrete official datasets or official open-data resources that fit the missing Colombia graph nodes.

### JudicialCase

- `du48-9apm`
- name: `Reporte General de Providencias Judiciales`
- fit: `JudicialCase`
- base URL: `https://www.datos.gov.co/d/du48-9apm`

### ActoAdministrativo

- `i7ij-y86t`
- name: `BASES DE DATOS ACTOS ADMINISTRATIVOS`
- fit: `ActoAdministrativo`
- base URL: `https://www.datos.gov.co/d/i7ij-y86t`

- `pmyn-wf77`
- name: `ACTOS ADMINISTRATIVOS`
- fit: secondary `ActoAdministrativo` candidate
- base URL: `https://www.datos.gov.co/d/pmyn-wf77`

### Inquiry / ControlPolitico

- `n63k-pfbt`
- name: `Proposiciones de control de político Cartagena de Indias`
- fit: `Inquiry/ControlPolitico`
- base URL: `https://www.datos.gov.co/d/n63k-pfbt`

### Gaceta / Territorial acts

- `9smt-mgt4`
- name: `Acuerdos`
- fit: `ActoAdministrativo/GacetaTerritorial`
- base URL: `https://www.datos.gov.co/d/9smt-mgt4`

### Specialized sanctions

- `fs36-azrv`
- name: `Registro de Sanciones Contadores`
- fit: professional-sanction overlay for accounting-related corruption and compliance
- base URL: `https://www.datos.gov.co/d/fs36-azrv`

- `it5q-hg94`
- name: `SECOPII - Multas y Sanciones`
- fit: procurement sanctions overlay
- base URL: `https://www.datos.gov.co/d/it5q-hg94`

### Project / Regalías

- `br9a-gygu`
- name: `Ejecución Financiera de Regalías`
- fit: `Project/BPIN/Regalías`
- base URL: `https://www.datos.gov.co/d/br9a-gygu`

## Practical Next Moves

1. Promote the new source candidates into the source registry and rank them by implementation value:
   - `du48-9apm`
   - `i7ij-y86t`
   - `n63k-pfbt`
   - `9smt-mgt4`
   - `fs36-azrv`
   - `it5q-hg94`
   - `br9a-gygu`

2. Add dedicated ETL for:
   - `JudicialCase`
   - `ActoAdministrativo`
   - `ControlPolitico`
   - `Regalías`

3. Use the existing `ICONTEC` finding as a benchmark lead for:
   - split contracting
   - sector baseline
   - document-backed procurement review

4. Tune public graph behavior for supernodes so `SourceDocument` can still surface on high-degree company pages.
