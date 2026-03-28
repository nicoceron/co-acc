# Datos Abiertos Research Memo

Date: 2026-03-18

Scope: live verification against the official `datos.gov.co` Socrata catalog and dataset endpoints. This pass did not trust prior notes, dataset titles, or repo aliases by default.

Method:
- Queried the live Socrata catalog by keyword against `www.datos.gov.co`.
- Fetched and scanned the full live dataset catalog snapshot returned by the official catalog API on 2026-03-18.
- Inspected exact column metadata for strong candidates.
- Spot-checked live row payloads for suspicious or badly named datasets.

## High-confidence corrections

- `health_providers` is not unresolved anymore. The live official dataset is [`c36g-9fc2`](https://www.datos.gov.co/d/c36g-9fc2), `Registro Especial de Prestadores y Sedes de Servicios de Salud`, verified on 2026-03-18.
- `secop_invoices` can be resolved to [`ibyt-yi2f`](https://www.datos.gov.co/d/ibyt-yi2f), `SECOP II - Facturas`, updated `2026-03-18T14:39:41Z`.
- `sqpp-4gyj` and `qmzu-gj57` are both live `SECOP II` supplier datasets as of 2026-03-15. `qmzu-gj57` is richer and should be preferred because it includes fields like `Número doc representante legal`, `Telefono representante legal`, `Correo`, and `Direccion`.
- `5p2a-fyvn` is a live companion view for [`gra4-pcp2`](https://www.datos.gov.co/d/gra4-pcp2), not a stronger canonical replacement.
- [`xnsw-bdfj`](https://www.datos.gov.co/d/xnsw-bdfj) is a real example of a badly named dataset. Its title and description say `DOSIS APLICADAS CONTRA COVID-19 -AÑO 2021`, but the live row payload is contract data and matches the contracting structure of [`niuu-28bi`](https://www.datos.gov.co/d/niuu-28bi). Treat it as a mislabeled duplicate or quarantine candidate, not as a health dataset.

## New canonical key families to add

The current normalization model is good, but the live catalog supports additional key families that materially improve joins:

- `payment_id` / `invoice_id`
  - Examples: `ID Pago`, `Numero de Factura`, `Radicado`, `Número de radicación`, `CUFE`
  - Evidence: [`ibyt-yi2f`](https://www.datos.gov.co/d/ibyt-yi2f), [`uymx-8p3j`](https://www.datos.gov.co/d/uymx-8p3j)

- `adjudication_id`
  - Examples: `ID Adjudicacion`, `ID Modificacion`
  - Evidence: [`tauh-5jvn`](https://www.datos.gov.co/d/tauh-5jvn), [`u5b4-ae3s`](https://www.datos.gov.co/d/u5b4-ae3s), [`3xwx-53wt`](https://www.datos.gov.co/d/3xwx-53wt)

- `budget_item_id`
  - Examples: `Identificador Unico`, `Identificador Compromiso`, `Identificador Item Compromiso`, `Compromiso presupuestal`, `Codigo rubro`
  - Evidence: [`cwhv-7fnp`](https://www.datos.gov.co/d/cwhv-7fnp), [`skc9-met7`](https://www.datos.gov.co/d/skc9-met7), [`uymx-8p3j`](https://www.datos.gov.co/d/uymx-8p3j)

- `oversight_actor_id`
  - Examples: `Nombre supervisor`, `Tipo documento supervisor`, `SUPERVISOR : NÚMERO DE CÉDULA o RUT`, `Nombre ordenador del gasto`, `Nombre Ordenador de Pago`
  - Evidence: [`uymx-8p3j`](https://www.datos.gov.co/d/uymx-8p3j), [`y524-had9`](https://www.datos.gov.co/d/y524-had9), [`jffd-39rd`](https://www.datos.gov.co/d/jffd-39rd)

- `administrative_file_id`
  - Examples: `CODIGO_EXPEDIENTE`, `N° DE EXPEDIENTE INTERNO`, `Numero Acto Administrativo`, `Numero AA`, `No VITAL`, `NUMERO_RADICADO`
  - Evidence: [`si2v-pbq5`](https://www.datos.gov.co/d/si2v-pbq5), [`bxkb-7j6i`](https://www.datos.gov.co/d/bxkb-7j6i), [`7fsy-xzzb`](https://www.datos.gov.co/d/7fsy-xzzb), [`xjzv-xx6n`](https://www.datos.gov.co/d/xjzv-xx6n)

- `mining_right_id`
  - Examples: `CODIGO_EXPEDIENTE`, `TITULO_MINERO`, `FECHA_ANOTACION`, `FECHA_EJECUTORIA`
  - Evidence: [`si2v-pbq5`](https://www.datos.gov.co/d/si2v-pbq5), [`42ha-fhvj`](https://www.datos.gov.co/d/42ha-fhvj), [`7amp-4swy`](https://www.datos.gov.co/d/7amp-4swy), [`xzu3-gnau`](https://www.datos.gov.co/d/xzu3-gnau), [`f385-sqmw`](https://www.datos.gov.co/d/f385-sqmw)

- `registry_lineage_id`
  - Examples: `matricula`, `matrícula_propietario`, `codigo_camara_propietario`, `numero_identificacion_propietario`
  - Evidence: [`nb3d-v3n7`](https://www.datos.gov.co/d/nb3d-v3n7), [`c82u-588k`](https://www.datos.gov.co/d/c82u-588k)

- `property_permit_id`
  - Examples: `Matrícula Inmobiliaria`, `Cedula Catastral`, `Tipo de Licencia`, `Numero Acto Administrativo`
  - Evidence: [`7fsy-xzzb`](https://www.datos.gov.co/d/7fsy-xzzb), [`n686-d6yb`](https://www.datos.gov.co/d/n686-d6yb)

## High-confidence additions to the procurement spine

These are the strongest newly verified datasets missing from the current memo or still unresolved there.

### Payment, invoice, and budget event layers

- [`ibyt-yi2f`](https://www.datos.gov.co/d/ibyt-yi2f) `SECOP II - Facturas`
  - Columns: `ID Pago`, `ID Contrato`, `Radicado`, `Numero de Factura`, `Fecha Factura`, `Valor Neto`, `Pago confirmado`
  - Why it matters: resolves the `secop_invoices` alias and creates payment-event joins.

- [`uymx-8p3j`](https://www.datos.gov.co/d/uymx-8p3j) `SECOP II - Plan de pagos`
  - Columns: `Número de radicación`, `Fecha real de pago`, `Fecha estimada de pago`, `Documento proveedor`, `Nombre supervisor`, `Tipo documento supervisor`, `CUFE`, `Compromiso presupuestal`, `Id del Contrato`
  - Why it matters: adds accounts-payable timing, approval chain, invoice identifiers, and supervisor linkage.

- [`cwhv-7fnp`](https://www.datos.gov.co/d/cwhv-7fnp) `SECOP II - Rubros Presupuestales`
  - Columns: `ID Contrato`, `Codigo`, `Valor Actual`, `Identificador Compromiso`, `Identificador Item Compromiso`, `Identificador Unico`
  - Why it matters: links contracts to budget-line detail instead of just aggregate commitments.

- [`u99c-7mfm`](https://www.datos.gov.co/d/u99c-7mfm) `SECOP II - Suspensiones de Contratos`
  - Columns: `ID Contrato`, `Tipo`, `Fecha de Aprobacion`, `Proposito de la modificacion`
  - Why it matters: directly supports stalled-work and execution-risk detection.

- [`wwhe-4sq8`](https://www.datos.gov.co/d/wwhe-4sq8) `SECOP II - Ubicaciones Adicionales`
  - Columns: `ID Contrato`, `Codigo Entidad`, `NIT Entidad`, `Departamento`, `Ciudad`
  - Why it matters: improves geography resolution for multi-site contracts.

### Procurement history, bidders, and adjudication layers

- [`tauh-5jvn`](https://www.datos.gov.co/d/tauh-5jvn) `SECOP I - Proponentes`
  - Columns: `ID Adjudicacion`, `ID Proceso`, `Num Doc Proponente`, `Tipo Doc Proponente`, `Calificacion`, `Adjudicado`
  - Why it matters: adds bidder-level evidence for SECOP I cartel and steering analysis.

- [`u5b4-ae3s`](https://www.datos.gov.co/d/u5b4-ae3s) `SECOP I - Modificaciones a Adjudicaciones`
  - Columns: `ID Adjudicacion`, `ID Modificacion`, `Campo Modificado`, `Valor Anterior`, `Valor Nuevo`, `Fecha Modificacion`
  - Why it matters: captures post-award changes at the adjudication level.

- [`qddk-cgux`](https://www.datos.gov.co/d/qddk-cgux) `SECOP I - Procesos de Compra Pública Historico`
  - Columns: `UID`, `Numero de Contrato`, `Identificacion del Contratista`, `NIT de la Entidad`, `Sexo RepLegal`, `Código de la Entidad`
  - Why it matters: adds pre-2018 historical continuity with explicit contractor ID fields.

- [`3xwx-53wt`](https://www.datos.gov.co/d/3xwx-53wt) `SECOP I - Origen de los Recursos`
  - Columns: `Codigo BPIN`, `ID Adjudicacion`, `Identificador`, `Valor`
  - Why it matters: creates a clean BPIN bridge for SECOP I awards.

- [`d9na-abhe`](https://www.datos.gov.co/d/d9na-abhe) `SECOP II - BPIN por Proceso`
  - Columns: `Codigo BPIN`, `ID Proceso`, `ID Contracto`, `ID Portafolio`, `Validacion BPIN`
  - Why it matters: this is the missing SECOP II project bridge.

### Interadministrative agreements

- [`s484-c9k3`](https://www.datos.gov.co/d/s484-c9k3) `SECOP - Convenios Interadministrativos`
- [`ityv-bxct`](https://www.datos.gov.co/d/ityv-bxct) `SECOP - Convenios Interadministrativos Historico`
  - Columns include: `Identificacion Contratista`, `ID Proceso`, `ID Contrato`, `ID Entidad`, `Origen de los recursos`, `Municipio`, `Departamento`
  - Why it matters: useful for state-to-state contracting paths that are easy to miss in the standard SECOP tables.

## High-confidence additions outside the core procurement spine

### Registry and corporate lineage

- [`nb3d-v3n7`](https://www.datos.gov.co/d/nb3d-v3n7) `Establecimientos - Agencias - Sucursales`
  - Columns: `matricula`, `nit_propietario`, `numero_identificacion_propietario`, `matrícula_propietario`, `codigo_camara_propietario`, `razon_social`
  - Why it matters: this adds establishment-level lineage under RUES, which is missing if you only normalize at the company root.

### Sectoral contracting feeder worth keeping

- [`jffd-39rd`](https://www.datos.gov.co/d/jffd-39rd) `Contratos Invias`
  - Columns: `Nit Entidad`, `Código BPIN`, `Nombre Representante Legal`, `Nombre ordenador del gasto`, `Nombre Ordenador de Pago`, `Contrato`
  - Why it matters: national sectoral feeder with ordering-officer and project linkage signal not always explicit elsewhere.

### Mining-rights and formalization stack

- [`si2v-pbq5`](https://www.datos.gov.co/d/si2v-pbq5) `ANM Títulos Mineros Anotaciones RMN`
- [`42ha-fhvj`](https://www.datos.gov.co/d/42ha-fhvj) `ANM RUCOM Explotador Minero Autorizado-Título Minero`
- [`7amp-4swy`](https://www.datos.gov.co/d/7amp-4swy) `ANM RUCOM Explotador Minero Autorizado-Solicitudes De Legalización/Formalización`
- [`xzu3-gnau`](https://www.datos.gov.co/d/xzu3-gnau) `ANM RUCOM Explotador Minero Autorizado-Subcontratos De Formalización`
- [`f385-sqmw`](https://www.datos.gov.co/d/f385-sqmw) `ANM RUCOM Explotador Minero Autorizado-Beneficiarios Áreas de Reserva Especial`
- [`xjzv-xx6n`](https://www.datos.gov.co/d/xjzv-xx6n) `ANM RUCOM Comercializadores/Consumidores Certificados`
- [`74ct-m5y8`](https://www.datos.gov.co/d/74ct-m5y8) `ANM RUCOM Plantas De Beneficio Certificadas`
  - Why they matter: together these create an expediente-based mining and commercialization graph with person/company, mineral, municipality, and legal-status transitions.

### Project-beneficiary and project-localization companions

- [`tmmn-mpqc`](https://www.datos.gov.co/d/tmmn-mpqc) `DNP-BeneficiariosProyectoCaracterizacion`
- [`iuc2-3r6h`](https://www.datos.gov.co/d/iuc2-3r6h) `DNP-BeneficiariosProyectoLocalizacion`
- [`xikz-44ja`](https://www.datos.gov.co/d/xikz-44ja) `DNP-LocalizacionProyecto`
- [`epzv-8ck4`](https://www.datos.gov.co/d/epzv-8ck4) `DNP-EntidadEjecutoraProyecto`
- [`wtyw-nhcv`](https://www.datos.gov.co/d/wtyw-nhcv) `Presupuesto de Gastos del Sistema General de Regalías (SGR) Histórico`
  - Why they matter: these improve beneficiary plausibility checks, executor mismatch checks, and historical SGR spend continuity.

## Feeder layers worth keeping but not promoting to the national backbone

- [`gwqv-sqvs`](https://www.datos.gov.co/d/gwqv-sqvs) `BASE DE DATOS DE EMPRESAS Y/O ENTIDADES ACTIVAS - JURISDICCIÓN CÁMARA DE COMERCIO DE IBAGUÉ`
  - Useful because it exposes `MATRICULA`, `NIT`, `PROPONENTE`, renewal dates, and payment dates, but it is local.

- [`7fsy-xzzb`](https://www.datos.gov.co/d/7fsy-xzzb) `Licencias de construcción Fusagasugá`
- [`n686-d6yb`](https://www.datos.gov.co/d/n686-d6yb) `LICENCIAS URBANÍSTICAS TRAMITADAS EN EL MUNICIPIO DE CHÍA`
  - Useful for property-permit patterns because they expose `Matrícula Inmobiliaria`, `Cedula Catastral`, `Tipo de Licencia`, and administrative-act fields.

- CORPOBOYACA/CORANTIOQUIA expediente families:
  - [`acrw-g46v`](https://www.datos.gov.co/d/acrw-g46v)
  - [`t9ab-rbjq`](https://www.datos.gov.co/d/t9ab-rbjq)
  - [`7h9i-7gun`](https://www.datos.gov.co/d/7h9i-7gun)
  - [`mnk6-hfcu`](https://www.datos.gov.co/d/mnk6-hfcu)
  - [`74p6-vttx`](https://www.datos.gov.co/d/74p6-vttx)
  - Why they matter: local but structurally strong expediente, acto administrativo, solicitante, identificación, and municipality joins.

## New practice-level connections this enables

- Procurement-to-project laundering
  - `SECOP II - BPIN por Proceso` plus `SECOP I - Origen de los Recursos` let you bridge awards and contracts to BPIN without relying only on text extraction.

- Payment delay and fake-completion scrutiny
  - `SECOP II - Facturas` plus `SECOP II - Plan de pagos` plus `SECOP II - Suspensiones de Contratos` let you compare invoices, expected payment, actual payment, suspension, and execution state.

- Bidder-ring continuity from SECOP I to SECOP II
  - `SECOP I - Proponentes`, `SECOP I - Modificaciones a Adjudicaciones`, `SECOP I - Procesos Historico`, and the existing SECOP II bidder tables make pre/post migration collusion patterns easier to track.

- Hidden representative and establishment branches
  - `Establecimientos - Agencias - Sucursales` adds branch and proprietor lineage that can link a contractor to a parent or related establishment network.

- Mining formalization capture and permit favoritism
  - The ANM expediente stack plus environmental trámite datasets create a route to analyze permit timing, formalization, and commercialization around the same people, territories, or projects.

- Beneficiary-footprint plausibility
  - DNP beneficiary-localization datasets let you compare project spending to intended beneficiary geography or demographic profile, which is useful in social-program and infrastructure narratives.

## Recommended classification changes

### Add now

- `ibyt-yi2f`
- `uymx-8p3j`
- `d9na-abhe`
- `3xwx-53wt`
- `tauh-5jvn`
- `u5b4-ae3s`
- `cwhv-7fnp`
- `u99c-7mfm`
- `wwhe-4sq8`
- `s484-c9k3`
- `ityv-bxct`
- `nb3d-v3n7`
- `f385-sqmw`
- `xjzv-xx6n`
- `74ct-m5y8`
- `tmmn-mpqc`
- `iuc2-3r6h`
- `xikz-44ja`
- `epzv-8ck4`

### Keep as feeder/meta-pipeline

- `jffd-39rd`
- `gwqv-sqvs`
- `7fsy-xzzb`
- `n686-d6yb`
- `acrw-g46v`
- `t9ab-rbjq`
- `7h9i-7gun`
- `mnk6-hfcu`
- `74p6-vttx`
- `niuu-28bi`

### Quarantine or deduplicate

- `xnsw-bdfj`
  - Reason: catalog title/description claim COVID doses, but live rows are contracting records that duplicate `niuu-28bi`.

## Official sources used

- Official Socrata catalog API for `datos.gov.co`: `https://api.us.socrata.com/api/catalog/v1`
- Official dataset pages referenced inline above under `https://www.datos.gov.co/d/<dataset-id>`
