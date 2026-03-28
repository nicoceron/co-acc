# Payroll, Pensions, and Benchmark Cases

Date: 2026-03-21

## Bottom line

The open-data universe currently supports `public payroll overlap`, `double-dipping`, `public servant -> contractor`, `supervisor/interventor abuse`, and some local `nomina paralela` style investigations.

It does **not** yet support a defensible national `ghost employees` or `ghost pensions` engine.

The reason is simple:

- payroll-side national open data is mostly active-public-servant rosters, directories, or salary attributes, not full monthly disbursement, attendance, or person-level payroll history
- pension-side national open data is mostly aggregate statistics, not person-level pension rolls with cédula and payment history

## Datasets that are actually usable

### Payroll-side

- `2jzx-383z` `Conjunto servidores públicos`
  - useful fields include `numerodeidentificacion`, `nombreentidad`, `denominacionempleoactual`, `asignacionbasicasalarial`, `fecha_de_vinculaci_n`
  - this is strong enough for:
    - active public servant <-> supplier overlap
    - active public servant <-> candidate/donor overlap
    - public servant <-> supervisor/payment-authorizer overlap
  - it is not a monthly payroll ledger
- `7pn8-vpxh` `Directorio Funcionarios Contraloría General de la República`
  - names, email, job title, center, payroll class
  - useful as local/name enrichment, but weak because it lacks a stable identifier
- `g75e-9nxr` `Directorio de los Funcionarios de la Alcaldía Municipal San José del Guaviare`
  - local roster only, weak for national matching

### Pension-side

- `yjvt-9cab` `Pensionados en Colombia`
  - aggregate counts by administrator, not person-level
- `gnut-8jsz` `Cantidad de pensionados de Colpensiones por tipo de pensión`
  - counts only
- `j3e8-4hke` `Estadistica de Pensionados Fonprecon - Formato 205`
  - statistical format, not person-level rolls
- `jvtd-3dgy` `Pensionados por entidad administradora de Colombia`
  - aggregate administrative totals
- `hdnf-a76p`, `np5z-haxm`, `p4st-2k4t`
  - aggregate territorial, age, or salary-band views

## What we can honestly detect right now

- active public servants who are also suppliers or company-linked actors
- people who supervise or authorize payments and also appear in payroll or political-exposure layers
- contractor concentration and `nomina paralela` style risk when a local government massively outsources functions through contracts
- `alumnos fantasma` or service-footprint fraud when official investigations identify fake beneficiaries and a contract supervisor / auditor / contractor chain exists

## What we cannot honestly claim yet

- national `ghost employees`
- national `ghost pensions`
- pensioners paid after death
- person-level duplicate pension benefits across systems

Those require person-level disbursement or beneficiary rolls with stable identifiers and time series.

## Benchmark cases for pattern validation

### 1. Supervisor / payment-authorizer abuse

These are clean benchmark cases for `SUPERVISA_PAGO` / `interventor authorizes payment despite non-compliance` style patterns.

#### Alejandro Ospina Coll, Alcaldía de Pereira

- On June 30, 2025, the Procuraduría confirmed a dismissal and nine-year disqualification against Alejandro Ospina Coll.
- The official bulletin states he was the contract supervisor and omitted demanding the services acquired for programs serving vulnerable older adults.
- The bulletin also states he failed to inform the contracting entity of risks and breaches in contract execution.

Why it matters:

- this is a direct public benchmark for `supervisor omits control while execution fails`
- it is a defensible validation target for payment-supervision or execution-gap motifs

Source:

- Procuraduría, 2025-06-30:
  https://www.procuraduria.gov.co/Pages/procuraduria-confirmo-sancion-supervisor-contrato-omitio-vigilar-cumplimiento.aspx

#### Federico García Arbeláez / SENA / NEOGLOBAL S.A.S.

- On February 22, 2023, the Procuraduría charged the representative legal of F.G.A. Consultorías y Construcciones for alleged overreach as external interventor in a SENA contract.
- The official bulletin says the interventor allegedly authorized payment for items not included in the initial budget and without an act modifying the contract.
- It also says activities were allegedly paid in advance despite not having been executed.

Why it matters:

- this is a direct benchmark for `interventor authorized improper or premature payment`
- it fits the exact logic of a payment-authorizer anomaly

Source:

- Procuraduría, 2023-02-22:
  https://www.procuraduria.gov.co/Pages/cargos-a-contratista-del-sena-por-extralimitacion-en-funciones.aspx

#### Jaime José Garcés García / Aguas del Cesar

- The Procuraduría reported charges against a funcionario de Aguas del Cesar for alleged irregularities in supervision of an interventoría contract.
- The official search result indicates the control body was verifying whether he endorsed charges above what should have been collected and payments that should not have been made.

Why it matters:

- this is another clean supervision-and-payment benchmark

Source:

- Procuraduría result snippet:
  https://www.procuraduria.gov.co/Pages/cargos-funcionario-aguas-cesar-presuntas-irregularidades-supervision-contrato-interventoria.aspx

### 2. `Alumnos fantasma` as service-footprint fraud

#### Cúcuta `alumnos fantasma`

- On August 29, 2019, Fiscalía reported charges against a former mayor, former public servants, a contract supervisor, an auditor, and a contractor.
- The audit found `14,836` reported students did not exist, `11,374` of them in Cúcuta.

Why it matters:

- this is a strong benchmark for `fake beneficiaries + supervisor/auditor/contractor chain`
- it shows that many corruption practices are not one-hop supplier cases; they are multi-hop fraud around beneficiaries, supervision, and certification

Source:

- Fiscalía, 2019-08-29:
  https://www.fiscalia.gov.co/colombia/en/2019/08/29/former-mayor-in-cucuta-4-former-public-servants-and-1-contractor-were-charged-in-the-case-on-alumnos-fantasma/

### 3. `Nomina paralela` style outsourcing risk

#### San Andrés, Providencia y Santa Catalina

- The Procuraduría reported preventive monitoring of more than `5,000` contracts signed in 2020 by the Gobernación de San Andrés, Providencia y Santa Catalina for more than `COP 191 billion`.
- The official search result says the control body was seeking to establish a possible `presunta nómina paralela`, with around `1,500` contractors, including people who allegedly did not reside on the island.

Why it matters:

- this is not a person-level ghost-employee benchmark
- it is a strong benchmark for `outsourced payroll inflation` or `parallel payroll through service contracts`

Source:

- Procuraduría result snippet:
  https://apps.procuraduria.gov.co/portal/Procuraduria-vigila-mas-de-5.000-contratos-suscritos-en-2020-por-la-Gobernacion-de-San-Andres-por-mas-de-_191-mil-millones.news

### 4. Education-control / irregular-credential / family-network capture

#### Fundación de Educación Superior San José

- On November 10, 2025, the Ministerio de Educación opened a preliminary investigation against Fundación San José.
- The official ministry page says it found alleged irregularities in awarding titles to graduates who had not taken Saber Pro or Saber TyT, a mandatory requirement.
- Resolution 021551 states the investigation covers the institution, its directors, legal representatives, counselors, administrators, statutory auditors, former secretary general, or any person who exercised administration or control.

Why it matters:

- this is a valid benchmark for `education-control capture` and `irregular credential issuance`
- it is not, by itself, a benchmark for `SUPERVISA_PAGO`

Sources:

- Ministerio de Educación, 2025-11-10:
  https://www.mineducacion.gov.co/1780/w3-article-426421.html
- Resolution 021551:
  https://www.mineducacion.gov.co/1780/articles-426422_recurso_1.pdf

#### San José / ICAFT network allegations

- On February 25, 2026, Caracol reported congresswoman Jennifer Pedraza’s allegations that the same controllers behind Fundación San José were also behind Politécnico ICAFT.
- The report states the allegation included a corporate holding, contracts with San José, and relatives placed as partners or key actors in ICAFT.

Why it matters:

- this is a benchmark for `institution -> controllers -> related institution -> relatives / company web`
- it supports the need for company-control and family/representative matching, not just procurement matching

Source:

- Caracol Radio, 2026-02-25:
  https://caracol.com.co/2026/02/25/jennifer-pedraza-denuncia-que-directivos-de-la-san-jose-tienen-tentaculos-en-una-universidad-fachada/

## Practical conclusion for CO-ACC

### Promote now

- `supervisor/interventor authorized improper payment` style patterns
- `supervisor omitted control while execution failed`
- `parallel payroll through contractors` style risk, but only as a procurement / concentration / outsourcing pattern, not as a ghost-employee claim
- `education-control / irregular credential / institution-capture` patterns around San José / ICAFT and similar cases

### Keep enrichment-only

- local payroll directories without identifiers
- aggregate pension datasets

### Do not promote yet

- `ghost employees`
- `ghost pensions`

## Next data requirement

To move from `payroll overlap` to `ghost payroll`, or from `aggregate pension risk` to `ghost pensions`, the project still needs at least one of:

- person-level payroll disbursement rolls with cédula and time
- person-level pension rolls with cédula and status
- death registry linkage usable for legal matching
- attendance or HR event logs

Without that, those claims are too easy to overstate.
