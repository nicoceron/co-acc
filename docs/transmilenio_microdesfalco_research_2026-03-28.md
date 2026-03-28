# TransMilenio Microdesfalco Research

Date: 2026-03-28

Scope: public-document extraction for a new `microdesfalco_contable_transmilenio` lane using official TransMilenio finance documents and OCI audit material.

## Bundle

- Collector: [collect_transmilenio_finance_public_evidence.py](/Users/ceron/Developer/corruption/scripts/collect_transmilenio_finance_public_evidence.py)
- Parser: [parse_transmilenio_finance_public_evidence.py](/Users/ceron/Developer/corruption/scripts/parse_transmilenio_finance_public_evidence.py)
- Evidence bundle: [transmilenio-finance-2026-03-28](/Users/ceron/Developer/corruption/audit-results/investigations/transmilenio-finance-2026-03-28)
- Structured output: [structured-evidence.json](/Users/ceron/Developer/corruption/audit-results/investigations/transmilenio-finance-2026-03-28/structured-evidence.json)

## What The Public Documents Already Show

1. OCI-2024-053 documented `10` exceptional payroll cheques for one TransMilenio employee for `COP 222,480,296`.
2. The same audit says the employee requested removal of restrictive cheque seals so the cheques could be collected directly at the teller window.
3. The same audit found a `COP 2,717,814,586` difference between treasury reporting and the JSP7 application for revenues with and without budget affectation.
4. The same audit documented deletion of CRP sequence `202401-4024` from the JSP7 administrator profile.
5. OCI-2025-020 confirms `Gestión de Información Financiera y Contable` sits inside the 2025 corruption-risk map with `1` risk and `2` controls.
6. The 2025 treasury annex reports:
   - `2` CDT investments for `COP 21.491B` nominal
   - `2,047` convenio payments for `COP 788B`
   - `COP 877B` in District PAC transfers plus `COP 3.1T` for FET
   - rollout of mass payment orders for contractors
7. The 2025 management report preserves a formal route for payments to third parties who are not the rights-holder, conditioned on notarized authorization.

## What This Means

- There is already enough public evidence to define a real red-zone practice around accounting and treasury exceptions in TransMilenio.
- There is **not** enough public evidence yet to attribute the pattern to a specific accountant or official with confidence.
- The useful public product is therefore a `new lead`, not an accusation.

## Hard Boundary

Public PDFs let us find:

- cheque exceptions
- reconciliation gaps
- deleted or altered budget/control records
- sensitive payment-exception routes
- high-volume treasury contexts where weak controls matter more

Public PDFs do **not** yet let us isolate:

- exact beneficiary ledger by month
- payment order chain per user
- bank reconciliation by third party
- full cheque serial trail
- user audit log inside JSP7

## Next Data Needed

- libro auxiliar contable
- órdenes de pago por tercero y mes
- consecutivos de cheque
- conciliaciones bancarias
- bitácora de usuario / auditoría de JSP7
- soportes formales de la excepción de nómina por cheque

## Official Sources

- https://www.transmilenio.gov.co/files/bfa4af5a-c943-42a4-85fe-704999c59c72/ed169828-fa42-4664-a649-3688981f27a5/Informe%20OCI-2024-053%20Auditor%C3%ADa%20Gesti%C3%B3n%20de%20Informaci%C3%B3n%20Financiera%20y%20Contable.pdf
- https://www.transmilenio.gov.co/files/d5484852-b481-4c34-9706-11c4b88ccb76/797bd1bf-ba6b-4ac0-90ca-7044d186f810/Informe%20OCI-2025-020%20Seguimiento%20Programa%20de%20Transparencia%20y%20%C3%89tica%20P%C3%BAblica%20-%20PTEP.pdf
- https://www.transmilenio.gov.co/files/a0f67b20-7bfb-45e1-bf50-8de36667e0c7/a0f6c328-4dc1-4706-a869-adad7193533d/Anexo-5-Informe-de-Tesoreria-2025.pdf
- https://www.transmilenio.gov.co/files/a0f67b20-7bfb-45e1-bf50-8de36667e0c7/a13d11e7-6949-4c25-ad7d-ad5ea98b61b8/Informe-de-gestion-y-sostenibilidad-2025-de-TRANSMILENIO-1.pdf
- https://www.transmilenio.gov.co/files/f208bef4-74dc-4e0d-9ee9-f5114b4800a2/1fd55b96-418d-4f99-a34e-49ad4f109644/Anexo%2014%20Gastos%20de%20TRANSMILENIO%20a%20diciembre%202024.pdf
- https://www.transmilenio.gov.co/files/f208bef4-74dc-4e0d-9ee9-f5114b4800a2/ddc4317d-451e-436f-a9fa-8d48c5e4c0b4/Anexo%2016%20Informe%20de%20Tesorer%C3%ADa%20de%20TRANSMILENIO%202024.pdf
