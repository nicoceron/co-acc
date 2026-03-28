# TransMilenio Microdesfalco Research

Date: 2026-03-28

Scope: public-document extraction for a new `microdesfalco_contable_transmilenio` lane using official TransMilenio finance documents and OCI audit material, then extending it into a multi-period recurrence check across 2023, 2024, and 2025.

## Bundle

- Collector: [collect_transmilenio_finance_public_evidence.py](/Users/ceron/Developer/corruption/scripts/collect_transmilenio_finance_public_evidence.py)
- Parser: [parse_transmilenio_finance_public_evidence.py](/Users/ceron/Developer/corruption/scripts/parse_transmilenio_finance_public_evidence.py)
- Evidence bundle: [transmilenio-finance-2026-03-28](/Users/ceron/Developer/corruption/audit-results/investigations/transmilenio-finance-2026-03-28)
- Structured output: [structured-evidence.json](/Users/ceron/Developer/corruption/audit-results/investigations/transmilenio-finance-2026-03-28/structured-evidence.json)

## What The Public Documents Already Show

1. The evidence bundle now covers `11` official PDFs across `3` vigencias: `2023`, `2024`, and `2025`.
2. OCI-2024-053 documented `10` exceptional payroll cheques for one TransMilenio employee for `COP 222,480,296`.
3. The same audit says the employee requested removal of restrictive cheque seals so the cheques could be collected directly at the teller window.
4. The same audit found a `COP 2,717,814,586` difference between treasury reporting and the JSP7 application for revenues with and without budget affectation.
5. The same audit documented deletion of CRP sequence `202401-4024` from the JSP7 administrator profile.
6. OCI-2025-020 confirms `Gestión de Información Financiera y Contable` sits inside the 2025 corruption-risk map with `1` risk and `2` controls.
7. Treasury reporting is now visible in at least `2` vigencias (`2024` and `2025`), with explicit public treasury annexes in both years.
8. The 2025 treasury annex reports:
   - `2` CDT investments for `COP 21.491B` nominal
   - `2,047` convenio payments for `COP 788B`
   - `COP 877B` in District PAC transfers plus `COP 3.1T` for FET
   - rollout of mass payment orders for contractors
9. The formal route for payments to third parties who are not the rights-holder appears in public management reporting in `2023` and again in `2025`, which makes it a persistent exception route rather than a single-year note.
10. Budget documents now show high aggregate modifications in more than one period:
   - `2024`: expense-side accumulated modifications around `COP 1.74T`
   - `2025`: income-side budget modifications around `COP 1.43T`
   - combined visible modification surface: roughly `COP 3.18T`

## What This Means

- There is already enough public evidence to define a real red-zone practice around accounting and treasury exceptions in TransMilenio.
- The red zone is no longer supported by a single audit alone; official documents now show persistence across multiple years.
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
- https://www.transmilenio.gov.co/files/ad2b1e49-f50f-4f0a-8964-a45bebfbd541/4bf24a21-90b8-4e07-bcd9-58201f719145/Informe%20de%20gesti%C3%B3n%202023%20de%20TRANSMILENIO.pdf
- https://www.transmilenio.gov.co/files/f208bef4-74dc-4e0d-9ee9-f5114b4800a2/0bf33777-f70e-4ec1-b689-86bbf3d52ad3/Informe%20de%20Gesti%C3%B3n%202024%20de%20TRANSMILENIO.pdf
- https://www.transmilenio.gov.co/files/a0f67b20-7bfb-45e1-bf50-8de36667e0c7/a0f6c328-4dc1-4706-a869-adad7193533d/Anexo-5-Informe-de-Tesoreria-2025.pdf
- https://www.transmilenio.gov.co/files/a0f67b20-7bfb-45e1-bf50-8de36667e0c7/a13d11e7-6949-4c25-ad7d-ad5ea98b61b8/Informe-de-gestion-y-sostenibilidad-2025-de-TRANSMILENIO-1.pdf
- https://www.transmilenio.gov.co/files/f208bef4-74dc-4e0d-9ee9-f5114b4800a2/9a8a19b6-c710-43cd-8906-c69fa30ecffa/Anexo%2013%20Ingresos%20de%20TRANSMILENIO%20a%20diciembre%202024_.pdf
- https://www.transmilenio.gov.co/files/f208bef4-74dc-4e0d-9ee9-f5114b4800a2/1fd55b96-418d-4f99-a34e-49ad4f109644/Anexo%2014%20Gastos%20de%20TRANSMILENIO%20a%20diciembre%202024.pdf
- https://www.transmilenio.gov.co/files/f208bef4-74dc-4e0d-9ee9-f5114b4800a2/c6f18b1a-d0db-4f8e-a70b-8b8acf073c69/Anexo%2015%20Modificaciones%20Presupuestales%20a%20diciembre%202024.pdf
- https://www.transmilenio.gov.co/files/f208bef4-74dc-4e0d-9ee9-f5114b4800a2/ddc4317d-451e-436f-a9fa-8d48c5e4c0b4/Anexo%2016%20Informe%20de%20Tesorer%C3%ADa%20de%20TRANSMILENIO%202024.pdf
- https://www.transmilenio.gov.co/files/a0f67b20-7bfb-45e1-bf50-8de36667e0c7/a0f6c297-a349-4a4a-a6e8-4fa6bd47072d/Anexo-4-Informe-de-Gestion-Presupuestal-2025.pdf
