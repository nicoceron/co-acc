# Real Pattern Coverage Scan

- Company queue scanned: `500` real company leads
- People queue scanned: `150` real people leads
- Source: live API over the current loaded public graph

## Company Exact Patterns

### Proveedor con antecedentes sancionatorios

- Pattern ID: `sanctioned_supplier_record`
- Hits: `354`
- Example: `800154801` SUMINISTROS MAYBE S.A.S. (risk `9`) {"company_identifier": "800154801", "company_name": "SUMINISTROS MAYBE S.A.S.", "risk_signal": 3.0, "amount_total": 372549587.0, "contract_count": 1, "sanction_count": 1, "window_start": "2016-11-25", "window_end": "2016-11-25", "evidence_count": 1}
- Example: `900398793` EGOBUS SAS (risk `6`) {"company_identifier": "900398793", "company_name": "EGOBUS SAS", "risk_signal": 24.0, "sanction_count": 12, "window_start": "2016-04-25", "window_end": "2016-04-28", "evidence_count": 12}
- Example: `900024808` MAQUINAS PROCESOS Y LOGISTICA- M P & L S.A.S (risk `6`) {"company_identifier": "900024808", "company_name": "MAQUINAS PROCESOS Y LOGISTICA- M P & L S.A.S", "risk_signal": 4.0, "sanction_count": 2, "window_start": "2014-05-07", "window_end": "2014-06-26", "evidence_count": 2}
- Example: `860050247` VIGIAS DE COLOMBIA S.R.L. LTDA (risk `6`) {"company_identifier": "860050247", "company_name": "VIGIAS DE COLOMBIA S.R.L. LTDA", "risk_signal": 4.0, "sanction_count": 2, "window_start": "2016-10-07", "window_end": "2016-11-17", "evidence_count": 2}
- Example: `830052968` EXOGENA LIMITADA (risk `6`) {"company_identifier": "830052968", "company_name": "EXOGENA LIMITADA", "risk_signal": 2.0, "sanction_count": 1, "window_start": "2015-08-12", "window_end": "2015-08-12", "evidence_count": 1}

### Proveedor con directivo o vínculo en cargo público

- Pattern ID: `public_official_supplier_overlap`
- Hits: `130`
- Example: `900258772` FONDO DE DESARROLLO DE PROYECTOS DE CUNDINAMARCA  FONDECUN (risk `10`) {"company_identifier": "900258772", "company_name": "FONDO DE DESARROLLO DE PROYECTOS DE CUNDINAMARCA  FONDECUN", "risk_signal": 6.0, "amount_total": 32854689276.0, "contract_count": 4, "official_officer_count": 1, "official_role_count": 1, "window_start": "2026-01-23", "window_end": "2026-01-29", "evidence_count": 4}
- Example: `53080682` CAROLINA PAEZ (risk `8`) {"company_identifier": "53080682", "company_name": "CAROLINA PAEZ", "risk_signal": 4.0, "amount_total": 161500000.0, "contract_count": 2, "official_officer_count": 1, "official_role_count": 1, "window_start": "2026-01-22", "window_end": "2026-01-27", "evidence_count": 2}
- Example: `93201530` WALTER TARCICIO ACOSTA BARRETO (risk `8`) {"company_identifier": "93201530", "company_name": "WALTER TARCICIO ACOSTA BARRETO", "risk_signal": 3.0, "amount_total": 140038800.0, "contract_count": 1, "official_officer_count": 1, "official_role_count": 1, "window_start": "2026-01-27", "window_end": "2026-01-27", "evidence_count": 1}
- Example: `73141223` ALFONSO CARRASQUILLA CASTILLA (risk `8`) {"company_identifier": "73141223", "company_name": "ALFONSO CARRASQUILLA CASTILLA", "risk_signal": 3.0, "amount_total": 70508136.0, "contract_count": 1, "official_officer_count": 1, "official_role_count": 1, "window_start": "2026-01-31", "window_end": "2026-01-31", "evidence_count": 1}
- Example: `80400949` DIEGO ALEXANDER HERRERA CAIPA (risk `6`) {"company_identifier": "80400949", "company_name": "DIEGO ALEXANDER HERRERA CAIPA", "risk_signal": 4.0, "amount_total": 256300000.0, "contract_count": 2, "official_officer_count": 1, "official_role_count": 1, "window_start": "2026-01-23", "window_end": "2026-01-30", "evidence_count": 2}

### Proveedor ligado a cargo sensible

- Pattern ID: `sensitive_public_official_supplier_overlap`
- Hits: `129`
- Example: `900258772` FONDO DE DESARROLLO DE PROYECTOS DE CUNDINAMARCA  FONDECUN (risk `10`) {"company_identifier": "900258772", "company_name": "FONDO DE DESARROLLO DE PROYECTOS DE CUNDINAMARCA  FONDECUN", "risk_signal": 6.0, "amount_total": 32854689276.0, "contract_count": 4, "sensitive_officer_count": 1, "sensitive_role_count": 1, "window_start": "2026-01-23", "window_end": "2026-01-29", "evidence_count": 5}
- Example: `53080682` CAROLINA PAEZ (risk `8`) {"company_identifier": "53080682", "company_name": "CAROLINA PAEZ", "risk_signal": 4.0, "amount_total": 161500000.0, "contract_count": 2, "sensitive_officer_count": 1, "sensitive_role_count": 1, "window_start": "2026-01-22", "window_end": "2026-01-27", "evidence_count": 3}
- Example: `93201530` WALTER TARCICIO ACOSTA BARRETO (risk `8`) {"company_identifier": "93201530", "company_name": "WALTER TARCICIO ACOSTA BARRETO", "risk_signal": 3.0, "amount_total": 140038800.0, "contract_count": 1, "sensitive_officer_count": 1, "sensitive_role_count": 1, "window_start": "2026-01-27", "window_end": "2026-01-27", "evidence_count": 2}
- Example: `73141223` ALFONSO CARRASQUILLA CASTILLA (risk `8`) {"company_identifier": "73141223", "company_name": "ALFONSO CARRASQUILLA CASTILLA", "risk_signal": 3.0, "amount_total": 70508136.0, "contract_count": 1, "sensitive_officer_count": 1, "sensitive_role_count": 1, "window_start": "2026-01-31", "window_end": "2026-01-31", "evidence_count": 2}
- Example: `80400949` DIEGO ALEXANDER HERRERA CAIPA (risk `6`) {"company_identifier": "80400949", "company_name": "DIEGO ALEXANDER HERRERA CAIPA", "risk_signal": 4.0, "amount_total": 256300000.0, "contract_count": 2, "sensitive_officer_count": 1, "sensitive_role_count": 1, "window_start": "2026-01-23", "window_end": "2026-01-30", "evidence_count": 3}

### Convenios interadministrativos apilados con contratación regular

- Pattern ID: `interadministrative_channel_stacking`
- Hits: `8`
- Example: `899999072` BENEFICENCIA DE CUNDINAMARCA (risk `8`) {"company_identifier": "899999072", "company_name": "BENEFICENCIA DE CUNDINAMARCA", "risk_signal": 49.0, "interadmin_agreement_count": 42, "interadmin_total": 2201026530.0, "contract_count": 6, "contract_total": 916698544.0, "execution_gap_contract_count": 5, "stack_signal_types": 1, "amount_total": 3117725074.0, "window_start": "2018-08-23", "window_end": "2026-01-28", "evidence_count": 48}
- Example: `830021022` EMPRESA INMOBILIARIA Y DE SERVICIOS LOGISTICOS DE CUNDINAMARCA (risk `8`) {"company_identifier": "830021022", "company_name": "EMPRESA INMOBILIARIA Y DE SERVICIOS LOGISTICOS DE CUNDINAMARCA", "risk_signal": 20.0, "interadmin_agreement_count": 12, "interadmin_total": 6024477474.0, "contract_count": 7, "contract_total": 9972586066.0, "execution_gap_contract_count": 5, "stack_signal_types": 1, "amount_total": 15997063540.0, "window_start": "2019-06-26", "window_end": "2026-01-30", "evidence_count": 19}
- Example: `900258772` FONDO DE DESARROLLO DE PROYECTOS DE CUNDINAMARCA  FONDECUN (risk `10`) {"company_identifier": "900258772", "company_name": "FONDO DE DESARROLLO DE PROYECTOS DE CUNDINAMARCA  FONDECUN", "risk_signal": 21.0, "interadmin_agreement_count": 14, "interadmin_total": 66801179912.0, "contract_count": 4, "contract_total": 32854689276.0, "official_officer_count": 1, "execution_gap_contract_count": 1, "stack_signal_types": 2, "amount_total": 99655869188.0, "window_start": "2018-12-13", "window_end": "2026-01-29", "evidence_count": 18}
- Example: `73141223` ALFONSO CARRASQUILLA CASTILLA (risk `8`) {"company_identifier": "73141223", "company_name": "ALFONSO CARRASQUILLA CASTILLA", "risk_signal": 6.0, "interadmin_agreement_count": 3, "interadmin_total": 119290800.0, "contract_count": 1, "contract_total": 70508136.0, "official_officer_count": 1, "stack_signal_types": 1, "amount_total": 189798936.0, "window_start": "2022-12-14", "window_end": "2026-01-31", "evidence_count": 4}
- Example: `860066942` CAJA DE COMPENSACION FAMILIAR COMPENSAR Y EL INSTITUTO COLOMBIANO DE BIENASTAR FAMILIAR ICBF (risk `5`) {"company_identifier": "860066942", "company_name": "CAJA DE COMPENSACION FAMILIAR COMPENSAR Y EL INSTITUTO COLOMBIANO DE BIENASTAR FAMILIAR ICBF", "risk_signal": 63.0, "interadmin_agreement_count": 49, "interadmin_total": 433874500835.0, "contract_count": 13, "contract_total": 18562634506.0, "execution_gap_contract_count": 1, "stack_signal_types": 1, "amount_total": 452437135341.0, "window_start": "2019-02-18", "window_end": "2097-10-23", "evidence_count": 62}

### Facturación o pagos por delante de la ejecución

- Pattern ID: `invoice_execution_gap`
- Hits: `5`
- Example: `899999072` BENEFICENCIA DE CUNDINAMARCA (risk `8`) {"company_identifier": "899999072", "company_name": "BENEFICENCIA DE CUNDINAMARCA", "risk_signal": 10.0, "amount_total": 144690437.0, "contract_count": 5, "window_start": "2026-01-22", "window_end": "2026-01-28", "evidence_count": 5}
- Example: `830021022` EMPRESA INMOBILIARIA Y DE SERVICIOS LOGISTICOS DE CUNDINAMARCA (risk `8`) {"company_identifier": "830021022", "company_name": "EMPRESA INMOBILIARIA Y DE SERVICIOS LOGISTICOS DE CUNDINAMARCA", "risk_signal": 10.0, "amount_total": 5437131478.99, "contract_count": 5, "window_start": "2026-01-23", "window_end": "2026-01-30", "evidence_count": 5}
- Example: `53080682` CAROLINA PAEZ (risk `8`) {"company_identifier": "53080682", "company_name": "CAROLINA PAEZ", "risk_signal": 4.0, "amount_total": 6850000.0, "contract_count": 2, "window_start": "2026-01-22", "window_end": "2026-01-27", "evidence_count": 2}
- Example: `860524654` ASEGURADORA SOLIDARIA DE COLOMBIA ENTIDAD COOPERATIVA. (risk `6`) {"company_identifier": "860524654", "company_name": "ASEGURADORA SOLIDARIA DE COLOMBIA ENTIDAD COOPERATIVA.", "risk_signal": 4.0, "amount_total": 63068093.0, "contract_count": 2, "window_start": "2026-02-20", "window_end": "2026-03-02", "evidence_count": 2}
- Example: `52961136` DIANA CAROLINA LINARES ROMERO (risk `4`) {"company_identifier": "52961136", "company_name": "DIANA CAROLINA LINARES ROMERO", "risk_signal": 4.0, "amount_total": 7082400.0, "contract_count": 2, "window_start": "2026-01-27", "window_end": "2026-01-27", "evidence_count": 2}

### split_contracts_below_threshold

- Pattern ID: `split_contracts_below_threshold`
- Hits: `5`
- Example: `1012441381` DANIELA RODRIGUEZ GOMEZ (risk `6`) {"company_identifier": "1012441381", "company_name": "DANIELA RODRIGUEZ GOMEZ", "risk_signal": 6.0, "amount_total": 131000000.0, "window_start": "2026-01-23", "window_end": "2026-01-29", "evidence_count": 5}
- Example: `1031146656` GINELL CAMILA CUERVO BUITRAGO (risk `6`) {"company_identifier": "1031146656", "company_name": "GINELL CAMILA CUERVO BUITRAGO", "risk_signal": 6.0, "amount_total": 130400000.0, "window_start": "2026-01-22", "window_end": "2026-01-28", "evidence_count": 5}
- Example: `1024569324` MARIA FERNANDA BARAJAS AGUILERA (risk `6`) {"company_identifier": "1024569324", "company_name": "MARIA FERNANDA BARAJAS AGUILERA", "risk_signal": 6.0, "amount_total": 125000000.0, "window_start": "2026-01-27", "window_end": "2026-01-30", "evidence_count": 5}
- Example: `63498440` JAQUELINE REMOLINA (risk `5`) {"company_identifier": "63498440", "company_name": "JAQUELINE REMOLINA", "risk_signal": 6.0, "amount_total": 98400000.0, "window_start": "2026-01-24", "window_end": "2026-01-30", "evidence_count": 5}
- Example: `79620611` DANIEL GOMEZ (risk `5`) {"company_identifier": "79620611", "company_name": "DANIEL GOMEZ", "risk_signal": 6.0, "amount_total": 90622920.0, "window_start": "2026-02-02", "window_end": "2026-02-06", "evidence_count": 5}

### Canales públicos múltiples sobre el mismo actor

- Pattern ID: `public_money_channel_stacking`
- Hits: `3`
- Example: `52077199` ANGELA PATRICIA MURCIA BALLESTEROS (risk `6`) {"company_identifier": "52077199", "company_name": "ANGELA PATRICIA MURCIA BALLESTEROS", "risk_signal": 3.0, "channel_count": 2, "contract_count": 1, "health_site_count": 1, "amount_total": 68382864.0, "window_start": "2026-01-31", "window_end": "2026-01-31", "evidence_count": 2}
- Example: `860066942` CAJA DE COMPENSACION FAMILIAR COMPENSAR Y EL INSTITUTO COLOMBIANO DE BIENASTAR FAMILIAR ICBF (risk `5`) {"company_identifier": "860066942", "company_name": "CAJA DE COMPENSACION FAMILIAR COMPENSAR Y EL INSTITUTO COLOMBIANO DE BIENASTAR FAMILIAR ICBF", "risk_signal": 15.0, "channel_count": 2, "contract_count": 13, "health_site_count": 43, "amount_total": 18562634506.0, "window_start": "2026-01-26", "window_end": "2026-01-30", "evidence_count": 56}
- Example: `860007336` CAJA DE COMPENSACION FAMILIAR COLSIBSIDIO - INSTITUTO COLOMBIANO DE BIENESTAR FAMILIAR - ICBF (risk `5`) {"company_identifier": "860007336", "company_name": "CAJA DE COMPENSACION FAMILIAR COLSIBSIDIO - INSTITUTO COLOMBIANO DE BIENESTAR FAMILIAR - ICBF", "risk_signal": 14.0, "channel_count": 2, "contract_count": 12, "health_site_count": 46, "amount_total": 8472859266.0, "window_start": "2026-01-23", "window_end": "2026-01-30", "evidence_count": 58}

### Baja competencia o invitación directa

- Pattern ID: `low_competition_bidding`
- Hits: `2`
- Example: `860524654` ASEGURADORA SOLIDARIA DE COLOMBIA ENTIDAD COOPERATIVA. (risk `6`) {"company_identifier": "860524654", "company_name": "ASEGURADORA SOLIDARIA DE COLOMBIA ENTIDAD COOPERATIVA.", "risk_signal": 12.0, "amount_total": 850951161.0, "bid_count": 5, "direct_invitation_count": 2, "window_start": "2026-02-09", "window_end": "2026-02-24", "evidence_count": 5}
- Example: `901312112` CAMERFIRMA (risk `4`) {"company_identifier": "901312112", "company_name": "CAMERFIRMA", "risk_signal": 10.0, "amount_total": 855610.0, "bid_count": 5, "window_start": "2026-01-20", "window_end": "2026-02-11", "evidence_count": 5}

### Suspensiones repetidas en contratos públicos

- Pattern ID: `contract_suspension_stacking`
- Hits: `1`
- Example: `93201530` WALTER TARCICIO ACOSTA BARRETO (risk `8`) {"company_identifier": "93201530", "company_name": "WALTER TARCICIO ACOSTA BARRETO", "risk_signal": 10.0, "amount_total": 140038800.0, "contract_count": 1, "suspension_event_count": 3, "addition_event_count": 6, "window_start": "2026-01-27", "window_end": "2026-03-02", "evidence_count": 1}

### sanctioned_still_receiving

- Pattern ID: `sanctioned_still_receiving`
- Hits: `1`
- Example: `800154801` SUMINISTROS MAYBE S.A.S. (risk `9`) {"company_identifier": "800154801", "company_name": "SUMINISTROS MAYBE S.A.S.", "risk_signal": 3.0, "amount_total": 372549587.0, "window_start": "2026-03-12", "window_end": "2026-03-12", "evidence_count": 3}

## People Alert Coverage

### disclosure risk stack

- Alert ID: `disclosure_risk_stack`
- Hits: `150`
- Example: `1083869061` JOSE WILSON ROJAS LOZANO (risk `62`) "Las declaraciones oficiales muestran intereses privados o referencias textuales relevantes: 0 mención(es), 1 declaración(es) de conflicto y 1 señal(es) de actividad corporativa."
- Example: `79458001` MAURICIO PAEZ BUSTOS (risk `58`) "Las declaraciones oficiales muestran intereses privados o referencias textuales relevantes: 0 mención(es), 1 declaración(es) de conflicto y 1 señal(es) de actividad corporativa."
- Example: `79956337` JHON ALEXANDER MELGAREJO CELEITA (risk `52`) "Las declaraciones oficiales muestran intereses privados o referencias textuales relevantes: 0 mención(es), 2 declaración(es) de conflicto y 2 señal(es) de actividad corporativa."
- Example: `1020777901` PABLO SOLER GARCIA (risk `52`) "Las declaraciones oficiales muestran intereses privados o referencias textuales relevantes: 0 mención(es), 1 declaración(es) de conflicto y 0 señal(es) de actividad corporativa."
- Example: `1022361922` JOHANA ALEXANDRA CALA HERNANDEZ (risk `52`) "Las declaraciones oficiales muestran intereses privados o referencias textuales relevantes: 0 mención(es), 1 declaración(es) de conflicto y 1 señal(es) de actividad corporativa."

### candidate supplier overlap

- Alert ID: `candidate_supplier_overlap`
- Hits: `17`
- Example: `1083869061` JOSE WILSON ROJAS LOZANO (risk `62`) "La misma persona aparece en candidaturas electorales y también como proveedora o directiva de proveedor con contratación pública."
- Example: `79458001` MAURICIO PAEZ BUSTOS (risk `58`) "La misma persona aparece en candidaturas electorales y también como proveedora o directiva de proveedor con contratación pública."
- Example: `79956337` JHON ALEXANDER MELGAREJO CELEITA (risk `52`) "La misma persona aparece en candidaturas electorales y también como proveedora o directiva de proveedor con contratación pública."
- Example: `1020777901` PABLO SOLER GARCIA (risk `52`) "La misma persona aparece en candidaturas electorales y también como proveedora o directiva de proveedor con contratación pública."
- Example: `1022361922` JOHANA ALEXANDRA CALA HERNANDEZ (risk `52`) "La misma persona aparece en candidaturas electorales y también como proveedora o directiva de proveedor con contratación pública."

### donor supplier overlap

- Alert ID: `donor_supplier_overlap`
- Hits: `15`
- Example: `1083869061` JOSE WILSON ROJAS LOZANO (risk `62`) "La misma persona aparece como donante electoral y también como proveedora o directiva de proveedor con contratación pública."
- Example: `79458001` MAURICIO PAEZ BUSTOS (risk `58`) "La misma persona aparece como donante electoral y también como proveedora o directiva de proveedor con contratación pública."
- Example: `52432215` LUZ ROCIO CASAS GONZALEZ (risk `50`) "La misma persona aparece como donante electoral y también como proveedora o directiva de proveedor con contratación pública."
- Example: `79913379` RAUL ANTONIO MORENO REYES (risk `50`) "La misma persona aparece como donante electoral y también como proveedora o directiva de proveedor con contratación pública."
- Example: `80113570` FABIAN ANDRES MUNOZ QUINONES (risk `50`) "La misma persona aparece como donante electoral y también como proveedora o directiva de proveedor con contratación pública."
