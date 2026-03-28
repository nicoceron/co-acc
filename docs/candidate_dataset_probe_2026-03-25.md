# Colombia candidate dataset probe

This report was generated from official `datos.gov.co` metadata and sample rows, then probed against the current live graph universe using normalized contract, process, company, person, and BPIN keys.

## Implement Next

### 3xwx-53wt - SECOP I - Origen de los Recursos

- Rows: `5176439`
- Families: `bpin, company_name`
- Reason: Connected live SoQL probes hit the current graph universe: bpin:codigo_bpin:85
- bpin: `codigo_bpin sample=0/6 live=85; anno_bpin sample=0/2 live=0`
- company_name: `orig_rec_nombre sample=0/7`

### 5p2a-fyvn - Vista SECOP II -Ubicaciones ejecución contratos

- Rows: `5641`
- Families: `company_id, company_name, contract_id, process_id, territory`
- Reason: Connected live SoQL probes hit the current graph universe: company_id:codigo_proveedor:77
- Errors: `process_id:urlproceso live probe failed: Server error '500 Server Error' for url 'https://www.datos.gov.co/resource/5p2a-fyvn.json?%24select=count%28%2A%29&%24where=urlproceso+in+%28%27CO1.BDOS.10000059%27%2C+%27CO1.BDOS.10000076%27%2C+%27CO1.BDOS.10000307%27%2C+%27CO1.BDOS.10000337%27%2C+%27CO1.BDOS.10000415%27%2C+%27CO1.BDOS.10000501%27%2C+%27CO1.BDOS.10000502%27%2C+%27CO1.BDOS.10000504%27%2C+%27CO1.BDOS.10000616%27%2C+%27CO1.BDOS.10000697%27%2C+%27CO1.BDOS.10001547%27%2C+%27CO1.BDOS.10001780%27%2C+%27CO1.BDOS.10002082%27%2C+%27CO1.BDOS.10002246%27%2C+%27CO1.BDOS.10002263%27%2C+%27CO1.BDOS.10002429%27%2C+%27CO1.BDOS.10002430%27%2C+%27CO1.BDOS.10002431%27%2C+%27CO1.BDOS.10002434%27%2C+%27CO1.BDOS.10002443%27%2C+%27CO1.BDOS.10002448%27%2C+%27CO1.BDOS.10002449%27%2C+%27CO1.BDOS.10002451%27%2C+%27CO1.BDOS.10002452%27%2C+%27CO1.BDOS.10002456%27%2C+%27CO1.BDOS.10002460%27%2C+%27CO1.BDOS.10002466%27%2C+%27CO1.BDOS.10002476%27%2C+%27CO1.BDOS.10002480%27%2C+%27CO1.BDOS.10002487%27%29'
For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/500`
- company_id: `nit_entidad sample=1/2 live=0; codigo_proveedor sample=1/210 live=77; documento_proveedor sample=18/209 live=0`
- company_name: `nombre_entidad sample=0/2; codigo_proveedor sample=0/210; tipodocproveedor sample=1/2; documento_proveedor sample=0/209`
- contract_id: `id_contrato sample=0/234 live=0`
- process_id: `proceso_de_compra sample=0/198 live=0; urlproceso sample=0/198`
- territory: `ubicacion sample=0/11`

### c82u-588k - Personas Naturales, Personas Jurídicas y Entidades Sin Animo de Lucro

- Rows: `9191995`
- Families: `company_id, company_name, person_id, person_name`
- Reason: Connected live SoQL probes hit the current graph universe: company_id:numero_identificacion:8, company_id:nit:7, person_id:num_identificacion_representante_legal:48
- company_id: `matricula sample=0/250 live=0; numero_identificacion sample=2/245 live=8; nit sample=2/96 live=7; fecha_matricula sample=0/233 live=0`
- company_name: `razon_social sample=1/250; primer_nombre sample=14/57; segundo_nombre sample=5/42`
- person_id: `num_identificacion_representante_legal sample=0/25 live=48`
- person_name: `primer_apellido sample=3/63; segundo_apellido sample=4/61; primer_nombre sample=1/57; segundo_nombre sample=0/42`

### epzv-8ck4 - DNP-EntidadEjecutoraProyecto

- Rows: `312144`
- Families: `bpin, company_name`
- Reason: Connected live SoQL probes hit the current graph universe: bpin:bpin:30
- bpin: `bpin sample=0/250 live=30`
- company_name: `nombreproyecto sample=0/250`

### iuc2-3r6h - DNP-BeneficiariosProyectoLocalizacion

- Rows: `645528`
- Families: `bpin, company_name, territory`
- Reason: Connected live SoQL probes hit the current graph universe: bpin:bpin:41
- bpin: `bpin sample=0/248 live=41`
- company_name: `nombreproyecto sample=0/248`
- territory: `departamento sample=0/31; municipio sample=0/201`

### nb3d-v3n7 - Establecimientos - Agencias - Sucursales

- Rows: `6238791`
- Families: `company_id, company_name`
- Reason: Connected live SoQL probes hit the current graph universe: company_id:numero_identificacion:648
- company_id: `matricula sample=9/247 live=0; codigo_estado_matricula sample=0/2 live=0; estado_matricula sample=0/0 live=0; numero_identificacion sample=1/234 live=648`
- company_name: `razon_social sample=0/246`

### qddk-cgux - SECOP I - Procesos de Compra Pública Historico

- Rows: `6122513`
- Families: `bpin, company_id, company_name, contract_id, person_id, process_id, territory`
- Reason: Connected live SoQL probes hit the current graph universe: company_id:identificacion_del_contratista:2804, bpin:codigo_bpin:25
- Errors: `company_id:nit_de_la_entidad live probe failed: The read operation timed out | process_id:cuantia_proceso live probe failed: Client error '400 Bad Request' for url 'https://www.datos.gov.co/resource/qddk-cgux.json?%24select=count%28%2A%29&%24where=cuantia_proceso+in+%28%27CO1.BDOS.10000059%27%2C+%27CO1.BDOS.10000076%27%2C+%27CO1.BDOS.10000307%27%2C+%27CO1.BDOS.10000337%27%2C+%27CO1.BDOS.10000415%27%2C+%27CO1.BDOS.10000501%27%2C+%27CO1.BDOS.10000502%27%2C+%27CO1.BDOS.10000504%27%2C+%27CO1.BDOS.10000616%27%2C+%27CO1.BDOS.10000697%27%2C+%27CO1.BDOS.10001547%27%2C+%27CO1.BDOS.10001780%27%2C+%27CO1.BDOS.10002082%27%2C+%27CO1.BDOS.10002246%27%2C+%27CO1.BDOS.10002263%27%2C+%27CO1.BDOS.10002429%27%2C+%27CO1.BDOS.10002430%27%2C+%27CO1.BDOS.10002431%27%2C+%27CO1.BDOS.10002434%27%2C+%27CO1.BDOS.10002443%27%2C+%27CO1.BDOS.10002448%27%2C+%27CO1.BDOS.10002449%27%2C+%27CO1.BDOS.10002451%27%2C+%27CO1.BDOS.10002452%27%2C+%27CO1.BDOS.10002456%27%2C+%27CO1.BDOS.10002460%27%2C+%27CO1.BDOS.10002466%27%2C+%27CO1.BDOS.10002476%27%2C+%27CO1.BDOS.10002480%27%2C+%27CO1.BDOS.10002487%27%29'
For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/400 | process_id:ruta_proceso_en_secop_i live probe failed: Server error '500 Server Error' for url 'https://www.datos.gov.co/resource/qddk-cgux.json?%24select=count%28%2A%29&%24where=ruta_proceso_en_secop_i+in+%28%27CO1.BDOS.10000059%27%2C+%27CO1.BDOS.10000076%27%2C+%27CO1.BDOS.10000307%27%2C+%27CO1.BDOS.10000337%27%2C+%27CO1.BDOS.10000415%27%2C+%27CO1.BDOS.10000501%27%2C+%27CO1.BDOS.10000502%27%2C+%27CO1.BDOS.10000504%27%2C+%27CO1.BDOS.10000616%27%2C+%27CO1.BDOS.10000697%27%2C+%27CO1.BDOS.10001547%27%2C+%27CO1.BDOS.10001780%27%2C+%27CO1.BDOS.10002082%27%2C+%27CO1.BDOS.10002246%27%2C+%27CO1.BDOS.10002263%27%2C+%27CO1.BDOS.10002429%27%2C+%27CO1.BDOS.10002430%27%2C+%27CO1.BDOS.10002431%27%2C+%27CO1.BDOS.10002434%27%2C+%27CO1.BDOS.10002443%27%2C+%27CO1.BDOS.10002448%27%2C+%27CO1.BDOS.10002449%27%2C+%27CO1.BDOS.10002451%27%2C+%27CO1.BDOS.10002452%27%2C+%27CO1.BDOS.10002456%27%2C+%27CO1.BDOS.10002460%27%2C+%27CO1.BDOS.10002466%27%2C+%27CO1.BDOS.10002476%27%2C+%27CO1.BDOS.10002480%27%2C+%27CO1.BDOS.10002487%27%29'
For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/500`
- bpin: `codigo_bpin sample=0/0 live=25`
- company_id: `nit_de_la_entidad sample=93/179; identificacion_del_contratista sample=13/228 live=2804; calificacion_definitiva sample=0/6 live=0`
- company_name: `nombre_entidad sample=120/203; nombre_regimen_de_contratacion sample=0/9; nombre_grupo sample=0/7; nombre_familia sample=0/49`
- contract_id: `numero_de_constancia sample=0/250 live=0`
- person_id: `proponentes_seleccionados sample=0/5 live=0`
- process_id: `estado_del_proceso sample=0/5 live=0; numero_de_proceso sample=0/249 live=0; cuantia_proceso sample=0/221; ruta_proceso_en_secop_i sample=0/250`
- territory: `municipio_de_obtencion sample=0/88; municipio_de_entrega sample=0/85; municipios_ejecucion sample=0/118; municipio_entidad sample=0/118`

### tmmn-mpqc - DNP-BeneficiariosProyectoCaracterizacion

- Rows: `1680734`
- Families: `bpin, company_name`
- Reason: Connected live SoQL probes hit the current graph universe: bpin:bpin:179
- bpin: `bpin sample=0/236 live=179`
- company_name: `nombreproyecto sample=0/236`

### wtyw-nhcv - Presupuesto de Gastos del Sistema General de Regalías (SGR) Histórico

- Rows: `100000`
- Families: `bpin, company_id, company_name, payment_or_invoice, territory`
- Reason: Connected live SoQL probes hit the current graph universe: bpin:codigobpinsgr:4
- bpin: `codigobpinsgr sample=0/0 live=4`
- company_id: `valorapropiaciondefinitiva sample=0/132 live=0`
- company_name: `nombrechip sample=2/6; nombredepartamento sample=0/4; nombremunicipio sample=1/6; nombrecuenta sample=0/42`
- payment_or_invoice: `valorpagos sample=0/111`
- territory: `codigodepartamento sample=0/4; nombredepartamento sample=0/4; codigomunicipio sample=0/6; nombremunicipio sample=0/6`

### wwhe-4sq8 - SECOP II - Ubicaciones Adicionales

- Rows: `5736832`
- Families: `company_id, company_name, contract_id, person_id, territory`
- Reason: Connected live SoQL probes hit the current graph universe: contract_id:id_contrato:30, company_id:nit_entidad:872277
- company_id: `nit_entidad sample=117/154 live=872277`
- company_name: `nombre_entidad sample=50/182`
- contract_id: `id_contrato sample=0/250 live=30`
- person_id: `direcci_n sample=0/17 live=0; direcci_n_original sample=0/164 live=0`
- territory: `departamento sample=0/25; ciudad sample=0/56; departamento_original sample=0/25; ciudad_original sample=0/56`

### xikz-44ja - DNP-LocalizacionProyecto

- Rows: `726948`
- Families: `bpin, company_name, territory`
- Reason: Connected live SoQL probes hit the current graph universe: bpin:bpin:84
- bpin: `bpin sample=0/225 live=84`
- company_name: `nombreproyecto sample=0/223`
- territory: `codigodepartamento sample=0/31; departamento sample=0/31; codigomunicipio sample=0/155; municipio sample=0/131`

## Enrichment Only

### bxkb-7j6i - Trámites Ambientales

- Rows: `106`
- Families: `administrative_file, company_id, company_name, payment_or_invoice, territory`
- Reason: Sample rows already overlap current normalized exact-id universes (10 exact sample matches).
- administrative_file: `n_de_expediente_interno sample=0/104; fecha_radicado sample=0/70; fecha_resolucion sample=0/52`
- company_id: `nit sample=10/73 live=0`
- company_name: `nombre_usuario sample=2/86`
- payment_or_invoice: `fecha_radicado sample=0/70`
- territory: `municipio sample=0/26`

### niuu-28bi - Contratación Efectuada en el año 2021

- Rows: `375`
- Families: `company_id, company_name, payment_or_invoice, person_id, person_name`
- Reason: Sample rows already overlap current normalized exact-id universes (5 exact sample matches).
- company_id: `nit_o_cedula sample=4/143 live=0; cedula_nit_del_interventor sample=0/9 live=0`
- company_name: `nombre_entidad sample=0/1; nombre_del_contratista sample=1/150; nombre_completo_del sample=0/10`
- payment_or_invoice: `valor_total_pagos_efectuados sample=0/88`
- person_id: `nit_o_cedula sample=1/143 live=0; cedula_nit_del_interventor sample=0/9 live=0`
- person_name: `nombre_completo_del sample=0/10`

### xnsw-bdfj - DOSIS APLICADAS CONTRA COVID-19 -AÑO 2021

- Rows: `378`
- Families: `company_id, company_name, payment_or_invoice, person_id, person_name`
- Reason: Sample rows already overlap current normalized exact-id universes (5 exact sample matches).
- company_id: `nit_o_cedula sample=4/142 live=0; cedula_nit_del_interventor sample=0/9 live=0`
- company_name: `nombre_entidad sample=0/1; nombre_del_contratista sample=1/150; nombre_completo_del sample=0/11`
- payment_or_invoice: `valor_total_pagos_efectuados sample=0/89`
- person_id: `nit_o_cedula sample=1/142 live=0; cedula_nit_del_interventor sample=0/9 live=0`
- person_name: `nombre_completo_del sample=0/11`

## Weak Feeder

### 42ha-fhvj - ANM RUCOM Explotador Minero Autorizado-Título Minero

- Rows: `12914`
- Families: `administrative_file, company_name, person_name, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- administrative_file: `codigo_expediente sample=0/38`
- company_name: `nombre_persona sample=0/164`
- person_name: `nombre_persona sample=0/164`
- territory: `municipio sample=0/42; departamento sample=0/15; codigo_dane sample=0/42`

### 74p6-vttx - Permiso de vertimientos con corte a 2026 Corpoboyacá

- Rows: `383`
- Families: `administrative_file, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- administrative_file: `expediente sample=0/250`
- territory: `municipio sample=0/60`

### 7amp-4swy - ANM RUCOM Explotador Minero Autorizado-Solicitudes De Legalización/Formalización

- Rows: `3449`
- Families: `administrative_file, company_name, person_name, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- administrative_file: `codigo_expediente sample=0/42`
- company_name: `nombre_persona sample=0/73`
- person_name: `nombre_persona sample=0/73`
- territory: `municipio sample=0/50; departamento sample=0/18; codigo_dane sample=0/50`

### 7h9i-7gun - CORPOBOYACA REGISTRO DE PLANTACIONES FORESTALES PROTECTORAS Y PROTECTORAS PRODUCTORAS HISTÓRICO DESDE 1975

- Rows: `53`
- Families: `administrative_file, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- administrative_file: `expediente sample=0/53; estado_expediente sample=0/3`
- territory: `municipio sample=0/29`

### 7pn8-vpxh - Directorio Funcionarios Contraloría General de la República

- Rows: `6636`
- Families: `company_name`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- company_name: `nombres sample=31/236; nombre_centro_costo sample=0/49; nombre_centro_trabajo sample=0/18`

### acrw-g46v - CORPOBOYACA HISTÓRICO EXPEDIENTES CONCESIÓN AGUAS SUBTERRÁNEAS

- Rows: `223`
- Families: `administrative_file, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- administrative_file: `expediente sample=0/223; estado_expediente sample=0/3`
- territory: `municipio sample=0/34`

### f385-sqmw - ANM RUCOM Explotador Minero Autorizado-Beneficiarios Áreas de Reserva Especial

- Rows: `3227`
- Families: `administrative_file, company_name, person_name, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- administrative_file: `codigo_expediente sample=0/48`
- company_name: `nombre_persona sample=0/191`
- person_name: `nombre_persona sample=0/191`
- territory: `municipio sample=0/38; departamento sample=0/14; codigo_dane_unificado sample=0/38`

### gwqv-sqvs - BASE DE DATOS DE EMPRESAS Y/O ENTIDADES ACTIVAS - JURISDICCIÓN CÁMARA DE COMERCIO DE IBAGUÉ - CORTE A 31 DE DICIEMBRE DE 2025

- Rows: `91348`
- Families: `company_id, company_name, payment_or_invoice, person_id, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- company_id: `matricula sample=0/250 live=0; estado_de_la_matricula sample=0/0 live=0; nit sample=0/242 live=0; fecha_de_matricula sample=0/166 live=0`
- company_name: `razon_social sample=0/250; nombre_de_propietario sample=0/5`
- payment_or_invoice: `fecha_de_pago_de_renovacion_2016 sample=0/6; fecha_de_pago_de_renovacion_2017 sample=0/7; fecha_de_pago_de_renovacion_2018 sample=0/7; fecha_de_pago_de_renovacion_2019 sample=0/5`
- person_id: `direccion_comercial sample=0/209 live=0; direcci_n_de_notificacion sample=0/211 live=0; direccion_propietario sample=0/5 live=0`
- territory: `municipio_comercial sample=0/1; municipio_de_notificacion sample=0/2; ubicacion sample=0/7; municipio_propietario sample=0/2`

### hdnf-a76p - Número de pensionados en Colpensiones desagregado por departamento y municipio

- Rows: `1098`
- Families: `territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- territory: `departamento sample=0/7; municipio sample=0/250`

### ityv-bxct - SECOP - Convenios Interadministrativos Historico

- Rows: `2362`
- Families: `company_name, contract_id, process_id, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- company_name: `nombre_entidad sample=100/101; contratista sample=173/220; tipo_contratista sample=0/4; identificacion_contratista sample=0/212`
- contract_id: `id_contrato sample=0/250 live=0`
- process_id: `id_proceso sample=0/250 live=0`
- territory: `departamento sample=0/19; municipio sample=0/55`

### jffd-39rd - Contratos Invias

- Rows: `16946`
- Families: `bpin, company_id, company_name, payment_or_invoice, person_name, process_id, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- bpin: `estado_bpin sample=0/0 live=0; c_digo_bpin sample=0/1 live=0; anno_bpin sample=0/1 live=0`
- company_id: `nit_entidad sample=1/1 live=0`
- company_name: `proveedor_adjudicado sample=0/250; nombre_entidad sample=0/1; tipodocproveedor sample=1/2; proveedor_adjudicado_1 sample=0/250`
- payment_or_invoice: `habilita_pago_adelantado sample=0/1; valor_de_pago_adelantado sample=0/1; valor_facturado sample=0/47; valor_pendiente_de_pago sample=0/232`
- person_name: `nombre_representante_legal sample=0/250`
- process_id: `descripcion_del_proceso sample=0/233 live=0`
- territory: `departamento sample=0/1; ciudad sample=0/1`

### jvtd-3dgy - Pensionados por entidad administradora de Colombia

- Rows: `71`
- Families: `company_name`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- company_name: `nombre_entidad_administradora sample=2/71`

### mnk6-hfcu - PERMISOS EMISIONES ATMOSFERICAS CORPOBOYACA

- Rows: `406`
- Families: `administrative_file, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- administrative_file: `expediente sample=0/250`
- territory: `municipio sample=0/32`

### sqpp-4gyj - Proveedores Registrados -SECOP II

- Rows: `78`
- Families: `company_id, company_name, person_name, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- company_id: `nit sample=2/77 live=0`
- company_name: `nombre sample=3/76; nombre_representante_legal sample=2/76`
- person_name: `nombre_representante_legal sample=0/76`
- territory: `ubicacion sample=0/1; departamento sample=0/1; municipio sample=0/1`

### t9ab-rbjq - CORPOBOYACA HISTÓRICO EXPEDIENTES CONCESIÓN DE AGUAS SUPERFICIALES

- Rows: `13030`
- Families: `administrative_file, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- administrative_file: `expediente sample=0/250; estado_expediente sample=0/3`
- territory: `municipio sample=0/68`

### xzu3-gnau - ANM RUCOM Explotador Minero Autorizado-Subcontratos De Formalización

- Rows: `502`
- Families: `administrative_file, company_name, person_name, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- administrative_file: `codigo_expediente sample=0/133`
- company_name: `nombre_persona sample=0/146`
- person_name: `nombre_persona sample=0/146`
- territory: `municipio sample=0/47; departamento sample=0/11; codigo_dane_unificado sample=0/47`

### y524-had9 - PROCESOS SECOP I -II - CORPOGUAVIO

- Rows: `464`
- Families: `company_name, person_id, person_name`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- company_name: `contratista_naturaleza sample=0/2; contratista_nombre_completo sample=6/232; supervisor_nombre_completo sample=0/4`
- person_id: `modalidad_de_selecci_n sample=0/6 live=0`
- person_name: `contratista_nombre_completo sample=3/232; supervisor_nombre_completo sample=0/4`

## Drop

### 74ct-m5y8 - ANM RUCOM Plantas De Beneficio Certificadas

- Rows: `292`
- Families: `administrative_file, company_name, payment_or_invoice, person_name`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.
- administrative_file: `numero_radicado sample=0/67`
- company_name: `nombre_persona sample=0/62`
- payment_or_invoice: `numero_radicado sample=0/67`
- person_name: `nombre_persona sample=0/62`

### 7fsy-xzzb - Licencias de construcción Fusagasugá

- Rows: `4603`
- Families: `none`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.

### g75e-9nxr - Directorio de los Funcionarios de la Alcaldía Municipal San José del Guaviare

- Rows: `97`
- Families: `company_name`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.
- company_name: `nombres sample=0/18`

### gnut-8jsz - Cantidad de pensionados de Colpensiones por tipo de pensión

- Rows: `50`
- Families: `none`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.

### j3e8-4hke - Estadistica de Pensionados Fonprecon - Formato 205

- Rows: `5225`
- Families: `none`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.

### n686-d6yb - LICENCIAS URBANÍSTICAS TRAMITADAS EN EL MUNICIPIO DE CHÍA

- Rows: `364`
- Families: `administrative_file, company_id, payment_or_invoice, person_id`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.
- administrative_file: `radicado sample=0/249`
- company_id: `matricula sample=0/247 live=0`
- payment_or_invoice: `radicado sample=0/249`
- person_id: `cedula_catastral sample=0/243 live=0`

### np5z-haxm - Cantidad de pensionados de Colpensiones por rango de edad

- Rows: `756`
- Families: `none`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.

### p4st-2k4t - Número de pensionados por departamento.

- Rows: `1098`
- Families: `none`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.

### si2v-pbq5 - ANM Títulos Mineros Anotaciones RMN

- Rows: `37763`
- Families: `administrative_file`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.
- administrative_file: `codigo_expediente sample=0/38`

### tauh-5jvn - SECOP I - Proponentes

- Rows: `116969`
- Families: `process_id`
- Reason: Dataset could not be probed cleanly from the official endpoint.
- Errors: `process_id:fecha_publicacion_del_proceso live probe failed: Client error '400 Bad Request' for url 'https://www.datos.gov.co/resource/tauh-5jvn.json?%24select=count%28%2A%29&%24where=fecha_publicacion_del_proceso+in+%28%27CO1.BDOS.10000059%27%2C+%27CO1.BDOS.10000076%27%2C+%27CO1.BDOS.10000307%27%2C+%27CO1.BDOS.10000337%27%2C+%27CO1.BDOS.10000415%27%2C+%27CO1.BDOS.10000501%27%2C+%27CO1.BDOS.10000502%27%2C+%27CO1.BDOS.10000504%27%2C+%27CO1.BDOS.10000616%27%2C+%27CO1.BDOS.10000697%27%2C+%27CO1.BDOS.10001547%27%2C+%27CO1.BDOS.10001780%27%2C+%27CO1.BDOS.10002082%27%2C+%27CO1.BDOS.10002246%27%2C+%27CO1.BDOS.10002263%27%2C+%27CO1.BDOS.10002429%27%2C+%27CO1.BDOS.10002430%27%2C+%27CO1.BDOS.10002431%27%2C+%27CO1.BDOS.10002434%27%2C+%27CO1.BDOS.10002443%27%2C+%27CO1.BDOS.10002448%27%2C+%27CO1.BDOS.10002449%27%2C+%27CO1.BDOS.10002451%27%2C+%27CO1.BDOS.10002452%27%2C+%27CO1.BDOS.10002456%27%2C+%27CO1.BDOS.10002460%27%2C+%27CO1.BDOS.10002466%27%2C+%27CO1.BDOS.10002476%27%2C+%27CO1.BDOS.10002480%27%2C+%27CO1.BDOS.10002487%27%29'
For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/400`
- process_id: `id_proceso sample=0/230 live=0; fecha_publicacion_del_proceso sample=0/140`

### u5b4-ae3s - SECOP I - Modificaciones a Adjudicaciones

- Rows: `812653`
- Families: `none`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.

### xjzv-xx6n - ANM RUCOM Comercializadores/Consumidores Certificados

- Rows: `20568`
- Families: `administrative_file, company_name, payment_or_invoice, person_name`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.
- administrative_file: `numero_radicado sample=0/68`
- company_name: `nombre_persona sample=0/68`
- payment_or_invoice: `numero_radicado sample=0/68`
- person_name: `nombre_persona sample=0/68`

### yjvt-9cab - Pensionados en Colombia

- Rows: `71`
- Families: `none`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.
