# Dataset join-key triage report

Generated: 2026-04-24T00:37:29+00:00
Input: configured dataset source inventories
Total probed: 311
Datasets with proven join keys: 148

## Ingest class counts

- ingest_priority: 0
- ingest: 0
- ingest_if_useful: 0
- context_enrichment: 0
- context_if_useful: 0
- schema_core_join: 118
- schema_context_join: 30
- weak_join: 0
- large_no_join: 0
- no_join: 163
- unknown: 0
- unreachable: 0

## schema_core_join

Metadata-only pass found a core join key; needs row count/freshness before promotion

| dataset_id | rows | join_keys | join_columns | density_sample | name |
|---|---:|---|---|---|---|
| `28vu-5tx7` | -1 | divipola|entity | divipola:codigo_departamento_curso|divipola:codigo_municipio_curso|entity:codigo_regional|entity:codigo_centro | — | CERTIFICACIÓN DE LA FORMACIÓN PROFESIONAL INTEGRAL |
| `2c7k-9iru` | -1 | divipola|entity | divipola:codigo_departamento_curso|divipola:codigo_municipio_curso|entity:codigo_regional|entity:codigo_centro | — | CUPOS EN FORMACIÓN PROFESIONAL INTEGRAL POR TIPO DE POBLACIÓN |
| `2jzx-383z` | -1 | divipola|entity|nit | divipola:idmunicipioentidad|entity:idmunicipioentidad|nit:numerodeidentificacion | — | Conjunto servidores públicos |
| `2v94-3ypi` | -1 | divipola|entity | divipola:cod_dpto|divipola:cod_mpio|entity:codigo_institucion | — | MEN_PROGRAMAS EDUCACIÓN PARA EL TRABAJO Y EL DESARROLLO HUMANO |
| `2x55-9wxm` | -1 | divipola|entity | divipola:estu_cod_reside_depto|divipola:estu_coddane_cole_termino|entity:inst_cod_institucion|divipola:estu_prgm_codmunicipio|divipola:estu_inst_codmunicipio|divipola:estu_cod_depto_presentacion | — | Resultados Saber Pro Competencias Genericas 2019-2 |
| `2zn7-bez3` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `36vw-pbq2` | -1 | process | process:id_proceso | — | SECOP I - Modificaciones a Procesos |
| `3hdv-smhz` | -1 | entity|nit | entity:nit_entidad|nit:nit_entidad|nit:nit_proveedor | — | TVEC - Compras por item |
| `3xwx-53wt` | -1 | bpin|process | bpin:codigo_bpin|process:id_adjudicacion | — | SECOP I - Origen de los Recursos |
| `4bku-d9az` | -1 | process | process:n_mero_de_proceso | — | Actuaciones judiciales ante la Corte Constitucional realizadas por el Ministerio de Justicia y del Derecho |
| `4ex9-j3n8` | -1 | entity|nit | entity:codigo_entidad|entity:nit_entidad|nit:nit_entidad|nit:n_mero_documento_representante_legal | — | SECOP II - Contacto Entidades y Proveedores |
| `4n4q-k399` | -1 | contract|entity|nit | contract:numero_de_contrato|entity:nit_entidad|nit:nit_entidad|nit:documento_contratista | — | Multas y Sanciones SECOP I |
| `4qkq-csdn` | -1 | nit | nit:nit | — | Registro Único de Prestadores de Servicios Públicos-RUPS |
| `4u2m-pguf` | -1 | divipola|entity | divipola:estu_cod_reside_depto|entity:inst_cod_institucion|divipola:estu_prgm_codmunicipio|divipola:estu_inst_codmunicipio|divipola:estu_cod_depto_presentacion | — | Resultados Saber TyT Genericas 2019-2 |
| `5n52-6rih` | -1 | entity | entity:id_ccf | — | Población Afiliada |
| `5phs-yqfw` | -1 | entity | entity:codigoentidad | — | Información de Gastos del Presupuesto General de la Nación |
| `5u9e-g5w9` | -1 | entity|nit | entity:cod_institucion|nit:identificacion_funcionario | — | Puestos Sensibles a la Corrupción |
| `6cat-2gcs` | -1 | nit | nit:nit | — | 10.000 Empresas mas Grandes del País |
| `6hqw-m3dm` | -1 | nit | nit:nit | — | Estados Financieros NIIF- Carátula |
| `7fix-nd37` | -1 | process | process:id_adjudicacion | — | SECOP I - Adiciones |
| `7jfv-7spn` | -1 | entity | entity:codigo_entidad | — | Montos y número de créditos aprobados o desembolsados por cosechas |
| `8cnh-7asj` | -1 | divipola|entity | divipola:codigo_departamento|divipola:codigo_municipio|entity:codigo_centro|entity:codigo_regional | — | Georeferenciación Centros de Aprendizaje SENA |
| `8qxx-ubmq` | -1 | nit | nit:nit | — | Hallazgos Fiscales |
| `8tz7-h3eu` | -1 | nit | nit:numero_documento | — | Declaración de activos - patrimonial servidores públicos |
| `9sue-ezhx` | -1 | entity|nit | entity:codigo_entidad|entity:nit_entidad|nit:nit_entidad | — | SECOPII - Plan Anual De Adquisiciones Detalle |
| `9xdg-hm6t` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `a65q-6den` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `a86w-fh92` | -1 | bpin|contract|entity|nit|process | bpin:entidad_bpin|bpin:bpin_codigo|contract:id_contrato|contract:referencia_contrato|entity:codigo_unidad_ejecutora|nit:nit|nit:nit_proveedor|process:id_proceso | — | SECOP II - Solicitudes CDPs |
| `ahbx-upcu` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `b6m4-qgqv` | -1 | entity | entity:codigo_entidad | — | SECOP II - PAA - Encabezado |
| `bpij-5vy9` | -1 | entity | entity:codigoentidad | — | Ejecución Presupuestal del Presupuesto General de la Nación detallada por Rubro Presupuestal |
| `br9a-gygu` | -1 | bpin|divipola|entity | bpin:codigobpin|divipola:codigodanedepartamento|divipola:codigodaneentidad|entity:codigodaneentidad | — | Ejecución Financiera de Regalías |
| `c36g-9fc2` | -1 | nit | nit:numeroidentificacion | — | Registro Especial de Prestadores y Sedes de Servicios de Salud |
| `c82u-588k` | -1 | nit | nit:numero_identificacion|nit:nit|nit:num_identificacion_representante_legal | — | Personas Naturales Personas Jurídicas y Entidades Sin Ánimo de Lucro |
| `cb9c-h8sn` | -1 | contract | contract:id_contrato | — | SECOP II - Adiciones |
| `ceth-n4bn` | -1 | nit | nit:codigo_grupo|nit:nit_grupo|nit:codigo_participante|nit:nit_participante | — | Grupos de Proveedores - SECOP II |
| `cf9k-55fw` | -1 | bpin|entity | bpin:bpin|entity:codigoentidadresponsable | — | DNP-proyectos_datos_basicos |
| `cmgp-8z8t` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `ctcp-462n` | -1 | nit | nit:nit | — | Estados Financieros NIIF- Estado de Flujo Efectivo |
| `cwhv-7fnp` | -1 | contract | contract:id_contrato|contract:referencia_contrato | — | SECOP II - Rubros Presupuestales |
| `d9na-abhe` | -1 | bpin|process | bpin:codigo_bpin|process:id_proceso | — | SECOP II - BPIN por Proceso |
| `dd55-74ss` | -1 | nit | nit:nit | — | SUJETOS OBLIGADOS |
| `dmgg-8hin` | -1 | entity|nit | entity:nit_entidad|nit:nit_entidad | — | SECOP II - Archivos Descarga Desde 2025 |
| `e2u2-swiw` | -1 | entity|nit | entity:nit_entidad|entity:codigo_entidad|nit:nit_entidad | — | SECOP II - Modificaciones a Procesos |
| `e88h-vxzi` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas Organizaciones Solidarias |
| `e8s8-v85a` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `e967-4a8r` | -1 | entity | entity:codigo_entidad | — | Información estadística y financiera por ramos de seguros Formato 290 |
| `epzv-8ck4` | -1 | bpin|entity | bpin:bpin|entity:codigoentidadejecutora | — | DNP - ejecutores de proyectos |
| `f789-7hwg` | -1 | bpin|contract|divipola|entity|nit|process | bpin:codigo_bpin|contract:numero_de_contrato|divipola:c_digo_de_la_entidad|entity:nit_de_la_entidad|entity:c_digo_de_la_entidad|nit:nit_de_la_entidad|nit:identificacion_del_contratista|nit:identific_representante_legal|process:id_adjudicacion | — | SECOP I - Procesos de Compra Pública |
| `fr8e-58py` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas SIIA |
| `fwf9-c4xb` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `gbry-rnq4` | -1 | nit | nit:numero_documento | — | Declaraciones conflictos de interés |
| `gjp9-cutm` | -1 | contract | contract:id_contrato | — | SECOP II - Garantias |
| `gra4-pcp2` | -1 | contract|entity|nit | contract:id_contrato|entity:codigo_entidad|entity:nit_entidad|entity:codigo_proveedor|nit:nit_entidad|nit:documento_proveedor | — | SECOP II - Ubicaciones ejecucion contratos |
| `h7zv-k39x` | -1 | divipola|entity|nit | divipola:idmunicipio|divipola:iddepartamento|entity:dm_institucion_cod_institucion|nit:ccb_nit_inst | — | Universo de entidades |
| `hb3d-dyp7` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas SIIA |
| `hd6e-hz7c` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `hgi6-6wh3` | -1 | entity|nit|process | entity:nit_entidad|entity:codigo_entidad|entity:codigo_proveedor|nit:nit_entidad|nit:nit_proveedor|process:id_procedimiento | — | Proponentes por Proceso SECOP II |
| `htue-emaw` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas SIIA |
| `iaeu-rcn6` | -1 | nit|process | nit:numero_identificacion|process:numero_proceso | — | Antecedentes de SIRI |
| `ibyt-yi2f` | -1 | contract|entity | contract:id_contrato|entity:codigo_entidad | — | SECOP II - Facturas |
| `it5q-hg94` | -1 | contract|entity|process | contract:id_contrato|entity:codigo_entidad_creadora|process:id_proceso | — | SECOPII - Multas y Sanciones |
| `ityv-bxct` | -1 | contract|entity|process | contract:id_contrato|contract:numero_de_contrato|entity:id_entidad|process:id_proceso | — | SECOP - Convenios Interadministrativos |
| `jbjy-vk9h` | -1 | contract|entity|nit | contract:id_contrato|entity:nit_entidad|entity:codigo_entidad|entity:codigo_proveedor|nit:nit_entidad|nit:documento_proveedor | — | SECOP II - Contratos Electrónicos |
| `jgra-rz2t` | -1 | nit | nit:can_identificacion|nit:ing_identificacion | — | Base ingresos cuentas claras 2019 |
| `jr8e-e8tu` | -1 | nit | nit:identificaci_n|nit:n_mero_de_identificaci_n | — | Responsabilidad Fiscal |
| `jzzx-knyw` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas SIIA |
| `k67a-zkz9` | -1 | divipola|entity | divipola:estu_cod_reside_depto|divipola:estu_coddane_cole_termino|entity:inst_cod_institucion|divipola:estu_prgm_codmunicipio|divipola:estu_inst_codmunicipio|divipola:estu_cod_depto_presentacion | — | Resultados Saber TyT Genéricas 2020-1 |
| `kg2d-yfyg` | -1 | entity|nit | entity:codentidad|nit:nit | — | Listado de Entidades del Sector Solidario |
| `mfmm-jqmq` | -1 | contract | contract:identificadorcontrato | — | SECOP II - Ejecución Contratos |
| `mj4p-5iuv` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `mxk5-ce6w` | -1 | entity | entity:codigo_entidad | — | Información financiera con fines de supervisión – CUIF por moneda Entidades vigiladas |
| `mzgh-shtp` | -1 | bpin|entity | bpin:codigobpin|entity:codejecutor | — | DNP-ProyectosSGR |
| `n5yy-8nav` | -1 | divipola|entity | divipola:cod_departamento|divipola:cod_municipio|entity:c_digo_instituci_n | — | MEN_INSTITUCIONES EDUCACIÓN SUPERIOR |
| `nb3d-v3n7` | -1 | nit | nit:numero_identificacion|nit:nit_propietario | — | RUES - establecimientos y sucursales conectadas |
| `nfdm-7zai` | -1 | entity | entity:c_digo_entidad|entity:c_digo_entidad_adscrita | — | Informes al congreso |
| `p6dx-8zbt` | -1 | entity|nit|process | entity:nit_entidad|entity:codigo_entidad|nit:nit_entidad|nit:nit_del_proveedor_adjudicado|process:id_adjudicacion | — | SECOP II - Procesos de Contratación |
| `pfdp-zks5` | -1 | nit | nit:nit | — | Estados Financieros NIIF- Estado de Situación Financiera |
| `prdx-nxyp` | -1 | divipola|entity|nit | divipola:codigo_municipio|entity:codigo_entidad|entity:nit_entidad|nit:nit_entidad | — | SECOP I - PAA Encabezado |
| `prwj-nzxa` | -1 | nit | nit:nit | — | Estados Financieros NIIF- Estado de Resultado Integral |
| `ps88-5e3v` | -1 | entity | entity:codigo_entidad | — | SECOP I - Archivos Descarga |
| `qddk-cgux` | -1 | bpin|contract|divipola|entity|nit|process | bpin:codigo_bpin|contract:numero_de_contrato|divipola:c_digo_de_la_entidad|entity:nit_de_la_entidad|entity:c_digo_de_la_entidad|nit:nit_de_la_entidad|nit:identificacion_del_contratista|nit:identific_representante_legal|process:id_adjudicacion | — | SECOP I - Procesos de Compra Pública Histórico |
| `qhpu-8ixx` | -1 | entity | entity:codigo_entidad | — | Rentabilidades de los Fondos de Inversión Colectiva (FIC) |
| `qkv4-ek54` | -1 | bpin|entity | bpin:bpin|entity:codigo_entidad | — | OVCF - SGR - Ejecución de Gastos |
| `qmzu-gj57` | -1 | nit | nit:nit | — | SECOP II - Proveedores Registrados |
| `r3d5-pipz` | -1 | entity | entity:codigo_entidad | — | Balance de entidades publicas vigiladas por la SFC. |
| `rgxm-mmea` | -1 | entity|nit | entity:nit_entidad|entity:id_entidad|nit:nit_proveedor|nit:nit_entidad | — | Tienda Virtual del Estado Colombiano - Consolidado |
| `rpmr-utcd` | -1 | entity|nit | entity:codigo_entidad_en_secop|entity:nit_de_la_entidad|nit:nit_de_la_entidad|nit:documento_proveedor | — | SECOP Integrado |
| `rtck-inie` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `rvii-eis8` | -1 | entity | entity:codigo_entidad | — | Distribución de cartera por producto |
| `s484-c9k3` | -1 | contract|entity|process | contract:id_contrato|contract:numero_de_contrato|entity:id_entidad|process:id_proceso | — | SECOP - Convenios Interadministrativos |
| `s97v-q3tx` | -1 | divipola|entity|nit | divipola:car_t1556_codigodane|entity:identificador_empresa|nit:nit | — | Superservicios - Inf. Comercial Usuarios NO Regulados |
| `sasi-u68b` | -1 | nit | nit:numero_nit|nit:n_mero_c_dula_representante | — | Entidades Acreditadas Octubre 2023 |
| `skc9-met7` | -1 | contract | contract:id_contrato|contract:referencia_contrato | — | SECOP II - Compromisos Presupuestales |
| `sr9n-792w` | -1 | entity|nit | entity:cod_entidad|nit:numeroidentificacion | — | Entidades  vigiladas por la Superfinanciera |
| `st4x-g6gz` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `tauh-5jvn` | -1 | contract|nit|process | contract:numero_contrato|nit:num_doc_proponente|process:id_proceso|process:id_adjudicacion | — | SECOP I - Proponentes |
| `thwd-ivmp` | -1 | divipola|nit | divipola:cod_dpto|divipola:cod_mun|nit:nit | — | Registro Nacional de Turismo - RNT |
| `tic6-rbue` | -1 | entity|nit | entity:codigo_entidad|nit:nit | — | Estados financieros de entidades solidarias desde 2017 – Actualidad |
| `tnbg-fy2n` | -1 | nit | nit:productor_nit | — | Producción de Bienes Nacionales |
| `tv9x-wfg5` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `u37r-hjmu` | -1 | divipola|entity | divipola:estu_cod_reside_depto|divipola:estu_coddane_cole_termino|divipola:estu_cod_depto_presentacion|entity:inst_cod_institucion|divipola:estu_inst_codmunicipio|divipola:estu_prgm_codmunicipio | — | Resultados únicos Saber Pro |
| `u4ze-bi7k` | -1 | entity | entity:codigo_regional|entity:codigo_centro | — | DESERCION DE LA FORMACIÓN PROFESIONAL INTEGRAL |
| `u5b4-ae3s` | -1 | process | process:id_adjudicacion | — | SECOP I - Modificaciones a Adjudicaciones |
| `u8cx-r425` | -1 | bpin|contract | bpin:codigo_bpin|contract:id_contrato | — | SECOP II - Modificaciones a contratos |
| `u99c-7mfm` | -1 | contract | contract:id_contrato | — | SECOP II - Suspensiones de Contratos |
| `uawh-cjvi` | -1 | entity | entity:codigo_entidad|entity:codigo_patrimonio | — | Fondo de Pensiones Obligatorias y Cesantías C.P L.P |
| `upr9-nkiz` | -1 | divipola|entity | divipola:codigomunicipioinstitucion|divipola:codigomunicipioprograma|entity:codigoinstitucion | — | MEN_PROGRAMAS_DE_EDUCACIÓN_SUPERIOR |
| `usqp-5nsn` | -1 | entity | entity:id_entidad | — | TVEC - Items |
| `uymx-8p3j` | -1 | contract|entity|nit | contract:id_del_contrato|contract:referencia_contrato|entity:codigo_entidad|entity:nit_entidad|nit:nit_entidad|nit:documento_proveedor | — | SECOP II - Plan de pagos |
| `vbr4-v5x7` | -1 | nit | nit:n_mero_nit | — | Entidades Acreditadas |
| `vv8g-8u9u` | -1 | divipola|entity | divipola:codigo_departamento_curso|divipola:codigo_municipio_curso|entity:codigo_regional|entity:codigo_centro | — | PROGRAMACIÓN ESPECÍFICA DE CURSOS LARGOS, ESPECIALES Y EVENTOS POR REGIONAL Y CENTRO |
| `wi7w-2nvm` | -1 | entity|nit|process | entity:nit_entidad_compradora|entity:c_digo_entidad|entity:c_digo_proveedor|nit:nit_entidad_compradora|nit:nit_del_proveedor|process:id_del_proceso_de_compra | — | SECOPII - Ofertas Por Proceso |
| `wwhe-4sq8` | -1 | contract|entity|nit | contract:id_contrato|contract:referencia_contrato|entity:nit_entidad|entity:codigo_entidad|nit:nit_entidad | — | SECOP II - Ubicaciones Adicionales |
| `xikz-44ja` | -1 | bpin|divipola|entity | bpin:bpin|divipola:codigomunicipio|entity:codigoentidadresponsable | — | DNP - localización de proyectos |
| `xjxk-qhsc` | -1 | entity | entity:codigo_entidad | — | Ejecución Presupuestal del Presupuesto General de la Nación |
| `xyy7-rn7p` | -1 | entity | entity:codigo_entidad | — | Quejas interpuestas por los consumidores financieros en contra de las entidades vigiladas en la plataforma Smartsupervision |
| `y3gh-x5g7` | -1 | nit | nit:nit | — | Estados Financieros NIIF- Otro Resultado Integral |

## schema_context_join

Metadata-only pass found only BPIN/divipola context keys; needs explicit signal use

| dataset_id | rows | join_keys | join_columns | density_sample | name |
|---|---:|---|---|---|---|
| `24ny-2dhf` | -1 | divipola | divipola:divipola | — | SECTORES CRITICOS POR EXCESO DE VELOCIDAD |
| `3y4s-dmxy` | -1 | divipola | divipola:cod_dane_depto|divipola:cod_dane_mun | — | MEN_INDICADORES_PRIMERA_INFANCIA |
| `5c2k-ahfc` | -1 | divipola | divipola:codigo_municipio|divipola:codigo_departamento | — | MEN_NÚMERO_BACHILLERES_POR_ETC |
| `5wck-szir` | -1 | divipola | divipola:c_digo_del_departamento_ies|divipola:c_digo_del_municipio_ies|divipola:c_digo_del_departamento_programa|divipola:c_digo_del_municipio_programa | — | MEN_MATRICULA_ESTADISTICA_ES |
| `6cta-vqaf` | -1 | divipola | divipola:c_digo_municipio_residencia | — | Solicitudes de inscripción en el Registro Único de Retorno -RUR |
| `6eyy-q57b` | -1 | divipola | divipola:departamento_divipola | — | Pre registros Estatuto Temporal de Protección para Migrantes Venezolanos |
| `7y2j-43cv` | -1 | divipola | divipola:divipola | — | IGAC OIC - transacciones inmobiliarias |
| `acs4-3wgp` | -1 | divipola | divipola:coddepto|divipola:codmpio | — | Detección de Cultivos de Coca (hectáreas) |
| `cfw5-qzt5` | -1 | divipola | divipola:cod_dane_departamento|divipola:cod_dane_municipio|divipola:codigo_dane | — | MEN_ESTABLECIMIENTOS_EDUCATIVOS_PREESCOLAR_BÁSICA_Y_MEDIA |
| `cm2t-qreq` | -1 | divipola | divipola:ik_divipola | — | Encuesta territorial de comportamiento en seguridad vial |
| `epkg-mphw` | -1 | divipola | divipola:codigo_departamento|divipola:codigo_municipio | — | MEN_INDICADORES_PAE |
| `ff7q-jcdw` | -1 | divipola | divipola:codigo_municipio | — | Comercialización Materiales de Referencia - Instituto Nacional de Metrología |
| `gaic-b8aw` | -1 | divipola | divipola:cod_depto | — | Exportaciones agrícolas no tradicionales y tradicionales |
| `h2yr-zfb2` | -1 | divipola | divipola:c_digo_divipola_departamento|divipola:c_digo_divipola_municipio | — | Subsidios De Vivienda Asignados |
| `iuc2-3r6h` | -1 | bpin | bpin:bpin | — | DNP - localización de beneficiarios |
| `j9sd-zau5` | -1 | divipola | divipola:cod_dane_departamento|divipola:cod_dane_municipio|divipola:codigo_dane|divipola:codigo_dane_sede | — | MEN_SEDES_EDUCATIVAS_PREESCOLAR_BÁSICA_Y_MEDIA |
| `ji8i-4anb` | -1 | divipola | divipola:c_digo_departamento | — | MEN_ESTADISTICAS_EN_EDUCACION_EN_PREESCOLAR, BÁSICA Y MEDIA_POR_DEPARTAMENTO |
| `nudc-7mev` | -1 | divipola | divipola:c_digo_municipio|divipola:c_digo_departamento | — | MEN_ESTADISTICAS_EN_EDUCACION_EN_PREESCOLAR, BÁSICA Y MEDIA_POR_MUNICIPIO |
| `rnvb-vnyh` | -1 | divipola | divipola:estu_cod_reside_depto|divipola:cole_cod_depto_ubicacion|divipola:estu_cod_depto_presentacion | — | Saber 11° 2020-2 |
| `rs3u-8r4q` | -1 | divipola | divipola:divipola | — | SECTORES CRITICOS DE SINIESTRALIDAD VIAL |
| `rtxu-twjm` | -1 | divipola | divipola:cod_dane_depto|divipola:cod_dane_municipio | — | Distritos de Riego activos |
| `s87b-tjcc` | -1 | divipola | divipola:codigodanemunicipiopredio | — | Estadísticas Solicitudes Restitución Discriminadas Municipios |
| `tmmn-mpqc` | -1 | bpin | bpin:bpin | — | DNP - caracterización demográfica de beneficiarios |
| `upd7-iccu` | -1 | divipola | divipola:codigo_municipio | — | Oferta Formativa en Metrología - Instituto Nacional de Metrología |
| `ux6v-gpit` | -1 | divipola | divipola:codigo_municipio|divipola:codigo_departamento | — | Población Intramural a Cargo del INPEC por Códigos DIVIPOLA |
| `v488-qa3u` | -1 | divipola | divipola:cod_dane_depto | — | MEN_INDICADORES_EDUCACION_MEDIA |
| `v5z5-e88h` | -1 | divipola | divipola:divipola_municipio | — | MEN_INDICE_PARIDAD_POR_GENERO_COBERTURA_BRUTA_ETC |
| `xcga-ji4i` | -1 | divipola | divipola:cod_dpto|divipola:cod_mun | — | Reporte de movilización mensual por municipio y departamento |
| `xm4c-2rks` | -1 | divipola | divipola:sic_codigo_dane | — | Solicitudes de conciliación |
| `y9ga-zwzy` | -1 | divipola | divipola:c_digo_delmunicipio | — | MEN_ESTADISTICAS MATRICULA POR MUNICIPIOS_ES |

## no_join

No recognized join key found; likely not useful for entity-linked pipeline

| dataset_id | rows | cols | join_keys | name |
|---|---:|---:|---|---|
| `27ia-hnnb` | -1 | 27 | — | Artesanias de Colombia S.A. - BIC. Registro Activos de información |
| `2fn4-ai3h` | -1 | 9 | — | Mesas Sectoriales SENA Base de Datos |
| `2iqs-g9cv` | -1 | 9 | — | Funcionarios del Cuerpo Diplomático y Consular acreditado en Colombia |
| `36ay-utxu` | -1 | 4 | — | UGPP - Tasa de Evasión Aportes al Sistema de la Protección Social |
| `3faq-g9ig` | -1 | 7 | — | Demandas de Protección al Consumidor admitidas y terminadas por la Delegatura para Asuntos Jurisdiccionales de la Superintendencia de Industria y Comercio |
| `3fbf-wags` | -1 | 7 | — | Notificaciones por aviso y personales de la Superfinanciera |
| `3wdf-ypyy` | -1 | 5 | — | Servicios de Calibración Prestados - Instituto Nacional de Metrología |
| `489x-mu3c` | -1 | 2 | — | Número de Casos de Niños, Niñas y Adolescentes (NNA) reportados en SIRITI por cada Tipo de Vulnerabilidad a Nivel Nacional. |
| `4cym-4b76` | -1 | 8 | — | Directorio Consultorios Jurídicos de las Instituciones de Educación Superior |
| `4e7j-65ci` | -1 | 14 | — | Programa Escuelas Deportivas Para Todos |
| `4hrb-y62g` | -1 | 10 | — | MEN_ESTADISTICAS DE MATRICULA POR DEPARTAMENTOS_ES |
| `4iyy-8sme` | -1 | 17 | — | Consolidado Escenarios Mindeporte |
| `4mka-qdvc` | -1 | 8 | — | Índice Información Clasificada Reservada |
| `4vvt-vbpv` | -1 | 14 | — | Patentes presentadas |
| `58aq-7nep` | -1 | 67 | — | Población Privada de la Libertad por Rango de Edades, Sexo y Situación Jurídica en Establecimientos de Reclusión |
| `58pz-wqvv` | -1 | 35 | — | Vista Inventario y Clasificación de Activos de Información - INVEMAR Macroproceso Procesos Gerenciales |
| `5dpu-htqu` | -1 | 37 | — | Reporte Índice de Calidad de Aguas Marinas y Costeras - ICAM |
| `62tk-nxj5` | -1 | 12 | — | Presión Atmosférica |
| `6arb-d547` | -1 | 35 | — | Alimentos del trópico para alimentación animal - AlimenTro |
| `6deq-vx84` | -1 | 2 | — | Averiguaciones Preliminares Iniciadas por Dirección Territorial |
| `6duu-4tms` | -1 | 0 | — | 25 Hojas de Ruta Sectoriales de Datos Abiertos Estratégicos 2025 - 2026 |
| `6gnm-kj8d` | -1 | 13 | — | Indice de Información Clasificada y Reservada Icfes 2022 |
| `6hcf-xdqm` | -1 | 6 | — | Listado general del estado de las licencias de Cannabis Psicoactivo, No Psicoactivo y Semillas en Colombia |
| `6kwm-9788` | -1 | 5 | — | Resultados Saber Pro Competencias Especificas 2019-2 |
| `7p9a-zd9k` | -1 | 7 | — | Directorio de Centros de Conciliación, Arbitraje, Amigable Composición e Insolvencia |
| `7q36-mkp5` | -1 | 17 | — | Inscripciones Electorales en el exterior |
| `7swi-ievq` | -1 | 8 | — | Costos Faena de Pesca año 2020 |
| `832u-c3pg` | -1 | 17 | — | Consolidado acotamiento de Ronda Hídrica |
| `89wx-5p2g` | -1 | 7 | — | Registro de activos de Información |
| `8dk5-vjaw` | -1 | 13 | — | Esquema de publicación |
| `8iut-hgij` | -1 | 5 | — | PQRSD Ministerio del Trabajo |
| `8pqf-rmzr` | -1 | 8 | — | Total Nacional inscritos en la agencia pública de empleo SENA |
| `96sh-4v8d` | -1 | 9 | — | Entradas de extranjeros a Colombia |
| `9aan-wm8m` | -1 | 9 | — | Base de datos relacionada con madera movilizada proveniente de Plantaciones Forestales Comerciales |
| `9m2f-pdxx` | -1 | 16 | — | Licencias de Cannabis otorgadas por el Ministerio de Justicia y del Derecho |
| `9n7g-3red` | -1 | 16 | — | Patentes publicadas |
| `9yh6-ggh4` | -1 | 16 | — | Iniciativas de Proyectos de Inversión en Agua Potable y Saneamiento Básico |
| `9z8f-npmd` | -1 | 7 | — | Base de Datos en RNBD |
| `9zpk-v2mx` | -1 | 8 | — | Registro Activos de Información Icfes 2023 |
| `afdg-3zpb` | -1 | 12 | — | Temperatura Mínima del Aire |
| `azeg-sgqg` | -1 | 11 | — | SECOP I - PAA Detalle |
| `b346-rc9m` | -1 | 3 | — | Consultas Verbales por Dirección Territorial |
| `bdmn-sqnh` | -1 | 12 | — | Nivel Instantáneo del rio |
| `bee9-sdwx` | -1 | 6 | — | Disparidad de Horas Trabajadas Hombres y Mujeres |
| `bgt6-3icu` | -1 | 5 | — | Sanciones Ejecutoriadas y NO Ejecutoriadas por Conductas Atentatorias contra el derecho de asociación sindical |
| `bign-27m7` | -1 | 10 | — | Paises que no exigen visa de turismo a colombianos |
| `bmcc-69wd` | -1 | 7 | — | Directorio de Casas de Justicia y Centros de Convivencia Ciudadana. |
| `btz9-gir8` | -1 | 13 | — | Solicitudes de Arbitraje |
| `bvqm-qcni` | -1 | 14 | — | Indice de Información Clasificada y Reservada - Ministerio de Justicia y del Derecho |
| `bx26-pux4` | -1 | 11 | — | Registro_PQRS_Mindeportes |
| `ccvq-rp9s` | -1 | 12 | — | Temperatura Máxima del Aire |
| `cf7h-nvwj` | -1 | 3 | — | Asistencias preventivas por Dirección Territorial |
| `cff6-j795` | -1 | 8 | — | Registro de activos de información 2022 |
| `cgyu-cds3` | -1 | 15 | — | Perfil de aspirantes a la carrera diplomática y consular |
| `ch4u-f3i5` | -1 | 32 | — | Resultados de Análisis de Laboratorio Suelos en Colombia |
| `cnh8-i5yd` | -1 | 11 | — | Oficinas de expedición de trámites de Cancillería - Ministerio de Relaciones Exteriores |
| `cqdw-gd52` | -1 | 0 | — | Mesas Sectoriales SENA |
| `d5w5-gqs5` | -1 | 7 | — | Historial Memorias de Oficio |
| `d76u-8x6w` | -1 | 5 | — | Postulados según situación penitenciaria. |
| `dr5c-eewa` | -1 | 0 | — | Mortalidad en vias |
| `dvz4-4xcz` | -1 | 0 | — | Sistematizacion de tramites de VPD |
| `dwbg-geen` | -1 | 13 | — | Inventario documental de historias laborales de la Dirección Nacional de Estupefacientes - DNE |
| `e97j-vuf7` | -1 | 12 | — | Colombianos detenidos en el exterior |
| `ef7j-amu8` | -1 | 11 | — | Registro de Activos de Información |
| `efw5-jiej` | -1 | 9 | — | Salidas de colombianos desde el territorio nacional |
| `egce-jd6s` | -1 | 5 | — | Directorio de funcionarios habilitados para conciliar |
| `enmx-7kvv` | -1 | 11 | — | MEN_MATRICULA_MIGRANTES_EN_EDUCACION_BASICA_Y_MEDIA |
| `f4a5-ab9q` | -1 | 11 | — | Seguimiento a la Ejecución Presupuestal del Sector Justicia |
| `fd7n-f4jp` | -1 | 19 | — | USPEC Alimentación PPL |
| `fdir-hk5z` | -1 | 16 | — | Tratados internacionales de Colombia |
| `feim-cysj` | -1 | 7 | — | Proyectos de ley del Senado de la Republica |
| `fiev-nid6` | -1 | 9 | — | Lista de normas cargadas en el Sistema Único de Información Normativa SUIN-Juriscol |
| `fn2v-r4gu` | -1 | 12 | — | Hoja de Ruta Nacional de datos abiertos estratégicos del estado colombiano |
| `fr9f-9g4b` | -1 | 50 | — | Encuesta Motociclistas |
| `fs36-azrv` | -1 | 11 | — | Registro de Sanciones Contadores |
| `fuyf-sb4r` | -1 | 4 | — | Procesos en Casas de Justicia |
| `fz2j-qt75` | -1 | 0 | — | Probabilidad de accidentalidad |
| `g7ps-wzb3` | -1 | 11 | — | Países que no requieren visa de turismo para ingresar a Colombia |
| `gi6t-dtvg` | -1 | 14 | — | Índice de información clasificada y reservada |
| `gium-im4r` | -1 | 3 | — | Recursos de Gobierno al Sistema de Naciones Unidas para el Desarrollo Sostenible |
| `gkbc-gw7x` | -1 | 4 | — | Desmovilizaciones colectivas e individuales |
| `gnut-8jsz` | -1 | 4 | — | Cantidad de pensionados de Colpensiones por tipo de pensión |
| `h468-umfj` | -1 | 10 | — | Registro de Activos de Información UGPP |
| `h7zg-qcya` | -1 | 9 | — | Esquema de publicación de información INPEC |
| `h8ui-2iee` | -1 | 5 | — | Número de pensionados de Colpensiones por rango salarial 2 |
| `h9p8-6xks` | -1 | 8 | — | Capacitaciones virtuales Archivo General de la Nación |
| `hdnf-a76p` | -1 | 3 | — | Número de pensionados en Colpensiones desagregado por departamento y municipio |
| `hex8-i6ry` | -1 | 12 | — | Unidades de las Áreas Coralinas (Polígonos) |
| `hf6d-emrx` | -1 | 6 | — | Disparidad Salarial Hombres Mujeres |
| `hjfm-ynaz` | -1 | 4 | — | Cantidad de Conciliadores en Equidad Nombrados por Municipio y Departamento |
| `hxkn-cb2p` | -1 | 9 | — | Esquema de publicación de información - UGPP |
| `ivxt-5jyc` | -1 | 0 | — | 1. Hoja de Ruta Nacional de Datos Abiertos 2025 -2026 |
| `jh3r-qtfq` | -1 | 5 | — | Sanciones no Ejecutoriadas y Ejecutoriadas por Dirección Territorial |
| `jvtd-3dgy` | -1 | 2 | — | Pensionados por entidad administradora de Colombia |
| `kccg-dij4` | -1 | 9 | — | Directorio de entidades avaladas para formación en conciliación y/o insolvencia |
| `kfcm-k5vw` | -1 | 30 | — | Seguimiento acumulado de actividades de participación MinJusticia |
| `khhm-wccm` | -1 | 4 | — | Reporte por departamento y tipo de intermediario relacionados en el Registro Único de Intermediarios de seguros en el ramo de riesgos laborales (RUI) - Del Grupo de Promoción y prevención de la Dirección de Riesgos de Laborales del Ministerio del Trabajo |
| `ksje-ycct` | -1 | 12 | — | Expedición de trámites en el exterior |
| `ksrx-5cr8` | -1 | 17 | — | Índice de Información Clasificada y Reservada |
| `kzk3-8dnx` | -1 | 4 | — | Genero y nacimiento de contadores por ciudad |
| `m3fg-eigf` | -1 | 2 | — | Investigaciones Administrativas Iniciadas por Dirección Territorial |
| `m4tv-4zwt` | -1 | 6 | — | Cuadro de clasificación documental |
| `m9r3-29qp` | -1 | 18 | — | Indice de Información Clasificada y Reservada |
| `mjt9-2zwc` | -1 | 25 | — | Población Privada de la Libertad por Nivel Académico, Sexo y Situación Jurídica en Establecimientos de Reclusión |
| `mqxk-srbz` | -1 | 6 | — | Control Administrativo de Sustancias y Productos Químicos ejercido por el Ministerio de Justicia y del Derecho |
| `msu7-rjqd` | -1 | 7 | — | Solicitudes de insolvencia |
| `muyy-6yw9` | -1 | 9 | — | MEN_DIRECTIVOS_DE_INSTITUCIONES_DE_EDUCACIÓN_SUPERIOR |
| `mxqg-ytrw` | -1 | 59 | — | MEN_INDICE_PARIDAD_POR_GENERO_PARA_ETNICOS |
| `nftv-7mae` | -1 | 9 | — | Esquema de Publicación de Información Migración Colombia - UAEMC |
| `np5z-haxm` | -1 | 5 | — | Cantidad de pensionados de Colpensiones por rango de edad |
| `npcj-hpw7` | -1 | 8 | — | Gestión de PQRS unificado Minjusticia |
| `p62d-8cf3` | -1 | 17 | — | Asistencias brindadas a connacionales en el exterior |
| `pgrh-8um9` | -1 | 11 | — | MEN_DOCENTES_OFICIALES_EPBM |
| `pqdu-ej7f` | -1 | 10 | — | Predios Beneficiarios PIDAR |
| `pt9a-aamx` | -1 | 12 | — | Nivel Mínimo del Rio |
| `pvyw-9yqs` | -1 | 0 | — | Reportes Portal Nacional Datos Abiertos |
| `pzu3-75kc` | -1 | 5 | — | Registo Activos Informacion FOGACOOP |
| `qr2y-27z8` | -1 | 13 | — | Inventario documental de historias laborales del Ministerio de Justicia y del Derecho |
| `qwzh-2su3` | -1 | 13 | — | Índice de información clasificada y reservada |
| `r29n-7bcm` | -1 | 5 | — | Sanciones Ejecutoriadas Y No Ejecutoriadas Por Intermediación Laboral Indebida por Dirección Territorial |
| `rqjn-d9f5` | -1 | 9 | — | Censo Nacional de Archivos |
| `rues-vdf5` | -1 | 11 | — | Misiones acreditadas en Colombia |
| `s34r-vs6z` | -1 | 7 | — | Actos administrativos de formalización de los Sistemas Locales de Justicia |
| `s44d-v5fw` | -1 | 4 | — | Desmovilizaciones por régimen legal |
| `s54a-sgyg` | -1 | 12 | — | Precipitación |
| `s64k-xyiz` | -1 | 9 | — | Esquema de Publicación de Información- Ministerio del Trabajo |
| `sbwg-7ju4` | -1 | 12 | — | Temperatura Ambiente del Aire |
| `sgfv-3yp8` | -1 | 12 | — | Velocidad del Viento |
| `sk9s-yxmi` | -1 | 6 | — | Afiliados al sistema general de pensiones de Colombia |
| `tj5j-bwvg` | -1 | 8 | — | Registro de activos de información del Ministerio de Justicia y del Derecho |
| `u8du-s7mh` | -1 | 18 | — | Inventario de activos de información del MEN |
| `u8iw-uggt` | -1 | 15 | — | Índice de Información Clasificada y Reservada de la Superfinanciera |
| `uds4-jdij` | -1 | 10 | — | DNP-plandesarrollo |
| `udsq-ti2d` | -1 | 35 | — | Inventario y Clasificación de Activos de Información - INVEMAR |
| `uext-mhny` | -1 | 12 | — | Humedad del Aire |
| `unak-3uez` | -1 | 0 | — | Fallecidos segun tramo y constructora |
| `us2n-5jaf` | -1 | 4 | — | Desmovilizaciones por indulto o amnistía |
| `uzcf-b9dh` | -1 | 57 | — | Asset Inventory - Public |
| `v29c-fg4a` | -1 | 5 | — | Países que apostillan y legalizan documentos |
| `v3rx-q7t3` | -1 | 25 | — | Densidad de Cultivos de Coca - Subdirección Estratégica y de Análisis - Ministerio de Justicia y del Derecho |
| `v8sa-zb2t` | -1 | 12 | — | Unidades  de las Áreas Coralinas  (Punto) |
| `vbf6-6bqx` | -1 | 10 | — | USPEC Esquema de Publicación Información |
| `vfth-yucv` | -1 | 12 | — | Nivel Máximo del Rio |
| `w8hf-jz4a` | -1 | 16 | — | Patentes concedidas |
| `wavk-2hmm` | -1 | 4 | — | Desmovilizaciones por grupo armado |
| `wziy-gz47` | -1 | 8 | — | Asistencias técnicas Gestión Documental Archivo General de la Nación |
| `x7wx-67fu` | -1 | 8 | — | Activos de Información de la SFC |
| `xbc7-65j4` | -1 | 6 | — | Listado de Errores Frecuentes de Calidad |
| `xbk8-edux` | -1 | 8 | — | Reporte de movilización mensual Intermediario Financiero |
| `xfsi-rqje` | -1 | 4 | — | Número de afiliados a Colpensiones desagregado por departamento y municipio |
| `xhzc-us2q` | -1 | 7 | — | Registros de Activos de la Información |
| `xqsb-y246` | -1 | 18 | — | USPEC Plan Anual de Adquisiciones |
| `xs69-evan` | -1 | 2 | — | Estadisticas NNA Registrados en SIRITI Nivel Nacional Clasificados por Actividad Economica en la que se Encuentra Trabajando |
| `y399-rzwf` | -1 | 14 | — | Connacionales inscritos en el Registro Ciudadano en Línea |
| `y7va-ab6n` | -1 | 8 | — | Registro de activos de información |
| `ybqk-8s42` | -1 | 15 | — | Sectores críticos mortalidad 2022 |
| `yd42-ttr4` | -1 | 9 | — | Mesas Sectoriales SENA |
| `yh8b-89bi` | -1 | 7 | — | Caracterización de Personas en Casas de Justicia |
| `yix6-7yeh` | -1 | 6 | — | Población NINI Entre 18 Y 28 Años |
| `yj6h-yjbu` | -1 | 19 | — | Comportamiento del trámite de expedición de visa colombiana a nacionales de Venezuela |
| `yjvt-9cab` | -1 | 1 | — | Pensionados en Colombia |
| `ykx8-9qcu` | -1 | 10 | — | Esquema de Publicación de Información ICFES 2022 |
| `yn26-hitk` | -1 | 5 | — | Sanciones no Ejecutoriadas y Ejecutoriadas por sectores económicos |
