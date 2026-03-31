# Hallazgos vivos de la ola Colombia `br-acc` (2026-03-30)

## Estado de carga validado

Capas nuevas ya cargadas en Neo4j:

- `JudicialCase`: `28,515`
- `ActoAdministrativo`: `117`
- `GacetaTerritorial`: `1,569`
- `Inquiry`: `2,405`
- `InquiryRequirement`: `128`
- `InquirySession`: `2,292`

Capas en carga al momento de este memo:

- `TVECOrder`: carga viva en progreso
- `EnvironmentalFile`: recarga viva en progreso con la ruta optimizada por `document_id`

## Hallazgos reales ya validados

### 1. Captura multientidad en TVEC con eco en SECOP

Fuente base: `rgxm-mmea` (Tienda Virtual del Estado Colombiano - Consolidado).

Se filtro `estado IN ('EMITIDO','EMITIDA','ENVIADO','PUBLICADO','FINALIZADO')` y se agrego por `nit_proveedor + proveedor + agregacion`.

#### PANAMERICANA OUTSOURCING S.A. (`8300776556`)

- TVEC:
  - `555` entidades
  - `1,371` ordenes
  - `COP 27.51B`
  - agregacion principal: `GRANDES SUPERFICIES`
- Grafo contractual existente:
  - `73` contratos
  - `73` compradores
  - `COP 171.92B`
- Patrones ya activos en SECOP:
  - `split_contracts_below_threshold`
  - monto agregado del patron: `COP 128.10M`

#### PROVEER INSTITUCIONAL SAS (`900365660`)

- TVEC:
  - `421` entidades
  - `988` ordenes
  - `COP 21.78B`
  - agregacion principal: `GRANDES SUPERFICIES`
- Grafo contractual existente:
  - `365` contratos
  - `213` compradores
  - `COP 13.49B`
- Patrones ya activos en SECOP:
  - `split_contracts_below_threshold`
  - monto agregado del patron: `COP 96.46M`

#### DISTRACOM S.A. (`811009788`)

- TVEC:
  - `341` entidades
  - `745` ordenes
  - `COP 146.37B`
  - agregacion principal: `COMBUSTIBLE (NACIONAL) III`
- Grafo contractual existente:
  - `723` contratos
  - `410` compradores
  - `COP 388.04B`
- Patrones ya activos en SECOP:
  - `split_contracts_below_threshold`
  - monto agregado del patron: `COP 130.01M`

#### ORGANIZACION TERPEL S.A. (`830095213`)

- TVEC:
  - `252` entidades
  - `562` ordenes
  - `COP 146.24B`
  - agregacion principal: `COMBUSTIBLE (NACIONAL) III`
- TVEC adicional:
  - `120` entidades
  - `213` ordenes
  - `COP 252.71B`
  - agregacion: `COMBUSTIBLE (BOGOTA) II`
- Grafo contractual existente:
  - `169` contratos
  - `168` compradores
  - `COP 1.93T`
- Patrones ya activos en SECOP:
  - `split_contracts_below_threshold`
  - monto agregado del patron: `COP 93.00M`
  - `contract_concentration`
  - monto agregado del patron: `COP 390.46B`

#### SUMIMAS S.A.S. (`830001338`)

- TVEC:
  - `218` entidades
  - `401` ordenes
  - `COP 7.73B`
- Grafo contractual existente:
  - `264` contratos
  - `176` compradores
  - `COP 692.06B`
- Patrones ya activos en SECOP:
  - `split_contracts_below_threshold`
  - monto agregado del patron: `COP 90.42M`

#### CENCOSUD COLOMBIA S.A. (`900155107`)

- TVEC:
  - `151` entidades
  - `376` ordenes
  - `COP 14.48B`
- Grafo contractual existente:
  - `102` contratos
  - `102` compradores
  - `COP 3.02B`
- Patrones ya activos en SECOP:
  - `split_contracts_below_threshold`
  - monto agregado del patron: `COP 343.78M`

### 2. Lo que esto si significa

Estos casos no prueban por si solos corrupcion. Si prueban algo mas util:

- hay proveedores con alcance multientidad muy alto dentro de un mismo canal agregado de compra publica;
- varios de esos mismos proveedores ya presentan patrones de riesgo contractuales en SECOP;
- la combinacion `captura de mercado TVEC + patrones previos SECOP` merece priorizacion investigativa.

### 3. Lo que no salio aun

- En pruebas manuales iniciales, `ICONTEC` y `PANAMERICANA LIBRERIA Y PAPELERIA` no devolvieron hits directos por string dentro de `JudicialCase`.
- El patron `beneficiario_bpin_o_regalias_contrata` todavia no esta listo para hallazgos publicos fuertes porque la capa `Project` necesita una recarga dedicada y el cruce limpio por `BPIN` sigue incompleto.

## Lectura operativa

La primera ola realmente util no fue `judicial` ni `ambiental`. Fue `TVEC`.

El hallazgo nuevo no es solo "proveedor grande"; eso ya se sabia. El hallazgo nuevo es:

- un mismo proveedor aparece repetidamente en muchas entidades dentro del mismo instrumento agregado;
- y al cruzarlo con SECOP ya no queda como simple volumen, sino como proveedor con senales adicionales de fragmentacion o concentracion.

## Proximo paso util

1. Terminar la carga completa de `TVECOrder` para correr `tvec_multi_entity_capture` desde el propio grafo.
2. Reintentar `environmental_files` completo con la ruta optimizada.
3. Recargar la capa `Project/BPIN` para cerrar `beneficiario_bpin_o_regalias_contrata`.
4. Convertir los casos TVEC de arriba en leads publicos con dossier corto y evidencia enlazada.
