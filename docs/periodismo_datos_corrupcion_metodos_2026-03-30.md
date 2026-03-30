# Periodismo de Datos Anticorrupción: Métodos Reales y Qué Podemos Replicar

Fecha: `2026-03-30`

## Pregunta

Cómo encuentran corrupción los periodistas de datos en el mundo y en Colombia, y cuánto de eso podemos replicar en este repo usando `datos abiertos`.

## Respuesta corta

Sí podemos replicar una parte importante de los métodos periodísticos más útiles, pero no todos en el mismo nivel.

Lo que el periodismo de investigación hace mejor no es “adivinar corrupción”, sino combinar:

1. `datos estructurados`
2. `cruces entre registros`
3. `banderas rojas`
4. `expedientes y documentos`
5. `verificación humana`
6. `peticiones de información, filtraciones o fuentes`

Con nuestro stack actual ya podemos replicar bastante bien los puntos `1` a `3`, parcialmente el `4`, y todavía somos débiles en `5` y `6`.

## Qué hacen los periodistas de datos anticorrupción

### 1. Siguen la contratación pública como rastro principal del dinero

Esta es la práctica más repetida globalmente.

Los patrones más comunes:

- contratación con `un solo oferente`
- empresas creadas poco antes de ganar contratos
- `pliegos sastre`
- fragmentación de compras para evitar competencia
- cronogramas imposibles
- sobrecostos
- contratos suspendidos, adicionados o nunca terminados
- concentración de contratos en pocos jugadores

Fuentes:

- GIJN documenta que periodistas de África Oriental parten de portales de contratación, limpian datos viejos, comparan registros y preguntan `“Who benefited from this contract?”`; además cruzan contratos con dueños, presupuestos y visitas de campo.  
  https://gijn.org/stories/investigating-corruption-procurement-data-africa/
- Open Contracting Partnership formaliza esta práctica como `red flags` y publica una metodología con `73` indicadores desde planeación hasta ejecución.  
  https://www.open-contracting.org/resources/red-flags-in-public-procurement-a-guide-to-using-data-to-detect-and-mitigate-risks/
- El caso de Kazajistán documentado por OCP muestra que los equipos de investigación y control cruzan portal de compras, tesorería, registro mercantil y otras bases para calcular indicadores automáticos.  
  https://www.open-contracting.org/2021/09/20/how-one-data-team-is-rooting-out-procurement-corruption-in-kazakhstan/

### 2. Conectan contratos con poder político, dueños reales y redes

No se quedan en “esta empresa ganó”.

Buscan:

- representantes legales
- accionistas o beneficiarios
- direcciones, teléfonos o correos compartidos
- vínculos con funcionarios, congresistas, alcaldes o partidos
- familiares o terceros como pantallas

Fuentes:

- GIJN resume esta lógica así: cruzar `company records`, `court papers` y `procurement records`; incluso detalles pequeños como teléfonos compartidos pueden revelar conexiones ocultas.  
  https://gijn.org/stories/investigating-corruption-procurement-data-africa/
- OCCRP publicó `¡Sigan el dinero!`, un manual precisamente para rastrear `empresas, propiedades, contratos públicos y fallos`, usando registros mercantiles, inmobiliarios, expedientes judiciales, PEPs y solicitudes de acceso a información.  
  https://www.occrp.org/en/announcement/occrp-publishes-spanish-language-follow-the-money-handbook
- OCCRP describe Aleph como infraestructura para `track assets`, `uncover corporate ownership` y analizar transacciones usando `government records`, `leaked archives` y `open data`.  
  https://www.occrp.org/en/announcement/occrp-announces-a-new-chapter-for-its-investigative-data-platform-aleph-pro

### 3. Cruzan filtraciones o documentos masivos con registros públicos

Este es el método de ICIJ, OCCRP y grandes consorcios internacionales.

La lógica:

- extraer estructura de millones de documentos
- convertir documentos a entidades
- generar grafos de personas, empresas, direcciones y activos
- cruzar contra listas de sanciones, registros mercantiles, PEPs y otros datasets públicos

Fuentes:

- ICIJ explica que en `Pandora Papers` estructuró datos no estructurados, los llevó a `Neo4j` y `Linkurious`, y luego los cruzó con `sanctions lists`, fugas anteriores, registros corporativos públicos, listas de multimillonarios y líderes políticos.  
  https://www.icij.org/investigations/pandora-papers/about-pandora-papers-leak-dataset/
- GIJN documentó que en `Panama Papers` ICIJ transformó SQL a `Neo4j`, visualizó redes y permitió que reporteros consultaran conexiones a varios grados de distancia.  
  https://gijn.org/stories/the-people-and-the-technology-behind-the-panama-papers/
- ICIJ también documentó mejoras de búsqueda y georreferenciación en Offshore Leaks, incluyendo corrección de errores tipográficos y matching geográfico de direcciones.  
  https://www.icij.org/inside-icij/2013/10/users-can-now-search-country-icij-offshore-leaks-database/

### 4. Usan auditorías, hallazgos y expedientes oficiales como multiplicadores

Muchos hallazgos periodísticos no empiezan con una licitación rara, sino con:

- informes de auditoría
- debates de control político
- decisiones disciplinarias
- fallos fiscales
- expedientes judiciales

Luego el periodista reconstruye la cadena documental alrededor de ese hallazgo.

Fuente:

- GIJN muestra el caso KEMSA en Kenia: un informe de auditoría y audiencias parlamentarias activaron seguimiento periodístico al rastro documental de proveedores vinculados a contratos de COVID.  
  https://gijn.org/stories/investigating-corruption-procurement-data-africa/

### 5. Verifican en terreno si la obra, servicio o entrega existe

La literatura periodística es consistente en esto: los números solos no bastan.

Se contrasta:

- contrato vs presupuesto
- contrato vs ejecución
- contrato vs beneficiario real
- contrato vs obra/servicio observado

Fuentes:

- GIJN enfatiza que hay que comparar registros de contratación con presupuestos, registros empresariales, documentos filtrados y `field visits`; “numbers alone do not mean anything if there is no real work.”  
  https://gijn.org/stories/investigating-corruption-procurement-data-africa/
- Consejo de Redacción insiste en que cada investigación debe entender el proceso completo, sus `banderas rojas`, y bajar eso a una reportería concreta sobre necesidad, costo, beneficiarios, planeación, plazos y ejecución.  
  https://consejoderedaccion.org/wp-content/uploads/2024/01/cdr-corrupcion-digital-final.pdf

### 6. Piden lo que no está publicado

El dato abierto rara vez cierra por sí solo el caso.

Los periodistas serios usan:

- derecho de petición / acceso a información
- expedientes completos
- anexos técnicos
- actas
- soportes de pagos
- registros mercantiles o inmobiliarios
- listas internas o bases exportadas

Fuentes:

- Consejo de Redacción trata la opacidad misma como `bandera roja` y subraya que todo lo que no aparece donde debería aparecer merece ser pedido formalmente.  
  https://consejoderedaccion.org/wp-content/uploads/2024/01/cdr-corrupcion-digital-final.pdf
- OCCRP estructura su manual latinoamericano alrededor de `FOI`, registros mercantiles, propiedad, contratación, cortes y PEPs.  
  https://www.occrp.org/en/announcement/occrp-publishes-spanish-language-follow-the-money-handbook

## Qué hacen específicamente en Colombia

### Consejo de Redacción: banderas rojas + rutas documentales

La guía `Pistas para investigar las rutas de la corrupción` es probablemente la síntesis metodológica más útil para Colombia.

Lo central de su enfoque:

- partir del `proceso completo`
- entender `la ley y la ruta administrativa`
- tratar el secretismo como alerta
- usar `SECOP`, `SIGEP`, `RUP`, registros mercantiles, catastros, procesos judiciales y derecho de petición
- revisar necesidad, justificación jurídica, cuantía, beneficiarios, planeación, publicación, cronogramas y fragmentación

La guía también aterriza banderas rojas concretas:

- adjudicación mal motivada
- urgencia injustificada
- publicidad insuficiente
- criterios de selección irregulares
- contratación directa donde debía haber competencia
- fragmentación del gasto
- proyectos grandes sin precios unitarios claros
- beneficiarios opacos
- proyectos fuera del plan o surgidos de la nada

Fuente:

- https://consejoderedaccion.org/wp-content/uploads/2024/01/cdr-corrupcion-digital-final.pdf

### Cuestión Pública: SECOP + Cámara de Comercio + contexto político + crecimiento patrimonial

En investigaciones como `We Love Mery Janneth Gutiérrez`, Cuestión Pública rastrea:

- contratos en SECOP a lo largo de varios años
- constitución y cambios societarios
- patrimonio y crecimiento empresarial
- relaciones políticas y familiares
- cronología de cargos públicos y adjudicaciones

No es solo una consulta en SECOP; es un cruce entre contratación, registro empresarial y redes de influencia.

Fuente:

- https://cuestionpublica.com/we-love-mery-janneth-gutierrez/

### La Silla Vacía y otros medios locales: concentración, megacontratistas y módulos

En coberturas territoriales, medios colombianos suelen revisar:

- todos los contratos por encima de un umbral
- concentración de montos en pocos contratistas
- módulos o lotes simultáneos que funcionan como un gran paquete
- anticipos, vigencias futuras y crédito a proveedor

Un ejemplo claro es la investigación de La Silla Vacía sobre el entramado contractual de Álex Char, basada en revisión sistemática de contratos grandes de SECOP.  
Fuente:

- https://www.lasillavacia.com/silla-nacional/caribe/el-entramado-de-la-contratacion-de-alex-char/

## Qué de esto ya podemos replicar en este repo

Verificado localmente en `http://127.0.0.1:8000/api/v1/meta/sources` el `2026-03-30`:

- `52` fuentes expuestas
- `51` cargadas
- universo cargado especialmente fuerte en:
  - `contracts`: `20`
  - `budget`: `10`
  - `sanctions`: `5`
  - `identity`: `4`
  - `disclosures`: `2`
  - `public_sector`: `2`

### Matriz de replicabilidad

| Método periodístico | ¿Se puede replicar con datos abiertos? | Estado en el repo | Qué ya tenemos | Qué falta |
| --- | --- | --- | --- | --- |
| Red flags de contratación | `Sí` | `Fuerte` | SECOP I/II procesos, contratos, ofertas, adiciones, suspensiones, ejecución, facturas, plan de pagos, sanciones, archivos | Falta sistematizar más indicadores: un solo oferente, plazos imposibles, precios atípicos, cesiones, anticipos |
| Conectar contratos con funcionarios y empresas | `Sí` | `Fuerte` | RUES, Cámara, sucursales, SIGEP, declaraciones de activos y conflictos, proveedores SECOP | Falta parentesco y beneficiario final real |
| Proveedor sancionado que sigue contratando | `Sí` | `Fuerte` | SIRI, fiscal responsibility, fiscal findings, PACO, SECOP sanctions, contract history | Falta cortes y expedientes completos |
| Elefante blanco / obra trabada | `Sí, parcialmente` | `Medio-fuerte` | DNP, MapaInversiones, SGR, BPIN, ubicaciones, ejecución, suspensiones, archivos SECOP | Falta verificación sistemática en campo, fotos, satélite o visitas |
| Campaña política -> contratación | `Sí, parcialmente` | `Medio` | Cuentas Claras 2019 + contratación | Falta más ciclos electorales y gasto, no solo ingresos |
| Declaraciones vs negocios corporativos | `Sí, parcialmente` | `Medio` | asset_disclosures + conflict_disclosures + company registry + public servants | Falta parser más profundo de textos y mejor entity resolution |
| Auditoría pública -> reconstrucción del caso | `Sí, parcialmente` | `Medio` | official_case_bulletins, fiscal findings, TransMilenio OCR/manual parse | Falta pipeline genérico para PDFs, Word, Excel, OCR |
| Microdesfalco contable / tesorería | `Parcial` | `Débil-medio` | auditorías e informes públicos puntuales, módulo TransMilenio | Faltan auxiliares, órdenes de pago, cheques, conciliaciones, bitácoras |
| Offshore / beneficiario final oculto | `No, no solo con abiertos nacionales` | `Débil` | solo cruces corporativos locales y algunos assets | Faltan fugas, registros BO, datos offshore externos |
| Propiedad / enriquecimiento por inmuebles | `Parcial` | `Débil` | IGAC transactions agregadas | Faltan folios, propietarios históricos, catastros más ricos, notarías |
| Nepotismo / familiares | `Parcial` | `Débil` | algunos solapes por apellidos o redes públicas | Falta registro fiable de parentesco |

## Donde ya estamos alineados con métodos periodísticos reales

Nuestras prácticas no están inventadas al azar. Varias ya calzan con metodologías periodísticas reales:

- `Proveedor con directivo o vínculo en cargo público`
  - periodismo clásico de conflicto de interés
- `Proveedor sancionado que siguió recibiendo contratos`
  - overlay de sanciones + contratación
- `Facturación o pagos por delante de la ejecución`
  - control documental y de flujo de pagos
- `Declaraciones con referencias corporativas o conflictos`
  - cruce de disclosures con registros
- `Donante que también aparece como proveedor`
  - cruce de financiación política con contratación
- `Elefante blanco / obra trabada`
  - desajuste entre promesa pública, inversión y entrega
- `Microdesfalco contable`
  - auditoría pública + reconstrucción de anomalías

## Donde todavía nos falta parecer más a un equipo periodístico de élite

### 1. Un lector documental genérico

Hoy leemos muy bien:

- metadatos de archivos SECOP
- algunos PDFs curados
- algunos expedientes manuales

Pero nos falta un pipeline general para:

- `pdf`
- `docx`
- `xlsx`
- `zip`
- OCR
- tablas
- anexos técnicos

Esto es crítico porque el periodismo real cierra muchos casos en el expediente, no en la tabla.

### 2. Un flujo formal de solicitudes de información

Nos falta convertir “esto no aparece” en:

- solicitud estándar
- checklist de campos
- seguimiento
- respuesta parcial / negación
- ingestión posterior

Esto importa especialmente para:

- tesorería
- auxiliares contables
- órdenes de pago
- actas
- soportes de interventoría
- expediente contractual completo

### 3. Más indicadores de competencia y colusión

Tenemos buena base SECOP, pero aún no explotamos al máximo:

- `single bidder`
- rotación cerrada de oferentes
- oferentes que comparten representante, dirección o contacto
- tiempos de entrega imposibles
- diferencias fuertes entre valor esperado y adjudicado
- cambios reiterados del proceso

Este debería ser uno de los próximos frentes prioritarios.

### 4. Cierre territorial y verificación física

El periodismo de corrupción no se queda en la base:

- visita la obra
- llama a beneficiarios
- compara promesa vs realidad
- revisa fotos satelitales o evidencia local

Nosotros todavía estamos mucho más fuertes en `detección de pista` que en `verificación material`.

### 5. Propiedad, patrimonio y ocultamiento de activos

A nivel global, gran parte del periodismo anticorrupción fuerte termina aquí.

Hoy el repo colombiano aún está flojo en:

- propiedad real
- histórico de inmuebles
- beneficiario final
- sociedades offshore
- relación activo-persona-familia

## Qué sí podemos replicar ya con datos abiertos colombianos

Alta prioridad y alta viabilidad:

1. `Contratos con baja competencia`
   - SECOP ofertas + procesos + contratos
2. `Empresas recién creadas que ganan rápido`
   - Cámara/RUES + SECOP
3. `Concentración territorial de megacontratistas`
   - SECOP + Cámara + sucursales
4. `Obras con dinero girado, cambios y poca entrega`
   - DNP/MapaInversiones/SGR + SECOP ejecución + suspensiones + archivos
5. `Funcionarios o exfuncionarios conectados con proveedores`
   - SIGEP + Cámara + SECOP + disclosures
6. `Sancionados que siguieron contratando`
   - SIRI/fiscales/SECOP sanctions + adjudicaciones
7. `Donantes o financiadores de campañas que luego contratan`
   - Cuentas Claras + SECOP
8. `Pagos / facturas / radicados anómalos`
   - facturas + plan de pagos + archivos SECOP
9. `Fragmentación contractual`
   - procesos/contratos repetidos por objeto, tiempo, entidad y proveedor
10. `Alertas por documentos de supervisión, pago y designación`
   - archivos SECOP ya tipificados

## Qué no debemos vender como si los datos abiertos solos lo resolvieran

- autor intelectual preciso de un esquema de tesorería interna
- parentescos no documentados
- beneficiario final oculto tras varias capas privadas
- propiedades reales detalladas sin acceso registral suficiente
- “corrupción probada” solo por una bandera roja

Eso no invalida el método; solo obliga a separar:

- `pista`
- `anomalía documentada`
- `caso corroborado`

## Backlog recomendado si queremos parecernos más a estas redacciones

### Inmediato

1. Implementar `single bidder`, `short tender`, `fragmentación`, `empresa recién creada`, `rotación cerrada` en la capa viva de ranking.
2. Hacer un `document reader` genérico con OCR y extracción de tablas.
3. Crear un módulo formal de `solicitudes de información`.
4. Añadir `price benchmarking` con facturas, rubros y compromisos.
5. Mejorar `entity resolution` para teléfono, dirección, correo y documento.

### Después

6. Añadir una capa de `field verification` y evidencia territorial.
7. Fortalecer propiedad y patrimonio.
8. Cargar más ciclos de `Cuentas Claras`.
9. Agregar cortes y expedientes disciplinarios/penales donde haya acceso.
10. Preparar ingestión para datasets externos tipo `offshore`, si aparecen cruces relevantes.

## Conclusión

El repositorio ya está bastante alineado con el corazón del periodismo de datos anticorrupción contemporáneo:

- seguir contratación
- cruzar registros
- levantar banderas rojas
- conectar contratos con poder
- usar sanciones y auditorías como anclas

Donde todavía no estamos al nivel de los mejores equipos es en el cierre:

- leer expedientes a escala
- pedir lo que falta
- verificar físicamente
- probar autoría individual
- seguir patrimonio oculto

En otras palabras:

- `sí` podemos replicar una parte seria de sus métodos con datos abiertos
- `sí` ya estamos aplicando varios de esos métodos
- `no` todavía no cerramos igual de bien los casos que combinan open data con expedientes, campo y fuentes

## Fuentes principales

- GIJN, `Following the Digital Trail: How East African Journalists Investigate Corruption through Procurement Data`  
  https://gijn.org/stories/investigating-corruption-procurement-data-africa/
- GIJN, `The People and the Technology Behind the Panama Papers`  
  https://gijn.org/stories/the-people-and-the-technology-behind-the-panama-papers/
- ICIJ, `Pandora Papers: An offshore data tsunami`  
  https://www.icij.org/investigations/pandora-papers/about-pandora-papers-leak-dataset/
- ICIJ, `Users can now search by country in the Offshore Leaks Database`  
  https://www.icij.org/inside-icij/2013/10/users-can-now-search-country-icij-offshore-leaks-database/
- OCCRP, `OCCRP Publishes Spanish-Language “Follow the Money” Handbook`  
  https://www.occrp.org/en/announcement/occrp-publishes-spanish-language-follow-the-money-handbook
- OCCRP, `Aleph Pro`  
  https://www.occrp.org/en/announcement/occrp-announces-a-new-chapter-for-its-investigative-data-platform-aleph-pro
- Aleph docs, `Cross-referencing your data`  
  https://docs.aleph.occrp.org/users/investigations/cross-referencing/
- Open Contracting Partnership, `Red Flags in Public Procurement`  
  https://www.open-contracting.org/resources/red-flags-in-public-procurement-a-guide-to-using-data-to-detect-and-mitigate-risks/
- Open Contracting Partnership, `Anticorruption`  
  https://www.open-contracting.org/anticorruption/
- Open Contracting Partnership, `Dominican Republic red-light for corruption`  
  https://www.open-contracting.org/2023/12/09/a-red-light-for-corruption-how-the-dominican-republic-is-using-open-data-better-processes-collaboration-to-fight-corruption/
- Open Contracting Partnership, `Kazakhstan`  
  https://www.open-contracting.org/2021/09/20/how-one-data-team-is-rooting-out-procurement-corruption-in-kazakhstan/
- Consejo de Redacción, `Pistas para investigar las rutas de la corrupción`  
  https://consejoderedaccion.org/wp-content/uploads/2024/01/cdr-corrupcion-digital-final.pdf
- Cuestión Pública, `We Love Mery Janneth Gutiérrez`  
  https://cuestionpublica.com/we-love-mery-janneth-gutierrez/
- La Silla Vacía, `El entramado de la contratación de Álex Char`  
  https://www.lasillavacia.com/silla-nacional/caribe/el-entramado-de-la-contratacion-de-alex-char/
