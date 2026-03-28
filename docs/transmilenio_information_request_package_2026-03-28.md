# TransMilenio Information Request Package

Date: 2026-03-28

Purpose: convert the current `microdesfalco_contable_transmilenio` lead into a named-actor investigation by requesting the transaction-level records that public PDFs do not expose.

## Current Public Basis

The current lead is supported by official TransMilenio documents from `2023`, `2024`, and `2025`, including:

- OCI-2024-053 on financial and accounting management
- OCI-2025-020 on the PTEP risk map
- treasury annexes for `2024` and `2025`
- management reports for `2023`, `2024`, and `2025`
- budget income, expense, and modification annexes

Public documents already show:

- `10` exceptional payroll cheques for one official for `COP 222,480,296`
- request to lift restrictive cheque seals for teller-window collection
- `COP 2,717,814,586` mismatch between treasury reporting and JSP7
- deletion of CRP sequence `202401-4024` from the JSP7 administrator profile
- repeated public mention of the payment-to-third-party exception route
- large aggregate budget modifications across more than one vigencia

## Ready-To-Send Request Draft

Subject: Solicitud de acceso a informacion publica sobre operaciones contables, tesoreria y trazabilidad de pagos en TRANSMILENIO S.A.

Body:

> Solicito copia y/o exportacion en formato reutilizable de los registros operativos y contables relacionados con la gestion financiera y contable de TRANSMILENIO S.A. para el periodo comprendido entre el `2023-01-01` y el `2025-12-31`, con el fin de verificar la trazabilidad de pagos, conciliaciones, excepciones de tesoreria y registros presupuestales mencionados en documentos oficiales de la entidad y de la Oficina de Control Interno.
>
> Solicito que la informacion sea entregada preferiblemente en `CSV`, `XLSX` o `TXT` cuando se trate de tablas o logs, y en `PDF` cuando se trate de soportes documentales no estructurados. Cuando existan restricciones sobre datos personales o sensibles, solicito una version parcialmente anonimizada, pero manteniendo un identificador estable por registro para permitir el cruce tecnico de la informacion.
>
> La solicitud se refiere unicamente a informacion ya obrante en los sistemas administrativos y documentales de la entidad, sin requerir elaboracion de analisis nuevos por parte de la administracion.

## Exact Tables To Request

### 1. Libro auxiliar contable por tercero y comprobante

Period: `2023-01-01` to `2025-12-31`

Need:

- all accounting movements tied to payments, payroll, treasury, bank movements, accounts payable, and budget affectation
- monthly export or full export
- one row per accounting line

Minimum columns:

- periodo_contable
- fecha_registro
- fecha_documento
- comprobante_tipo
- comprobante_numero
- consecutivo
- cuenta_contable
- subcuenta
- tercero_tipo_documento
- tercero_numero_documento
- tercero_nombre
- dependencia
- centro_costo
- concepto
- valor_debito
- valor_credito
- saldo
- orden_pago_id
- cheque_numero
- transferencia_id
- usuario_registro
- usuario_aprobacion

### 2. Ordenes de pago

Period: `2023-01-01` to `2025-12-31`

Need:

- one row per payment order
- include cancelled, reversed, replaced, and manually adjusted orders

Minimum columns:

- orden_pago_id
- fecha_orden
- estado
- tipo_pago
- beneficiario_tipo_documento
- beneficiario_numero_documento
- beneficiario_nombre
- valor_bruto
- descuentos
- valor_neto
- concepto_pago
- contrato_id
- comprobante_presupuestal
- registro_presupuestal
- fuente_recurso
- medio_pago
- cuenta_bancaria_destino
- cheque_numero
- usuario_creacion
- usuario_aprobacion
- fecha_aprobacion
- orden_reemplazada_id

### 3. Registro de cheques emitidos, anulados, cobrados y reexpedidos

Period: `2023-01-01` to `2025-12-31`

Minimum columns:

- cheque_numero
- fecha_emision
- fecha_cobro
- fecha_anulacion
- estado_cheque
- beneficiario_tipo_documento
- beneficiario_numero_documento
- beneficiario_nombre
- valor
- cuenta_bancaria_origen
- orden_pago_id
- motivo_anulacion
- motivo_reexpedicion
- sello_restrictivo
- levantamiento_sello_fecha
- levantamiento_sello_soporte
- usuario_emision
- usuario_modificacion

Special request:

- soportes completos del caso referido en OCI-2024-053 sobre los `10` cheques de nomina y la solicitud de cobro por ventanilla

### 4. Conciliaciones bancarias

Period: monthly, `2023-01` to `2025-12`

Minimum columns:

- banco
- cuenta_bancaria
- periodo_conciliacion
- saldo_libros
- saldo_banco
- diferencia
- partida_conciliatoria_id
- partida_conciliatoria_tipo
- partida_conciliatoria_valor
- partida_conciliatoria_fecha
- referencia_externa
- orden_pago_id
- cheque_numero
- tercero_identificador
- usuario_cierre
- fecha_cierre

### 5. Bitacora de auditoria del aplicativo JSP7

Period: `2023-01-01` to `2025-12-31`

Minimum columns:

- evento_id
- fecha_hora
- usuario
- perfil
- dependencia
- modulo
- accion
- registro_afectado_tipo
- registro_afectado_id
- valor_anterior
- valor_nuevo
- ip_origen
- observacion

Special request:

- trazas asociadas al consecutivo `202401-4024`
- trazas de eliminacion, modificacion, anulacion y reverso sobre CRP, ordenes de pago y registros de tesoreria

### 6. Relacion de pagos a terceros no titulares

Period: `2023-01-01` to `2025-12-31`

Minimum columns:

- solicitud_id
- fecha_solicitud
- titular_derecho_tipo_documento
- titular_derecho_numero_documento
- titular_derecho_nombre
- tercero_beneficiario_tipo_documento
- tercero_beneficiario_numero_documento
- tercero_beneficiario_nombre
- valor
- motivo
- soporte_notarial
- orden_pago_id
- cheque_numero
- transferencia_id
- usuario_validador
- estado

### 7. Catalogos y tablas de apoyo

Need:

- catalogo de dependencias
- catalogo de roles de usuario
- catalogo de estados de orden de pago
- catalogo de tipos de comprobante
- catalogo de motivos de anulacion o reverso
- manual o diccionario de campos de los modulos de tesoreria y JSP7 usados para estos procesos

## Request Framing

If TransMilenio objects that the request is too broad, narrow it in this order:

1. `2023-01-01` to `2024-12-31`
2. records tied to payroll cheques, treasury/JSP7 reconciliation, CRP deletion, and third-party payments
3. only the exact datasets listed above in reusable format

If TransMilenio objects on personal-data grounds, request:

- masked document number
- stable hashed identifier per person or beneficiary
- full user role, dependency, and timestamp
- full transaction identifiers

That still allows technical cross-checking.

## What This Unlocks

If delivered, these tables would let the project test:

- cheque exception recurrence by user, beneficiary, or month
- payment splitting below approval thresholds
- repeated manual adjustments before closing
- third-party beneficiary clusters
- bank-reconciliation gaps tied to specific movements
- JSP7 edits tied to specific users and records

