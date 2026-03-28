# Colombia candidate dataset probe

This report was generated from official `datos.gov.co` metadata and sample rows, then probed against the current live graph universe using normalized contract, process, company, person, and BPIN keys.

## Implement Next

_None._

## Enrichment Only

_None._

## Weak Feeder

### bvqe-m7ze - BASE DE DATOS CONTADORES PUBLICOS CCP

- Rows: `88`
- Families: `company_id, company_name`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- company_id: `matricula sample=1/88 live=0; a_o_matricula sample=0/86 live=0`
- company_name: `razon_social sample=4/88`

### iwgf-bkfk - Resultados únicos Saber TyT

- Rows: `1050966`
- Families: `company_name, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- company_name: `inst_nombre_institucion sample=1/56`
- territory: `estu_inst_departamento sample=0/15; estu_prgm_municipio sample=0/37; estu_inst_municipio sample=0/27; estu_prgm_departamento sample=0/17`

### sam8-8a3c - Registro profesional COPNIA

- Rows: `252724`
- Families: `territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- territory: `departamento sample=0/4; ciudad_residencia sample=0/5`

### u37r-hjmu - Resultados únicos Saber Pro

- Rows: `1217482`
- Families: `company_name, territory`
- Reason: Only weak name or territory overlap was observed; not enough for direct promotion.
- company_name: `inst_nombre_institucion sample=0/120`
- territory: `estu_inst_departamento sample=0/23; estu_inst_municipio sample=0/37; estu_prgm_departamento sample=0/25; estu_prgm_municipio sample=0/48`

## Drop

### 7d7s-zztj - MATRICULA PROFESIONAL CONSEJO PROFESIONAL DE ADMINISTRACION DE EMPRESAS

- Rows: `141940`
- Families: `none`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.

### 7fsy-xzzb - Licencias de construcción Fusagasugá

- Rows: `4603`
- Families: `none`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.

### my8c-6xkk - Registro Único Nacional del Talento Humano en Salud​ - Rethus

- Rows: `92373`
- Families: `none`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.
