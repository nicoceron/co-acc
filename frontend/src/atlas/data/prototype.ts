export type Severity = "low" | "medium" | "high" | "critical";

export interface FeedItem {
  t: string;
  signal: string;
  severity: Severity;
  entity: string;
  doc: string;
  note: string;
}

export interface SourceItem {
  code: string;
  name: string;
  desc: string;
  cov: number;
  rows: string;
  stale?: boolean;
}

export interface SectorItem {
  id: string;
  label: string;
  hits: number;
  contracts: string;
  accent: string;
}

export interface Departamento {
  code: string;
  name: string;
  x: number;
  y: number;
  hits: number;
  sev: Severity;
}

export interface SignalSummary {
  id: string;
  severity: Severity;
  title: string;
  desc: string;
  category: string;
  hits: number;
  public: boolean;
  confidence?: number | null;
  lastSeen?: string | null;
  materialized?: boolean;
  materializationState?: "materialized" | "registered_only";
}

export interface SignalDetail {
  version: string;
  scope: string;
  runner: string;
  policy: {
    public: boolean;
    identity: string[];
    dedup: string[];
  };
  sourcesRequired: string[];
  entityTypes: string[];
  sampleHits: {
    label: string;
    conf: number;
    ev: number;
    score: number;
    identity: number;
    traceability: number;
    corroboration: number;
  }[];
}

export interface EntityExample {
  id: string;
  type: string;
  name: string;
  doc: string;
  pep: boolean;
  firstSeen: string;
  lastSeen: string;
  sources: string[];
  contracts: number;
  valueCop: string;
  sanctions: number;
  pepLinks: number;
  signals: {
    id: string;
    title: string;
    sev: Severity;
    ev: number;
    score: number;
  }[];
  relations: {
    id: string;
    label: string;
    type: "persona" | "empresa" | "entidad";
    weight: number;
    role: string;
  }[];
  timeline: {
    d: string;
    k: string;
    label: string;
  }[];
}

export interface CaseStory {
  slug: string;
  date: string;
  kicker: string;
  title: string;
  lede: string;
  docs: number;
  entities: number;
  signals: string[];
  published: boolean;
}

export interface AtlasFixture {
  feed: FeedItem[];
  sources: SourceItem[];
  sectors: SectorItem[];
  departamentos: Departamento[];
  signals: SignalSummary[];
  signalDetail: Record<string, SignalDetail>;
  entityExample: EntityExample;
  cases: CaseStory[];
  seriesHits: number[];
  seriesSourcesOk: number[];
}

export const atlasFixture: AtlasFixture = {
  feed: [
    { t: "11:42:08", signal: "S-014", severity: "high", entity: "Vertice Andina S.A.S.", doc: "NIT 900.412.118-5", note: "co-licitacion con sancionada" },
    { t: "11:41:51", signal: "S-031", severity: "medium", entity: "Carlos M. Beltran Pinilla", doc: "CC 79.412.087", note: "PEP adjudicado en ventana corta" },
    { t: "11:41:04", signal: "S-009", severity: "critical", entity: "Constructora Llanura Ltda.", doc: "NIT 830.118.402-1", note: "5 contratos · 1 proveedor" },
    { t: "11:40:22", signal: "S-002", severity: "low", entity: "Operadora Caribe Verde", doc: "NIT 901.554.330-7", note: "RUES sin fecha de constitucion" },
    { t: "11:39:18", signal: "S-014", severity: "high", entity: "Sanidad Pacifico S.A.", doc: "NIT 900.221.115-3", note: "sancion activa al adjudicar" },
    { t: "11:38:54", signal: "S-022", severity: "medium", entity: "Inversiones Magdalena", doc: "NIT 900.880.224-1", note: "cambio de RL post-adjudicacion" },
    { t: "11:38:11", signal: "S-007", severity: "high", entity: "Pavimentos del Norte S.A.S.", doc: "NIT 901.221.660-2", note: "concentracion 71% valor" },
    { t: "11:37:29", signal: "S-031", severity: "low", entity: "Maria L. Ortega Mejia", doc: "CC 52.331.087", note: "PEP en SIGEP · Ley 2013" },
    { t: "11:36:12", signal: "S-009", severity: "critical", entity: "Geomatica Andina S.A.", doc: "NIT 830.044.118-9", note: "ventana menor al decil 1" },
    { t: "11:35:03", signal: "S-002", severity: "medium", entity: "Hidroflujo del Cauca", doc: "NIT 900.118.732-4", note: "regalias sin BPIN" },
    { t: "11:34:48", signal: "S-014", severity: "high", entity: "Logistica Tropico S.A.", doc: "NIT 900.665.110-3", note: "co-adjudicacion con sancionada" },
    { t: "11:33:21", signal: "S-007", severity: "medium", entity: "Constructora Ipiales", doc: "NIT 901.331.220-1", note: "concentracion local" },
  ],
  sources: [
    { code: "SECOP-II", name: "SECOP II", desc: "Contratacion publica electronica", cov: 0.93, rows: "4.2M" },
    { code: "SECOP-I", name: "SECOP I", desc: "Contratacion archivo historico", cov: 0.88, rows: "2.7M" },
    { code: "RUES", name: "Registro Unico Empresarial", desc: "Empresas y representantes", cov: 0.81, rows: "1.4M" },
    { code: "SIGEP", name: "SIGEP", desc: "Servidores y contratistas", cov: 0.74, rows: "612k" },
    { code: "SIRI", name: "SIRI", desc: "Sanciones disciplinarias", cov: 0.69, rows: "184k" },
    { code: "LEY-2013", name: "Ley 2013", desc: "Declaracion de bienes y rentas", cov: 0.62, rows: "98k" },
    { code: "SGR", name: "SGR · BPIN", desc: "Regalias y proyectos", cov: 0.71, rows: "212k" },
    { code: "ANLA", name: "ANLA · SIAC", desc: "Licencias ambientales", cov: 0.58, rows: "47k" },
    { code: "ANH", name: "ANH", desc: "Hidrocarburos", cov: 0.66, rows: "31k" },
    { code: "ANM", name: "ANM", desc: "Titulos mineros", cov: 0.54, rows: "22k" },
    { code: "CGR", name: "CGR · SIREC", desc: "Responsabilidad fiscal", cov: 0.49, rows: "18k" },
    { code: "ICFES", name: "ICFES", desc: "Educacion y acreditacion", cov: 0.78, rows: "204k" },
    { code: "TVEC", name: "TVEC", desc: "Tienda virtual del Estado", cov: 0.83, rows: "611k" },
    { code: "DIAN", name: "DIAN", desc: "Tributario publico", cov: 0.41, rows: "12k", stale: true },
    { code: "FURAG", name: "FURAG", desc: "Gestion y desempeno", cov: 0.36, rows: "8k", stale: true },
    { code: "PACO", name: "PACO sanciones", desc: "Inhabilidades", cov: 0.55, rows: "19k" },
  ],
  sectors: [
    { id: "infra", label: "Infraestructura", hits: 312, contracts: "12.4k", accent: "var(--coacc-ochre)" },
    { id: "salud", label: "Salud", hits: 264, contracts: "8.1k", accent: "var(--coacc-coral)" },
    { id: "educ", label: "Educacion", hits: 188, contracts: "4.9k", accent: "var(--coacc-cyan)" },
    { id: "minas", label: "Minas y energia", hits: 142, contracts: "3.2k", accent: "var(--coacc-moss)" },
    { id: "ambiente", label: "Ambiente", hits: 118, contracts: "1.8k", accent: "var(--coacc-iris)" },
    { id: "agro", label: "Agricultura", hits: 96, contracts: "2.1k", accent: "var(--coacc-lime)" },
    { id: "trans", label: "Transporte", hits: 224, contracts: "7.6k", accent: "var(--coacc-amber)" },
    { id: "justicia", label: "Justicia", hits: 74, contracts: "1.1k", accent: "var(--coacc-rose)" },
  ],
  departamentos: [
    { code: "BOG", name: "Bogota D.C.", x: 0.50, y: 0.55, hits: 412, sev: "critical" },
    { code: "ANT", name: "Antioquia", x: 0.42, y: 0.38, hits: 287, sev: "high" },
    { code: "VAC", name: "Valle del Cauca", x: 0.30, y: 0.58, hits: 198, sev: "high" },
    { code: "ATL", name: "Atlantico", x: 0.42, y: 0.18, hits: 156, sev: "medium" },
    { code: "BOL", name: "Bolivar", x: 0.40, y: 0.22, hits: 143, sev: "medium" },
    { code: "SAN", name: "Santander", x: 0.55, y: 0.36, hits: 132, sev: "medium" },
    { code: "MAG", name: "Magdalena", x: 0.46, y: 0.18, hits: 118, sev: "medium" },
    { code: "CES", name: "Cesar", x: 0.50, y: 0.22, hits: 98, sev: "low" },
    { code: "CUN", name: "Cundinamarca", x: 0.48, y: 0.50, hits: 96, sev: "low" },
    { code: "NSA", name: "N. Santander", x: 0.60, y: 0.30, hits: 92, sev: "low" },
    { code: "TOL", name: "Tolima", x: 0.42, y: 0.55, hits: 88, sev: "low" },
    { code: "BOY", name: "Boyaca", x: 0.54, y: 0.45, hits: 84, sev: "low" },
    { code: "RIS", name: "Risaralda", x: 0.36, y: 0.48, hits: 76, sev: "low" },
    { code: "CAL", name: "Caldas", x: 0.40, y: 0.46, hits: 72, sev: "low" },
    { code: "QUI", name: "Quindio", x: 0.36, y: 0.52, hits: 64, sev: "low" },
    { code: "HUI", name: "Huila", x: 0.40, y: 0.66, hits: 58, sev: "low" },
    { code: "NAR", name: "Narino", x: 0.26, y: 0.78, hits: 68, sev: "low" },
    { code: "CAU", name: "Cauca", x: 0.30, y: 0.66, hits: 78, sev: "low" },
    { code: "CHO", name: "Choco", x: 0.28, y: 0.40, hits: 46, sev: "low" },
    { code: "COR", name: "Cordoba", x: 0.36, y: 0.26, hits: 62, sev: "low" },
    { code: "SUC", name: "Sucre", x: 0.40, y: 0.26, hits: 44, sev: "low" },
    { code: "GUA", name: "La Guajira", x: 0.54, y: 0.12, hits: 52, sev: "low" },
    { code: "MET", name: "Meta", x: 0.58, y: 0.58, hits: 88, sev: "low" },
    { code: "CAS", name: "Casanare", x: 0.60, y: 0.50, hits: 56, sev: "low" },
    { code: "ARA", name: "Arauca", x: 0.62, y: 0.40, hits: 38, sev: "low" },
    { code: "VIC", name: "Vichada", x: 0.72, y: 0.55, hits: 18, sev: "low" },
    { code: "GUI", name: "Guainia", x: 0.80, y: 0.62, hits: 10, sev: "low" },
    { code: "VAU", name: "Vaupes", x: 0.74, y: 0.72, hits: 14, sev: "low" },
    { code: "GUV", name: "Guaviare", x: 0.62, y: 0.66, hits: 22, sev: "low" },
    { code: "CAQ", name: "Caqueta", x: 0.50, y: 0.74, hits: 34, sev: "low" },
    { code: "PUT", name: "Putumayo", x: 0.36, y: 0.82, hits: 28, sev: "low" },
    { code: "AMA", name: "Amazonas", x: 0.62, y: 0.88, hits: 16, sev: "low" },
  ],
  signals: [
    { id: "S-002", severity: "low", title: "RUES sin fecha de constitucion", desc: "Empresas registradas en SECOP cuya inscripcion RUES carece de fecha verificable.", category: "rues", hits: 412, public: true },
    { id: "S-007", severity: "high", title: "Concentracion de proveedor", desc: "Una entidad concentra mas de 60% del valor adjudicado en un programa o vigencia.", category: "secop", hits: 1284, public: true },
    { id: "S-009", severity: "critical", title: "Ventana corta de adjudicacion", desc: "Procesos cerrados en menos del decil 1 del tiempo esperado para su categoria.", category: "secop", hits: 922, public: true },
    { id: "S-014", severity: "high", title: "Co-licitacion con sancionada", desc: "Adjudicaciones donde el oferente principal o secundario tiene SIRI activa.", category: "siri", hits: 1108, public: true },
    { id: "S-022", severity: "medium", title: "Cambio de RL post-adjudicacion", desc: "Cambio de representante legal dentro de 90 dias de una adjudicacion significativa.", category: "rues", hits: 388, public: true },
    { id: "S-031", severity: "medium", title: "PEP adjudicado en ventana corta", desc: "Persona expuesta politicamente adjudicada en proceso de duracion atipica.", category: "pep", hits: 274, public: true },
    { id: "S-035", severity: "high", title: "Regalias sin proyecto BPIN", desc: "Recursos SGR ejecutados sin proyecto BPIN asociado o con proyecto no aprobado.", category: "sgr", hits: 612, public: false },
    { id: "S-041", severity: "low", title: "Declaracion Ley 2013 omitida", desc: "Servidor obligado en SIGEP sin declaracion del periodo correspondiente.", category: "ley2013", hits: 542, public: true },
    { id: "S-043", severity: "critical", title: "Licencia ambiental y sancion cruzada", desc: "Titular de licencia ANLA con SIRI activa al momento del otorgamiento.", category: "anla", hits: 86, public: false },
  ],
  signalDetail: {
    "S-014": {
      version: "3.2.1",
      scope: "process_oferente",
      runner: "sql:signals/co_licitation_with_siri.sql",
      policy: { public: true, identity: ["nit", "cc"], dedup: ["proceso_id", "oferente_id"] },
      sourcesRequired: ["SECOP-II", "SECOP-I", "SIRI", "RUES"],
      entityTypes: ["empresa", "persona", "contrato"],
      sampleHits: [
        { label: "Vertice Andina S.A.S. · proceso CO1.PCCNTR.4421", conf: 94, identity: 1, traceability: 1, corroboration: 0.9, ev: 12, score: 0.91 },
        { label: "Sanidad Pacifico S.A. · proceso CO1.PCCNTR.5102", conf: 88, identity: 0.9, traceability: 1, corroboration: 0.7, ev: 8, score: 0.84 },
        { label: "Logistica Tropico S.A. · proceso CO1.PCCNTR.6612", conf: 92, identity: 1, traceability: 1, corroboration: 0.7, ev: 11, score: 0.81 },
        { label: "Constructora Llanura Ltda. · proceso CO1.PCCNTR.5588", conf: 79, identity: 0.75, traceability: 1, corroboration: 0.7, ev: 6, score: 0.76 },
        { label: "Pavimentos del Norte S.A.S. · proceso CO1.PCCNTR.7012", conf: 86, identity: 0.9, traceability: 1, corroboration: 0.7, ev: 9, score: 0.72 },
      ],
    },
  },
  entityExample: {
    id: "ent_co_900412118",
    type: "empresa",
    name: "Vertice Andina S.A.S.",
    doc: "900.412.118-5",
    pep: false,
    firstSeen: "2017-03-12",
    lastSeen: "2026-04-22",
    sources: ["SECOP-II", "RUES", "SIRI", "SGR", "TVEC"],
    contracts: 184,
    valueCop: "$ 142.8B",
    sanctions: 2,
    pepLinks: 3,
    signals: [
      { id: "S-014", title: "Co-licitacion con sancionada", sev: "high", ev: 12, score: 0.91 },
      { id: "S-007", title: "Concentracion de proveedor", sev: "high", ev: 8, score: 0.84 },
      { id: "S-022", title: "Cambio de RL post-adjudicacion", sev: "medium", ev: 3, score: 0.62 },
      { id: "S-009", title: "Ventana corta de adjudicacion", sev: "critical", ev: 6, score: 0.78 },
    ],
    relations: [
      { id: "r1", label: "C. Beltran Pinilla", type: "persona", weight: 0.9, role: "RL actual" },
      { id: "r2", label: "M. Ortega Mejia", type: "persona", weight: 0.6, role: "RL anterior" },
      { id: "r3", label: "Constructora Llanura", type: "empresa", weight: 0.7, role: "co-oferente" },
      { id: "r4", label: "Pavimentos del Norte", type: "empresa", weight: 0.5, role: "co-oferente" },
      { id: "r5", label: "INVIAS", type: "entidad", weight: 0.8, role: "contratante" },
      { id: "r6", label: "Alcaldia de Tunja", type: "entidad", weight: 0.4, role: "contratante" },
    ],
    timeline: [
      { d: "2017-03-12", k: "registro", label: "Inscripcion RUES · Bogota D.C." },
      { d: "2018-09-04", k: "contrato", label: "Primer contrato SECOP · INVIAS · $ 8.4B" },
      { d: "2020-02-18", k: "senal", label: "S-007 · Concentracion de proveedor" },
      { d: "2021-11-30", k: "sancion", label: "SIRI · suspension 90 dias · RL anterior" },
      { d: "2022-06-22", k: "contrato", label: "Adjudicacion TVEC · $ 22.1B" },
      { d: "2023-04-08", k: "senal", label: "S-014 · Co-licitacion con sancionada" },
      { d: "2024-08-12", k: "cambio", label: "Cambio de RL · C. Beltran Pinilla" },
      { d: "2025-12-01", k: "senal", label: "S-009 · Ventana corta · proceso 7012" },
      { d: "2026-04-22", k: "contrato", label: "Ultima adjudicacion visible · Alcaldia Tunja" },
    ],
  },
  cases: [
    { slug: "transmilenio-microdesfalco", date: "2026-04-12", kicker: "Investigacion · Transporte", title: "Microdesfalco en operadores de Transmilenio", lede: "1.342 contratos cruzados con SIRI revelan tres operadores adjudicados pese a sanciones disciplinarias activas. El dossier publica la evidencia documental.", docs: 184, entities: 47, signals: ["S-014", "S-007", "S-022"], published: true },
    { slug: "regalias-fantasma-llanos", date: "2026-03-30", kicker: "Investigacion · Regalias", title: "Regalias fantasma en los Llanos Orientales", lede: "Recursos del SGR ejecutados sin proyecto BPIN asociado en cuatro municipios de Casanare y Meta.", docs: 92, entities: 28, signals: ["S-035", "S-002"], published: true },
    { slug: "ventana-corta-vias-terciarias", date: "2026-03-14", kicker: "Patron · Infraestructura", title: "El decil 1: vias terciarias adjudicadas en horas", lede: "Doce procesos cerraron en menos del decil inferior del tiempo esperado para su categoria.", docs: 64, entities: 19, signals: ["S-009", "S-007"], published: true },
    { slug: "pep-cruzados-sigep", date: "2026-02-22", kicker: "Investigacion · Transparencia", title: "Personas expuestas politicamente, cruzadas en SIGEP", lede: "Once PEP aparecen como adjudicatarios en procesos de duracion atipica entre 2024 y 2025.", docs: 47, entities: 11, signals: ["S-031", "S-009"], published: false },
    { slug: "anla-sanciones-cruzadas", date: "2026-02-08", kicker: "Patron · Ambiente", title: "Licencias ANLA otorgadas a sancionados", lede: "Cinco titulares con SIRI activa al momento del otorgamiento de licencia ambiental.", docs: 38, entities: 9, signals: ["S-043", "S-014"], published: false },
  ],
  seriesHits: [12, 18, 22, 16, 28, 34, 30, 42, 38, 46, 52, 48, 55, 62, 58, 68, 72, 66, 80, 88, 84, 92, 104, 110, 118, 124, 130, 138, 148, 154],
  seriesSourcesOk: [10, 11, 11, 12, 12, 12, 11, 12, 12, 12, 13, 13, 12, 12, 13, 12, 12, 13, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12],
};
