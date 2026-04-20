export interface SectorDef {
  id: string;
  name: string;
  custodian: string;
  description: string;
  signal_categories: string[];
  sources: { id: string; name: string; note?: string }[];
  pida_datasets: number;
}

export const SECTORS: Record<string, SectorDef> = {
  procurement: {
    id: "procurement",
    name: "Contratación pública",
    custodian: "Colombia Compra Eficiente · ANCP-CCE",
    description:
      "Toda la contratación estatal nacional y territorial — adjudicaciones, modificaciones, suspensiones, ejecución financiera. Base para el grueso de las señales de cruce.",
    signal_categories: ["procurement", "ordering"],
    sources: [
      { id: "secop_i", name: "SECOP I", note: "legacy" },
      { id: "secop_ii", name: "SECOP II" },
      { id: "secop_integrado", name: "SECOP Integrado", note: "vista unificada" },
      { id: "tvec", name: "Tienda Virtual del Estado Colombiano" },
      { id: "rues", name: "Registro Único Empresarial", note: "Confecámaras" },
    ],
    pida_datasets: 6,
  },
  conflict: {
    id: "conflict",
    name: "Declaraciones y conflictos",
    custodian: "DAFP · Función Pública · Procuraduría",
    description:
      "Servidores públicos, posiciones sensibles, declaraciones de bienes y renta, conflictos de interés — cruzables contra adjudicaciones contractuales.",
    signal_categories: ["conflict", "governance"],
    sources: [
      { id: "sigep", name: "SIGEP II", note: "servidores y contratistas" },
      { id: "asset_disclosures", name: "Declaración de bienes Ley 2013" },
      { id: "paco", name: "PACO", note: "probidad y conflicto" },
      { id: "ley_2013", name: "Ley 2013 de 2019" },
    ],
    pida_datasets: 4,
  },
  sanctions: {
    id: "sanctions",
    name: "Sanciones y responsabilidad",
    custodian: "Procuraduría · Contraloría · Fiscalía",
    description:
      "Sanciones disciplinarias (SIRI), responsabilidad fiscal (SIREC), inhabilidades, procesos penales. La trazabilidad entre una sanción y una adjudicación posterior es una señal crítica.",
    signal_categories: ["sanctions", "judicial"],
    sources: [
      { id: "siri", name: "SIRI", note: "Procuraduría" },
      { id: "cgr_sirec", name: "SIREC", note: "Contraloría" },
      { id: "fiscalia", name: "Fiscalía General" },
      { id: "furag", name: "FURAG", note: "desempeño institucional" },
    ],
    pida_datasets: 3,
  },
  projects: {
    id: "projects",
    name: "Proyectos e inversión",
    custodian: "DNP · SGR · SUIFP",
    description:
      "Regalías (SGR) y Banco de Proyectos de Inversión (BPIN) — qué se aprueba, cuánto se gira, a qué municipios y sectores llega, quién ejecuta.",
    signal_categories: ["projects"],
    sources: [
      { id: "sgr_transparencia", name: "SGR · Transparencia" },
      { id: "bpin_dnp", name: "BPIN · DNP" },
      { id: "suifp", name: "SUIFP" },
      { id: "presupuesto_nacion", name: "Presupuesto general de la Nación" },
    ],
    pida_datasets: 5,
  },
  environment: {
    id: "environment",
    name: "Ambiente y territorio",
    custodian: "ANLA · IDEAM · MinAmbiente",
    description:
      "Licencias ambientales, sanciones, alertas deforestación, títulos mineros. Cruza contra contratistas para detectar ejecución en áreas protegidas o sin licencia.",
    signal_categories: ["environment", "property"],
    sources: [
      { id: "anla_siac", name: "ANLA · SIAC" },
      { id: "ideam", name: "IDEAM" },
      { id: "anm", name: "ANM · títulos mineros" },
      { id: "catastro_igac", name: "Catastro IGAC" },
    ],
    pida_datasets: 4,
  },
  extractive: {
    id: "extractive",
    name: "Extractivas y energía",
    custodian: "ANH · ANM · UPME · CREG",
    description:
      "Contratos de hidrocarburos, títulos mineros, subsidios de energía. Alta concentración sectorial; cruza con declaraciones de servidores y sanciones ambientales.",
    signal_categories: ["energy"],
    sources: [
      { id: "anh", name: "ANH · hidrocarburos" },
      { id: "anm", name: "ANM · minería" },
      { id: "upme", name: "UPME" },
      { id: "creg", name: "CREG" },
    ],
    pida_datasets: 2,
  },
  health: {
    id: "health",
    name: "Salud",
    custodian: "MinSalud · SISPRO · Invima",
    description:
      "Prestadores de servicios, habilitaciones, registros sanitarios, vigilancia. Cruce con SECOP revela captura de contratos por EPS o prestadores sancionados.",
    signal_categories: ["cross_sector"],
    sources: [
      { id: "sivigila", name: "SIVIGILA" },
      { id: "sispro", name: "SISPRO" },
      { id: "invima", name: "Invima" },
      { id: "adres", name: "ADRES" },
    ],
    pida_datasets: 3,
  },
  education: {
    id: "education",
    name: "Educación",
    custodian: "MinEducación · ICFES · SNIES",
    description:
      "Establecimientos educativos, estadísticas de desempeño, contratos de infraestructura y alimentación escolar. Histórico fértil para cruces contra PAE.",
    signal_categories: ["cross_sector"],
    sources: [
      { id: "icfes", name: "ICFES" },
      { id: "snies", name: "SNIES" },
      { id: "mineducacion", name: "MinEducación" },
      { id: "pae", name: "PAE", note: "alimentación escolar" },
    ],
    pida_datasets: 3,
  },
};

export const SECTOR_ORDER: string[] = [
  "procurement",
  "conflict",
  "sanctions",
  "projects",
  "environment",
  "extractive",
  "health",
  "education",
];
