export const dataColors = {
  person: "#58A4D0",      /* Refined blue */
  company: "#D08570",     /* Muted terracotta */
  election: "#6BB08E",    /* Sage green */
  contract: "#E0E0E0",    /* Clean off-white */
  sanction: "#D05050",    /* Muted danger red */
  amendment: "#9D95B0",   /* Muted lavender */
  publicOffice: "#208078",/* Sophisticated teal */
  health: "#D8508A",      /* Muted magenta */
  finance: "#4A80D0",     /* Deep sky blue */
  embargo: "#D87530",     /* Muted orange */
  education: "#9560D8",   /* Deep violet */
  convenio: "#3FB0A0",    /* Ocean teal */
  laborstats: "#607080",  /* Steel blue-gray */
  offshoreEntity: "#20A0D0",
  offshoreOfficer: "#20B0C0",
  globalPep: "#C850D0",
  cvmProceeding: "#E04860",
  expense: "#80B840",     /* Muted lime */
  pepRecord: "#B040C0",
  expulsion: "#C04040",
  leniencyAgreement: "#209060",
  internationalSanction: "#A03030",
  govCardExpense: "#D0A030",
  govTravel: "#20A090",
  bid: "#8060E0",
  fund: "#2080B8",
  douAct: "#807870",
  taxWaiver: "#C84078",
  municipalFinance: "#208880",
  declaredAsset: "#784AE0",
  partyMembership: "#209060",
  barredNgo: "#A83060",
  bcbPenalty: "#A86020",
  laborMovement: "#506078",
  legalCase: "#B88820",
  cpi: "#8848D0",
} as const;

export type DataEntityType = keyof typeof dataColors;

export const relationshipColors: Record<string, string> = {
  SOCIO_DE: "#58A4D0",
  DONO_A: "#6BB08E",
  CANDIDATO_EM: "#6BB08E",
  GANO: "#E0E0E0",
  CONTRATOU: "#D08570",
  ADJUDICOU_A: "#9560D8",
  AUTOR_EMENDA: "#E0E0E0",
  SANCIONADA: "#D05050",
  OPERA_UNIDAD: "#D8508A",
  DEVE: "#4A80D0",
  RECEBEU_EMPRESTIMO: "#4A80D0",
  EMBARGADA: "#D87530",
  MANTIENE_A: "#9560D8",
  BENEFICIO: "#3FB0A0",
  GENERO_CONVENIO: "#3FB0A0",
  SAME_AS: "#607080",
  POSSIBLY_SAME_AS: "#20A0D0",
  OFFICER_OF: "#20B0C0",
  INTERMEDIARY_OF: "#20B0C0",
  GLOBAL_PEP_MATCH: "#C850D0",
  CVM_SANCIONADA: "#E04860",
  GASTO: "#80B840",
  SUMINISTRO: "#80B840",
  PEP_REGISTRADA: "#B040C0",
  EXPULSO: "#C04040",
  FIRMO_TRANSPARENCIA: "#209060",
  HOLDING_DE: "#D08570",
  GASTO_CARTAO: "#D0A030",
  VIAJO: "#20A090",
  LICITO: "#8060E0",
  ADMINISTRA: "#2080B8",
  GESTIONA: "#2080B8",
  PUBLICO: "#807870",
  MENCIONO: "#807870",
  RECIBIO_EXENCION: "#C84078",
  DECLARO_FINANZAS: "#208880",
  SUMINISTRO_LICITACAO: "#8060E0",
  RECIBIO_SALARIO: "#208078",
  REFERENTE_A: "#784AE0",
  DECLARO_BIEN: "#784AE0",
  AAFILIADO_A: "#209060",
  INHABILITADA: "#A83060",
  BCB_PENALIZADA: "#A86020",
  MOVILIZO: "#506078",
  EMPLEADO_EN: "#506078",
  RELATOR_DE: "#B88820",
  SANCIONADA_INTERNACIONALMENTE: "#A03030",
  UN_SANCTIONED: "#A03030",
  PARTICIPO_INVESTIGACION: "#8848D0",
};

export const semanticColors = {
  success: "#209060",
  warning: "#D0A030",
  danger: "#C04040",
  info: "#4A80D0",
} as const;

// Backward compat: entityColors maps old types to new data palette
export const entityColors: Record<string, string> = {
  person: dataColors.person,
  company: dataColors.company,
  contract: dataColors.contract,
  election: dataColors.election,
  sanction: dataColors.sanction,
  amendment: dataColors.amendment,
  publicOffice: dataColors.publicOffice,
  finance: dataColors.finance,
  legal: dataColors.legalCase,
  health: dataColors.health,
  environment: dataColors.municipalFinance,
  labor: dataColors.laborstats,
  education: dataColors.education,
  regulatory: dataColors.convenio,
  property: dataColors.bid,
  offshore: dataColors.offshoreEntity,
  pep: dataColors.globalPep,
  cvm: dataColors.cvmProceeding,
  expense: dataColors.expense,
  pepRecord: dataColors.pepRecord,
  expulsion: dataColors.expulsion,
  leniencyAgreement: dataColors.leniencyAgreement,
  internationalSanction: dataColors.internationalSanction,
  govCardExpense: dataColors.govCardExpense,
  govTravel: dataColors.govTravel,
  bid: dataColors.bid,
  fund: dataColors.fund,
  douAct: dataColors.douAct,
  taxWaiver: dataColors.taxWaiver,
  municipalFinance: dataColors.municipalFinance,
  declaredAsset: dataColors.declaredAsset,
  partyMembership: dataColors.partyMembership,
  barredNgo: dataColors.barredNgo,
  bcbPenalty: dataColors.bcbPenalty,
  laborMovement: dataColors.laborMovement,
  legalCase: dataColors.legalCase,
  cpi: dataColors.cpi,
};

export type EntityType = keyof typeof entityColors;
