import { beforeEach, describe, expect, it } from "vitest";

import { useGraphExplorerStore } from "./graphExplorer";

const INITIAL_ENTITY_TYPES = [
  "person",
  "company",
  "election",
  "contract",
  "sanction",
  "amendment",
  "publicOffice",
  "health",
  "finance",
  "embargo",
  "education",
  "convenio",
  "laborstats",
  "pepRecord",
  "expulsion",
  "leniencyAgreement",
  "internationalSanction",
  "govCardExpense",
  "govTravel",
  "bid",
  "fund",
  "douAct",
  "taxWaiver",
  "municipalFinance",
  "declaredAsset",
  "partyMembership",
  "barredNgo",
  "bcbPenalty",
  "laborMovement",
  "legalCase",
  "cpi",
];

const INITIAL_REL_TYPES = [
  "SOCIO_DE",
  "DONO_A",
  "CANDIDATO_EM",
  "GANO",
  "CONTRATOU",
  "ADJUDICOU_A",
  "AUTOR_EMENDA",
  "SANCIONADA",
  "OPERA_UNIDAD",
  "DEVE",
  "RECEBEU_EMPRESTIMO",
  "EMBARGADA",
  "MANTIENE_A",
  "BENEFICIO",
  "GENERO_CONVENIO",
  "SAME_AS",
  "PEP_REGISTRADA",
  "EXPULSO",
  "FIRMO_TRANSPARENCIA",
  "HOLDING_DE",
  "GASTO_CARTAO",
  "VIAJO",
  "LICITO",
  "ADMINISTRA",
  "GESTIONA",
  "PUBLICO",
  "MENCIONO",
  "RECIBIO_EXENCION",
  "DECLARO_FINANZAS",
  "SUMINISTRO_LICITACAO",
  "DECLARO_BIEN",
  "RECIBIO_SALARIO",
  "REFERENTE_A",
  "AAFILIADO_A",
  "INHABILITADA",
  "BCB_PENALIZADA",
  "MOVILIZO",
  "EMPLEADO_EN",
  "RELATOR_DE",
  "SANCIONADA_INTERNACIONALMENTE",
  "UN_SANCTIONED",
  "PARTICIPO_INVESTIGACION",
];

describe("useGraphExplorerStore", () => {
  beforeEach(() => {
    useGraphExplorerStore.getState().reset();
  });

  it("initial state has correct defaults", () => {
    const state = useGraphExplorerStore.getState();
    expect(state.depth).toBe(1);
    expect(state.enabledTypes).toEqual(new Set(INITIAL_ENTITY_TYPES));
    expect(state.enabledRelTypes).toEqual(new Set(INITIAL_REL_TYPES));
    expect(state.selectedNodeIds).toEqual(new Set());
    expect(state.hoveredNodeId).toBeNull();
    expect(state.hiddenNodeIds).toEqual(new Set());
    expect(state.layoutMode).toBe("force");
    expect(state.sidebarCollapsed).toBe(false);
    expect(state.detailPanelOpen).toBe(false);
    expect(state.isFullscreen).toBe(false);
    expect(state.contextMenu).toBeNull();
  });

  it("setDepth changes depth", () => {
    useGraphExplorerStore.getState().setDepth(3);
    expect(useGraphExplorerStore.getState().depth).toBe(3);

    useGraphExplorerStore.getState().setDepth(0);
    expect(useGraphExplorerStore.getState().depth).toBe(0);
  });

  it("toggleType removes an existing entity type", () => {
    useGraphExplorerStore.getState().toggleType("person");
    const types = useGraphExplorerStore.getState().enabledTypes;
    expect(types.has("person")).toBe(false);
    expect(types.size).toBe(INITIAL_ENTITY_TYPES.length - 1);
  });

  it("toggleType adds a new entity type", () => {
    // Remove then re-add
    useGraphExplorerStore.getState().toggleType("person");
    useGraphExplorerStore.getState().toggleType("person");
    const types = useGraphExplorerStore.getState().enabledTypes;
    expect(types.has("person")).toBe(true);
    expect(types.size).toBe(INITIAL_ENTITY_TYPES.length);
  });

  it("toggleRelType removes an existing relationship type", () => {
    useGraphExplorerStore.getState().toggleRelType("DONO_A");
    const rels = useGraphExplorerStore.getState().enabledRelTypes;
    expect(rels.has("DONO_A")).toBe(false);
    expect(rels.size).toBe(INITIAL_REL_TYPES.length - 1);
  });

  it("toggleRelType adds a new relationship type", () => {
    useGraphExplorerStore.getState().toggleRelType("DONO_A");
    useGraphExplorerStore.getState().toggleRelType("DONO_A");
    const rels = useGraphExplorerStore.getState().enabledRelTypes;
    expect(rels.has("DONO_A")).toBe(true);
    expect(rels.size).toBe(INITIAL_REL_TYPES.length);
  });

  it("selectNode sets a single node", () => {
    useGraphExplorerStore.getState().selectNode("node-1");
    const ids = useGraphExplorerStore.getState().selectedNodeIds;
    expect(ids).toEqual(new Set(["node-1"]));
  });

  it("selectNode with null clears selection", () => {
    useGraphExplorerStore.getState().selectNode("node-1");
    useGraphExplorerStore.getState().selectNode(null);
    const ids = useGraphExplorerStore.getState().selectedNodeIds;
    expect(ids.size).toBe(0);
  });

  it("toggleNodeSelection adds to multi-select set", () => {
    useGraphExplorerStore.getState().toggleNodeSelection("a");
    useGraphExplorerStore.getState().toggleNodeSelection("b");
    const ids = useGraphExplorerStore.getState().selectedNodeIds;
    expect(ids).toEqual(new Set(["a", "b"]));
  });

  it("toggleNodeSelection removes from multi-select set", () => {
    useGraphExplorerStore.getState().toggleNodeSelection("a");
    useGraphExplorerStore.getState().toggleNodeSelection("b");
    useGraphExplorerStore.getState().toggleNodeSelection("a");
    const ids = useGraphExplorerStore.getState().selectedNodeIds;
    expect(ids).toEqual(new Set(["b"]));
  });

  it("hideNode adds to hiddenNodeIds", () => {
    useGraphExplorerStore.getState().hideNode("h1");
    useGraphExplorerStore.getState().hideNode("h2");
    const hidden = useGraphExplorerStore.getState().hiddenNodeIds;
    expect(hidden).toEqual(new Set(["h1", "h2"]));
  });

  it("showAllNodes clears hiddenNodeIds", () => {
    useGraphExplorerStore.getState().hideNode("h1");
    useGraphExplorerStore.getState().hideNode("h2");
    useGraphExplorerStore.getState().showAllNodes();
    expect(useGraphExplorerStore.getState().hiddenNodeIds.size).toBe(0);
  });

  it("setLayoutMode switches between force and hierarchy", () => {
    useGraphExplorerStore.getState().setLayoutMode("hierarchy");
    expect(useGraphExplorerStore.getState().layoutMode).toBe("hierarchy");

    useGraphExplorerStore.getState().setLayoutMode("force");
    expect(useGraphExplorerStore.getState().layoutMode).toBe("force");
  });

  it("toggleSidebar flips sidebarCollapsed", () => {
    expect(useGraphExplorerStore.getState().sidebarCollapsed).toBe(false);
    useGraphExplorerStore.getState().toggleSidebar();
    expect(useGraphExplorerStore.getState().sidebarCollapsed).toBe(true);
    useGraphExplorerStore.getState().toggleSidebar();
    expect(useGraphExplorerStore.getState().sidebarCollapsed).toBe(false);
  });

  it("reset restores initial state after mutations", () => {
    const store = useGraphExplorerStore.getState();
    store.setDepth(5);
    store.toggleType("person");
    store.toggleRelType("DONO_A");
    store.selectNode("node-1");
    store.hideNode("h1");
    store.setLayoutMode("hierarchy");
    store.toggleSidebar();

    useGraphExplorerStore.getState().reset();

    const state = useGraphExplorerStore.getState();
    expect(state.depth).toBe(1);
    expect(state.enabledTypes).toEqual(new Set(INITIAL_ENTITY_TYPES));
    expect(state.enabledRelTypes).toEqual(new Set(INITIAL_REL_TYPES));
    expect(state.selectedNodeIds.size).toBe(0);
    expect(state.hiddenNodeIds.size).toBe(0);
    expect(state.layoutMode).toBe("force");
    expect(state.sidebarCollapsed).toBe(false);
  });
});
