import { create } from "zustand";

type LayoutMode = "force" | "hierarchy";

interface GraphExplorerState {
  depth: number;
  enabledTypes: Set<string>;
  enabledRelTypes: Set<string>;
  selectedNodeIds: Set<string>;
  hoveredNodeId: string | null;
  hiddenNodeIds: Set<string>;
  layoutMode: LayoutMode;
  sidebarCollapsed: boolean;
  detailPanelOpen: boolean;
  isFullscreen: boolean;
  contextMenu: { x: number; y: number; nodeId: string } | null;

  setDepth: (depth: number) => void;
  toggleType: (type: string) => void;
  toggleRelType: (type: string) => void;
  selectNode: (id: string | null) => void;
  toggleNodeSelection: (id: string) => void;
  setHoveredNode: (id: string | null) => void;
  hideNode: (id: string) => void;
  showAllNodes: () => void;
  setLayoutMode: (mode: LayoutMode) => void;
  toggleSidebar: () => void;
  toggleDetailPanel: () => void;
  toggleFullscreen: () => void;
  setContextMenu: (
    menu: { x: number; y: number; nodeId: string } | null,
  ) => void;
  reset: () => void;
}

const INITIAL_ENTITY_TYPES = new Set([
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
]);

const INITIAL_REL_TYPES = new Set([
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
]);

function initialState() {
  return {
    depth: 1,
    enabledTypes: new Set(INITIAL_ENTITY_TYPES),
    enabledRelTypes: new Set(INITIAL_REL_TYPES),
    selectedNodeIds: new Set<string>(),
    hoveredNodeId: null as string | null,
    hiddenNodeIds: new Set<string>(),
    layoutMode: "force" as LayoutMode,
    sidebarCollapsed: false,
    detailPanelOpen: false,
    isFullscreen: false,
    contextMenu: null as { x: number; y: number; nodeId: string } | null,
  };
}

export const useGraphExplorerStore = create<GraphExplorerState>((set) => ({
  ...initialState(),

  setDepth: (depth) => set({ depth }),

  toggleType: (type) =>
    set((state) => {
      const next = new Set(state.enabledTypes);
      if (next.has(type)) {
        next.delete(type);
      } else {
        next.add(type);
      }
      return { enabledTypes: next };
    }),

  toggleRelType: (type) =>
    set((state) => {
      const next = new Set(state.enabledRelTypes);
      if (next.has(type)) {
        next.delete(type);
      } else {
        next.add(type);
      }
      return { enabledRelTypes: next };
    }),

  selectNode: (id) =>
    set(() => {
      if (id === null) {
        return { selectedNodeIds: new Set<string>() };
      }
      return { selectedNodeIds: new Set([id]) };
    }),

  toggleNodeSelection: (id) =>
    set((state) => {
      const next = new Set(state.selectedNodeIds);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return { selectedNodeIds: next };
    }),

  setHoveredNode: (id) => set({ hoveredNodeId: id }),

  hideNode: (id) =>
    set((state) => {
      const next = new Set(state.hiddenNodeIds);
      next.add(id);
      return { hiddenNodeIds: next };
    }),

  showAllNodes: () => set({ hiddenNodeIds: new Set<string>() }),

  setLayoutMode: (mode) => set({ layoutMode: mode }),

  toggleSidebar: () =>
    set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),

  toggleDetailPanel: () =>
    set((state) => ({ detailPanelOpen: !state.detailPanelOpen })),

  toggleFullscreen: () =>
    set((state) => ({ isFullscreen: !state.isFullscreen })),

  setContextMenu: (menu) => set({ contextMenu: menu }),

  reset: () => set(initialState()),
}));
