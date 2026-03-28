import { describe, expect, it } from "vitest";

import { formatPropertyLabel, formatSourceName, humanizeIdentifier } from "./display";

describe("display helpers", () => {
  it("formats known source ids into readable labels", () => {
    expect(formatSourceName("sigep_public_servants")).toBe("SIGEP public servants");
    expect(formatSourceName("secop_integrado")).toBe("Integrated SECOP contracts");
    expect(formatSourceName("paco_sanctions")).toBe("PACO sanctions and red flags");
    expect(formatSourceName("mapa_inversiones_projects")).toBe("MapaInversiones projects");
    expect(formatSourceName("supersoc_top_companies")).toBe("Supersociedades top companies");
    expect(formatSourceName("rues_chambers")).toBe("RUES chambers of commerce");
  });

  it("formats known schema keys into readable labels", () => {
    expect(formatPropertyLabel("razon_social")).toBe("Razón social");
    expect(formatPropertyLabel("buyer_document_id")).toBe("Documento del comprador");
    expect(formatPropertyLabel("execution_ratio")).toBe("Avance de ejecución");
    expect(formatPropertyLabel("operating_revenue_current")).toBe("Ingresos operacionales (año actual)");
    expect(formatPropertyLabel("transaction_count")).toBe("Cantidad de transacciones");
    expect(formatPropertyLabel("identity_status")).toBe("Estado de identidad");
  });

  it("humanizes unknown identifiers safely", () => {
    expect(humanizeIdentifier("custom_source_name")).toBe("Custom Source Name");
    expect(humanizeIdentifier("supplier_document_type")).toBe("Supplier Document Type");
  });
});
