import { useParams } from "react-router";

import { Placeholder } from "@/components/common/Placeholder";

export function Sector() {
  const { sectorId } = useParams();
  return (
    <Placeholder
      kicker="sector"
      title={sectorId ? `Sector · ${sectorId}` : "Sector"}
      body="Tablero sectorial con cobertura de fuentes, señales activas, proyectos BPIN/SGR, y comparativas territoriales. Pendiente de rediseño."
    />
  );
}
