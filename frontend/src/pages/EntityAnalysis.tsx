import { useParams } from "react-router";

import { Placeholder } from "@/components/common/Placeholder";

export function EntityAnalysis() {
  const { entityId } = useParams();
  return (
    <Placeholder
      kicker="workspace · entity"
      title={entityId ? `Entidad · ${entityId}` : "Entidad"}
      body="Vista de análisis de una entidad: señales activas, conexiones, línea temporal, exposición sectorial, grafo ego. Pendiente de rediseño."
    />
  );
}
