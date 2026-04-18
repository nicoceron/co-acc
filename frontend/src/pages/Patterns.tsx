import { useParams } from "react-router";

import { Placeholder } from "@/components/common/Placeholder";

export function Patterns() {
  const { entityId } = useParams();
  return (
    <Placeholder
      kicker="workspace · patterns"
      title={entityId ? `Patrones · ${entityId}` : "Patrones"}
      body="Detección de patrones cruzados entre fuentes: concentración de proveedores, ventanas cortas de adjudicación, co-licitación, capturas territoriales. Pendiente de rediseño."
    />
  );
}
