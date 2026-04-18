import { useParams } from "react-router";

import { Placeholder } from "@/components/common/Placeholder";

export function Cases() {
  const { caseId } = useParams();
  return (
    <Placeholder
      kicker="workspace · cases"
      title={caseId ? `Caso · ${caseId}` : "Casos"}
      body="Gestión interna de investigaciones: borradores, revisión, publicación al archivo público. Pendiente de rediseño."
    />
  );
}
