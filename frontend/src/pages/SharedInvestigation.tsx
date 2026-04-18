import { useParams } from "react-router";

import { Placeholder } from "@/components/common/Placeholder";

export function SharedInvestigation() {
  const { token } = useParams();
  return (
    <Placeholder
      kicker="shared"
      title="Investigación compartida"
      body={
        token
          ? `Vista de solo lectura para el token ${token}. Pendiente de rediseño.`
          : "Vista de solo lectura para investigaciones enlazadas. Pendiente de rediseño."
      }
    />
  );
}
