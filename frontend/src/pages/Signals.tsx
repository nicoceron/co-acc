import { useParams } from "react-router";

import { Placeholder } from "@/components/common/Placeholder";

export function Signals() {
  const { signalId } = useParams();
  return (
    <Placeholder
      kicker="workspace · signals"
      title={signalId ? `Señal · ${signalId}` : "Señales"}
      body="Catálogo de 43 señales de cruce — Wave A en vivo, Wave B en materialización. Cada señal con fuentes, dependencias, y ejecuciones recientes. Pendiente de rediseño."
    />
  );
}
