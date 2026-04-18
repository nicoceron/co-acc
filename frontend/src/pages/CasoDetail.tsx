import { useParams } from "react-router";

import { Placeholder } from "@/components/common/Placeholder";

export function CasoDetail() {
  const { slug } = useParams();
  return (
    <Placeholder
      kicker="caso"
      title={slug ? `Dossier · ${slug}` : "Dossier"}
      body="Dossier individual con entidades, vínculos, señales activadas, línea de tiempo y archivos oficiales citados. Pendiente de rediseño."
    />
  );
}
