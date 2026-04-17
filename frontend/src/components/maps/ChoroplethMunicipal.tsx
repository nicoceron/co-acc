import "maplibre-gl/dist/maplibre-gl.css";

import type { StyleSpecification } from "maplibre-gl";
import Map, { Layer, Source, type LayerProps } from "react-map-gl/maplibre";

export interface TerritorialHit {
  divipola?: string | null;
  municipality: string;
  department?: string | null;
  hits: number;
  sector?: string | null;
  geometry?: GeoJSON.Geometry | null;
}

const mapStyle: StyleSpecification = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "OpenStreetMap",
    },
  },
  layers: [{ id: "osm", type: "raster", source: "osm" }],
};

const fillLayer: LayerProps = {
  id: "municipal-hit-fill",
  type: "fill",
  paint: {
    "fill-color": [
      "interpolate",
      ["linear"],
      ["get", "hits"],
      0,
      "#fef3c7",
      10,
      "#f97316",
      30,
      "#991b1b",
    ],
    "fill-opacity": 0.58,
  },
};

export function ChoroplethMunicipal({
  data,
  onMunicipalityClick,
}: {
  data: TerritorialHit[];
  onMunicipalityClick?: (divipola: string) => void;
}) {
  const features = data
    .filter((row) => row.geometry)
    .map((row) => ({
      type: "Feature" as const,
      properties: {
        divipola: row.divipola ?? row.municipality,
        municipality: row.municipality,
        hits: row.hits,
      },
      geometry: row.geometry as GeoJSON.Geometry,
    }));

  if (features.length === 0) {
    return (
      <div>
        {data.slice(0, 6).map((row) => (
          <button
            key={`${row.divipola ?? row.municipality}-${row.sector ?? "all"}`}
            type="button"
            onClick={() => onMunicipalityClick?.(row.divipola ?? row.municipality)}
          >
            <span>{row.municipality}</span>
            <strong>{row.hits}</strong>
          </button>
        ))}
      </div>
    );
  }

  return (
    <Map
      initialViewState={{ longitude: -74.3, latitude: 4.6, zoom: 4.3 }}
      mapStyle={mapStyle}
      style={{ width: "100%", height: 360 }}
      interactiveLayerIds={["municipal-hit-fill"]}
      onClick={(event) => {
        const feature = event.features?.[0];
        const divipola = feature?.properties?.divipola;
        if (typeof divipola === "string") onMunicipalityClick?.(divipola);
      }}
    >
      <Source type="geojson" data={{ type: "FeatureCollection", features }}>
        <Layer {...fillLayer} />
      </Source>
    </Map>
  );
}
