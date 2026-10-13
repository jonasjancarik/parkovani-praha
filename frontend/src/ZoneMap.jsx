import { useEffect, useRef } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

import { formatDistance, formatNumber } from "./format.js";

const PRAGUE_CENTER = [50.0755, 14.4378];
const INK = "#24231f";
const TEAL = "#114f50";

function zoneStyle(feature) {
  const isReference = feature.properties.is_reference;
  return {
    color: isReference ? INK : TEAL,
    weight: isReference ? 2.5 : 1,
    fillColor: TEAL,
    fillOpacity: isReference ? 0.34 : 0.16,
  };
}

export function ZoneMap({ point, radius, geometry, zones, bounds, onPick }) {
  const elementRef = useRef(null);
  const mapRef = useRef(null);
  const layersRef = useRef(null);
  const onPickRef = useRef(onPick);
  onPickRef.current = onPick;

  useEffect(() => {
    const map = L.map(elementRef.current, {
      center: PRAGUE_CENTER,
      zoom: 12,
      minZoom: 10,
      // Scrolling the page must not zoom the map by accident.
      scrollWheelZoom: false,
      maxBounds: bounds
        ? [[bounds.lat[0], bounds.lon[0]], [bounds.lat[1], bounds.lon[1]]]
        : undefined,
    });
    map.attributionControl.setPrefix(false);
    L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: '© <a href="https://www.openstreetmap.org/copyright">přispěvatelé OpenStreetMap</a>',
      maxZoom: 19,
    }).addTo(map);
    map.on("click", (event) => onPickRef.current(event.latlng.lng, event.latlng.lat));
    layersRef.current = L.layerGroup().addTo(map);
    mapRef.current = map;
    const observer = new ResizeObserver(() => map.invalidateSize());
    observer.observe(elementRef.current);
    return () => {
      observer.disconnect();
      map.remove();
    };
  }, [bounds]);

  useEffect(() => {
    const map = mapRef.current;
    const layers = layersRef.current;
    layers.clearLayers();
    if (!point) return;

    const center = [point.lat, point.lon];
    const circle = L.circle(center, {
      radius,
      color: INK,
      weight: 1.5,
      dashArray: "5 5",
      fill: false,
      interactive: false,
    }).addTo(layers);
    if (geometry) {
      const zoneByCode = new Map((zones ?? []).map((zone) => [zone.code, zone]));
      L.geoJSON(geometry, {
        style: zoneStyle,
        onEachFeature: (feature, layer) => {
          const zone = zoneByCode.get(feature.properties.code);
          const details = zone
            ? ` · ${formatNumber(zone.spaces)} míst · ${formatDistance(zone.distance_m)}`
            : "";
          layer.bindTooltip(`${feature.properties.code}${details}`, { sticky: true });
        },
      }).addTo(layers);
    }
    L.circleMarker(center, {
      radius: 6,
      color: "#fffdf8",
      weight: 2,
      fillColor: INK,
      fillOpacity: 1,
      interactive: false,
    }).addTo(layers);
    map.fitBounds(circle.getBounds(), { padding: [24, 24] });
  }, [point, radius, geometry, zones]);

  return (
    <div
      ref={elementRef}
      className="zone-map"
      role="application"
      aria-label="Mapa Prahy. Kliknutím vyberete místo, pro které chcete zobrazit okolní parkovací zóny."
    />
  );
}
