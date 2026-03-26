// src/components/dashboard/FlightMap.tsx
"use client";

import { useEffect, useRef, useState } from "react";
import type { Layer, Map as LeafletMap, Marker } from "leaflet";
import type { Flight } from "@/lib/api";

// Must import CSS in layout.tsx or page.tsx:
// import "leaflet/dist/leaflet.css";

interface Props {
  flights: Flight[];
  selected: number | null;
  onSelect: (id: number | null) => void;
}

export default function FlightMap({ flights, selected, onSelect }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<LeafletMap | null>(null);
  const markersRef = useRef<Record<number, Marker>>({});
  const overlaysRef = useRef<Layer[]>([]);
  const [ready, setReady] = useState(false);

  // Init map
  useEffect(() => {
    if (mapRef.current || typeof window === "undefined") return;

    const initMap = async () => {
      const L = (await import("leaflet")).default;
      if (!containerRef.current || mapRef.current) return;

      const map = L.map(containerRef.current, {
        center: [25, 40],
        zoom: 3,
        minZoom: 2,
        maxZoom: 12,
        zoomControl: true,
        worldCopyJump: true,
      });

      L.tileLayer(
        "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        {
          attribution:
            '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
          subdomains: "abcd",
          maxZoom: 19,
        }
      ).addTo(map);

      mapRef.current = map;
      setReady(true);
    };

    initMap();

    return () => {
      if (mapRef.current) {
        mapRef.current.remove();
        mapRef.current = null;
        setReady(false);
      }
    };
  }, []);

  // Update markers
  useEffect(() => {
    if (!ready || !mapRef.current) return;
    const loadL = async () => {
      const L = (await import("leaflet")).default;
      const map = mapRef.current;
      if (!map) return;

      // Remove stale markers
      const ids = new Set(flights.map((f) => f.flight_id));
      Object.keys(markersRef.current).forEach((k) => {
        const id = Number(k);
        if (!ids.has(id)) {
          map.removeLayer(markersRef.current[id]);
          delete markersRef.current[id];
        }
      });

      flights.forEach((f) => {
        const isSel = selected === f.flight_id;
        const sz = isSel ? 26 : 16;
        const color = f.flight_status === "landed" ? "#60a5fa" : "#facc15";
        const opacity = f.flight_status === "landed" ? 0.35 : 1;
        const rot = (f.direction || 0) - 45;
        const glow = isSel
          ? "filter:drop-shadow(0 0 6px " + color + ");"
          : "";

        const icon = L.divIcon({
          className: "",
          html:
            '<div style="transform:rotate(' + rot + "deg);" + glow + 'transition:transform 0.4s;">' +
            '<svg width="' + sz + '" height="' + sz + '" viewBox="0 0 24 24" fill="' + color + '" opacity="' + opacity + '">' +
            '<path d="M21 16v-2l-8-5V3.5A1.5 1.5 0 0 0 11.5 2 1.5 1.5 0 0 0 10 3.5V9l-8 5v2l8-2.5V19l-2 1.5V22l3.5-1 3.5 1v-1.5L13 19v-5.5l8 2.5z"/>' +
            "</svg></div>",
          iconSize: [sz, sz],
          iconAnchor: [sz / 2, sz / 2],
        });

        const lat = f.current_location?.latitude ?? f.lat;
        const lon = f.current_location?.longitude ?? f.lon;

        if (markersRef.current[f.flight_id]) {
          markersRef.current[f.flight_id].setLatLng([lat, lon]);
          markersRef.current[f.flight_id].setIcon(icon);
        } else {
          const marker = L.marker([lat, lon], { icon, interactive: true });
          marker.on("click", () => onSelect(f.flight_id));
          marker.bindTooltip(
            '<div style="font-family:system-ui;font-size:11px;line-height:1.5;">' +
            "<b>" + f.airline + "</b><br/>" +
            f.departure_city + " → " + f.arrival_city + "<br/>" +
            "ALT: " + (f.current_altitude_m || 0).toLocaleString() + "m | SPD: " + (f.current_speed_km_h || f.speed || 0) + " km/h</div>",
            { direction: "top", offset: [0, -10] }
          );
          marker.addTo(map);
          markersRef.current[f.flight_id] = marker;
        }
      });
    };
    loadL();
  }, [flights, selected, onSelect, ready]);

  // Route overlay
  useEffect(() => {
    if (!ready || !mapRef.current) return;
    const drawRoute = async () => {
      const L = (await import("leaflet")).default;
      const map = mapRef.current;
      if (!map) return;

      overlaysRef.current.forEach((layer) => {
        try { map.removeLayer(layer); } catch {}
      });
      overlaysRef.current = [];

      if (selected === null) return;
      const f = flights.find((fl) => fl.flight_id === selected);
      if (!f) return;

      const depLat = f.lat;
      const depLon = f.lon;
      const arrLat = f.dest_lat;
      const arrLon = f.dest_lon;

      if (arrLat == null || arrLon == null) return;

      // Dashed route
      const pts: [number, number][] = [];
      for (let t = 0; t <= 1; t += 0.02) {
        pts.push([depLat + (arrLat - depLat) * t, depLon + (arrLon - depLon) * t]);
      }
      const line = L.polyline(pts, {
        color: "#06b6d4", weight: 1.5, opacity: 0.35, dashArray: "6,5",
      });
      line.addTo(map);
      overlaysRef.current.push(line);

      // Dep/Arr dots
      const depIcon = L.divIcon({
        className: "",
        html: '<div style="width:10px;height:10px;background:#22d3ee;border-radius:50%;border:2px solid #0e7490;box-shadow:0 0 6px #22d3ee;"></div>',
        iconSize: [10, 10], iconAnchor: [5, 5],
      });
      const arrIcon = L.divIcon({
        className: "",
        html: '<div style="width:10px;height:10px;background:#f472b6;border-radius:50%;border:2px solid #be185d;box-shadow:0 0 6px #f472b6;"></div>',
        iconSize: [10, 10], iconAnchor: [5, 5],
      });

      const dm = L.marker([depLat, depLon], { icon: depIcon, interactive: false });
      dm.bindTooltip(f.departure_city, { direction: "bottom" });
      dm.addTo(map);
      overlaysRef.current.push(dm);

      const am = L.marker([arrLat, arrLon], { icon: arrIcon, interactive: false });
      am.bindTooltip(f.arrival_city, { direction: "bottom" });
      am.addTo(map);
      overlaysRef.current.push(am);
    };
    drawRoute();
  }, [selected, flights, ready]);

  return (
    <div className="relative w-full h-[500px]">
      <div ref={containerRef} className="w-full h-full rounded-xl bg-slate-900" />
      {!ready && (
        <div className="absolute inset-0 flex items-center justify-center bg-slate-900 rounded-xl">
          <div className="text-slate-500 text-sm flex items-center gap-2">
            <div className="w-4 h-4 border-2 border-cyan-500 border-t-transparent rounded-full animate-spin" />
            Loading map...
          </div>
        </div>
      )}
    </div>
  );
}
