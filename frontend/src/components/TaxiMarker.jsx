// frontend/src/components/TaxiMarker.jsx
import React from "react";
import { Marker } from "react-leaflet";
import L from "leaflet";

const taxiIcon = new L.Icon({
  iconUrl: "https://cdn-icons-png.flaticon.com/512/685/685352.png", // small taxi icon (remote). Replace with local asset if you prefer.
  iconSize: [36, 36],
  iconAnchor: [18, 18],
  popupAnchor: [0, -18],
});

export default function TaxiMarker({ position }) {
  // position: [lat, lon]
  if (!position) return null;
  return <Marker position={position} icon={taxiIcon} />;
}
