// frontend/src/components/RideMap.jsx
import React, { useEffect, useRef, useState } from "react";
import { MapContainer, TileLayer, Marker, Polyline, useMapEvents } from "react-leaflet";
import L from "leaflet";
import TaxiMarker from "./TaxiMarker";

/*
 Props:
  - socket: socket.io client
  - latestPosition: last position object from socket (taxi_position)
  - currentBooking: booking object returned from backend after booking
  - mapSelectMode, setMapSelectMode: when selecting pickup/drop on map
*/

const DEFAULT_CENTER = [40.758, -73.985]; // Times Square (NYC) — change if you want

function MapClickHandler({ mapSelectMode, setCoordsCallback }) {
  // listens for clicks and sends lat/lng back through setCoordsCallback
  useMapEvents({
    click(e) {
      if (!mapSelectMode) return;
      const { lat, lng } = e.latlng;
      setCoordsCallback({ lat: lat.toFixed(6), lon: lng.toFixed(6), mode: mapSelectMode });
      // disable mode after one pick
      // (we don't have direct access to setMapSelectMode here, so caller should handle)
    }
  });
  return null;
}

export default function RideMap({
  socket,
  latestPosition,
  currentBooking,
  mapSelectMode,
  setMapSelectMode
}) {
  const [pickup, setPickup] = useState(null);
  const [dropoff, setDropoff] = useState(null);
  const [routePoints, setRoutePoints] = useState([]); // array of [lat, lon] drawn progressively
  const [taxiPos, setTaxiPos] = useState(null); // current taxi location for animation
  const [fullRoute, setFullRoute] = useState([]); // full route coordinates (for reference)
  const mapRef = useRef(null);

  // when user chooses coordinates on map (from BookingForm)
  useEffect(() => {
    // listens to clicks via a custom DOM event fired below in this component
    function handler(e) {
      const detail = e.detail;
      if (!detail) return;
      const { lat, lon, mode } = detail;
      if (mode === "pickup") {
        setPickup({ lat: parseFloat(lat), lon: parseFloat(lon) });
        setMapSelectMode(null);
      } else if (mode === "drop") {
        setDropoff({ lat: parseFloat(lat), lon: parseFloat(lon) });
        setMapSelectMode(null);
      }
    }
    window.addEventListener("map-select", handler);
    return () => window.removeEventListener("map-select", handler);
  }, [setMapSelectMode]);

  // allow BookingForm to set coordinates by dispatching a window event
  const setCoordsCallback = ({ lat, lon, mode }) => {
    const ev = new CustomEvent("map-select", { detail: { lat, lon, mode } });
    window.dispatchEvent(ev);
  };

  // expose map click handler
  useEffect(() => {
    if (!mapSelectMode) return;
    // start listening to clicks using a small timeout to allow map to be ready
    // MapClickHandler component uses useMapEvents — we need to render it conditionally below.
    // Nothing extra required here; BookingForm requested mapSelectMode and MapClickHandler will capture the click
  }, [mapSelectMode]);

  // When a booking is created, center map to pickup & pan and clear previous route
  useEffect(() => {
    if (!currentBooking) return;
    // set markers from booking
    setPickup({ lat: currentBooking.pickup_lat, lon: currentBooking.pickup_lon });
    setDropoff({ lat: currentBooking.dropoff_lat, lon: currentBooking.dropoff_lon });
    setRoutePoints([]); // will be filled progressively by taxi_position events
    setFullRoute([]); // backend drives the route; we only receive points from kafka
    // pan map to pickup
    const map = mapRef.current;
    if (map && map.flyTo) {
      map.flyTo([currentBooking.pickup_lat, currentBooking.pickup_lon], 13, { duration: 1.0 });
    }
  }, [currentBooking]);

  // When socket emits latestPosition, update taxi and append route point
  useEffect(() => {
    if (!latestPosition) return;
    // last message includes 'latitude' 'longitude' 'progress_percent' 'point_index' etc.
    const lat = latestPosition.latitude;
    const lon = latestPosition.longitude;
    // update taxi pos
    setTaxiPos({ lat, lon, progress: latestPosition.progress_percent });

    // Add to progressive polyline
    setRoutePoints(prev => {
      // avoid duplicates by comparing with last
      if (prev.length > 0) {
        const last = prev[prev.length - 1];
        if (Math.abs(last[0] - lat) < 1e-6 && Math.abs(last[1] - lon) < 1e-6) {
          return prev;
        }
      }
      return [...prev, [lat, lon]];
    });

    // auto-pan to taxi if zoomed out too far
    const map = mapRef.current;
    if (map) {
      const bounds = map.getBounds();
      if (!bounds.contains([lat, lon])) {
        map.panTo([lat, lon]);
      }
    }
  }, [latestPosition]);

  // wire up a custom global click handler: map clicks create a CustomEvent that BookingForm or parent listens to
  function onMapClick(e) {
    if (!mapSelectMode) return;
    const lat = e.latlng.lat.toFixed(6);
    const lon = e.latlng.lng.toFixed(6);
    setCoordsCallback({ lat, lon, mode: mapSelectMode });
    setMapSelectMode(null);
  }

  // small helper to render pickup / drop markers
  const PickupMarker = ({ pos }) =>
    pos ? <Marker position={[pos.lat, pos.lon]} /> : null;

  return (
    <div className="ride-map">
      <MapContainer
        center={DEFAULT_CENTER}
        zoom={12}
        style={{ height: "100%", width: "100%" }}
        whenCreated={(mapInstance) => { mapRef.current = mapInstance; }}
        onClick={onMapClick}
      >
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
        {/* Map click handler component only active when selecting */}
        {mapSelectMode ? <MapClickHandler mapSelectMode={mapSelectMode} setCoordsCallback={setCoordsCallback} /> : null}

        {/* draw progressive route */}
        {routePoints.length > 0 ? <Polyline positions={routePoints} weight={5} /> : null}

        {/* full static route (optional) */}
        {fullRoute.length > 0 ? <Polyline positions={fullRoute} color="#cccccc" weight={3} dashArray="5,10" /> : null}

        {/* pickup & drop markers */}
        {pickup ? <Marker position={[pickup.lat, pickup.lon]}><></></Marker> : null}
        {dropoff ? <Marker position={[dropoff.lat, dropoff.lon]}><></></Marker> : null}

        {/* animated taxi */}
        {taxiPos ? <TaxiMarker position={[taxiPos.lat, taxiPos.lon]} progress={taxiPos.progress} /> : null}
      </MapContainer>
    </div>
  );
}
