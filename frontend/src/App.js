// frontend/src/App.js
import React, { useEffect, useState } from "react";
import io from "socket.io-client";
import BookingForm from "./components/BookingForm";
import RideMap from "./components/RideMap";
import StatsPanel from "./components/StatsPanel";
import "./App.css";
import "leaflet/dist/leaflet.css";

const BACKEND = "http://localhost:5000"; // change if different

function App() {
  const [socket, setSocket] = useState(null);
  const [latestPosition, setLatestPosition] = useState(null);
  const [latestStats, setLatestStats] = useState(null);
  const [currentBooking, setCurrentBooking] = useState(null);
  const [mapSelectMode, setMapSelectMode] = useState(null); // 'pickup' | 'drop' | null

  useEffect(() => {
    const s = io(BACKEND, {
      transports: ["websocket", "polling"],
      reconnection: true,
    });

    s.on("connect", () => {
      console.log("Socket connected");
    });

    s.on("taxi_position", (data) => {
      // taxi_position events come from backend Kafka consumer forwarding taxi-positions topic
      setLatestPosition(data);
    });

    s.on("ride_stats", (data) => {
      setLatestStats(data);
    });

    s.on("disconnect", () => {
      console.log("Socket disconnected");
    });

    setSocket(s);
    return () => s.close();
  }, []);

  // called when a successful booking is created (from BookingForm)
  const handleBookingCreated = (booking) => {
    setCurrentBooking(booking);
  };

  return (
    <div className="app-root">
      <header className="app-header">
        <h1>🚕 Real-time Taxi (OSRM + Kafka)</h1>
      </header>

      <div className="app-main">
        <aside className="left-panel">
          <BookingForm
            backendUrl={BACKEND}
            onBookingCreated={handleBookingCreated}
            mapSelectMode={mapSelectMode}
            setMapSelectMode={setMapSelectMode}
          />
          <StatsPanel stats={latestStats} />
        </aside>

        <main className="map-panel">
          <RideMap
            socket={socket}
            latestPosition={latestPosition}
            currentBooking={currentBooking}
            mapSelectMode={mapSelectMode}
            setMapSelectMode={setMapSelectMode}
          />
        </main>
      </div>
    </div>
  );
}

export default App;
