// frontend/src/components/StatsPanel.jsx
import React, { useEffect, useState } from "react";

/*
 Props:
   - stats: last ride_stats message from socket
*/

export default function StatsPanel({ stats }) {
  const [last, setLast] = useState(null);

  useEffect(() => {
    if (stats) setLast(stats);
  }, [stats]);

  if (!last) {
    return (
      <div className="stats-card">
        <h3>Live Ride Stats</h3>
        <p>No ride active</p>
      </div>
    );
  }

  // show either ride_started, position updates not expected here. Backend sends start + complete events on ride-stats
  if (last.event === "ride_started") {
    return (
      <div className="stats-card">
        <h3>Ride Summary</h3>
        <div><strong>Ride ID:</strong> {last.ride_id}</div>
        <div><strong>Passenger:</strong> {last.passenger_name}</div>
        <div><strong>Distance:</strong> {last.distance_km?.toFixed(2)} km ({last.distance_miles?.toFixed(2)} mi)</div>
        <div><strong>ETA:</strong> {last.estimated_duration_min?.toFixed(1)} min</div>
        <div><strong>Estimated fare:</strong> ${last.estimated_fare?.toFixed(2)}</div>
        <div style={{ marginTop: 8 }}>
          <small>Points on route: {last.total_route_points}</small>
        </div>
      </div>
    );
  }

  if (last.event === "ride_completed") {
    return (
      <div className="stats-card">
        <h3>Ride Completed</h3>
        <div><strong>Ride ID:</strong> {last.ride_id}</div>
        <div><strong>Final fare:</strong> ${last.final_fare ?? last.estimated_fare}</div>
        <div><strong>Total distance:</strong> {(last.total_distance_km ?? 0).toFixed(2)} km</div>
        <div><strong>Duration:</strong> {(last.total_duration_min ?? 0).toFixed(1)} min</div>
      </div>
    );
  }

  // default fallback
  return (
    <div className="stats-card">
      <h3>Ride Stats</h3>
      <pre style={{ whiteSpace: "pre-wrap", fontSize: 12 }}>{JSON.stringify(last, null, 2)}</pre>
    </div>
  );
}
