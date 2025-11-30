import React, { useState, useEffect } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import './App.css';

function App() {
  const [strikes, setStrikes] = useState([]);
  const [clusters, setClusters] = useState([]);
  const [stats, setStats] = useState({ strikes: 0, clusters: 0, lastUpdate: null });

  const fetchData = async () => {
    try {
      // Use a relative path so the frontend works both when served by backend and when
      // you deploy under the same origin. If you run CRA dev server and need to reach a
      // backend on another port, set up a proxy or use an env var.
      const response = await fetch('/api/lightning');
      if (!response.ok) {
        throw new Error(`API returned ${response.status}`);
      }
      const data = await response.json();
      const strikesData = data.strikes || [];
      const clustersData = data.clusters || [];
      setStrikes(strikesData);
      setClusters(clustersData);
      setStats({
        strikes: strikesData.length,
        clusters: clustersData.length,
        lastUpdate: new Date().toLocaleTimeString()
      });
    } catch (err) {
      // Show the full error in console so it's easier to diagnose connectivity/CORS errors
      console.error('Error fetching lightning data:', err);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  const getColor = (count) => {
    const colors = ['#ffeda0', '#fed976', '#feb24c', '#fd8d3c', '#fc4e2a', '#e31a1c', '#bd0026'];
    return colors[Math.min(Math.floor(count / 2), colors.length - 1)];
  };

  return (
    <div style={{ height: '100vh', position: 'relative' }}>
      <MapContainer
        center={[20, 0]}
        zoom={2}
        style={{ height: '100%', width: '100%' }}
      >
        <TileLayer
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          attribution='© OpenStreetMap'
        />

        {/* Individual strikes */}
        {strikes.map((strike, index) => (
          <CircleMarker
            key={`strike-${index}`}
            center={[strike.lat, strike.lon]}
            radius={2}
            fillColor="#ff4444"
            color="#ff0000"
            weight={1}
            opacity={0.6}
            fillOpacity={0.4}
          />
        ))}

        {/* Heatmap clusters */}
        {clusters.map((cluster, index) => (
          <CircleMarker
            key={`cluster-${index}`}
            center={[cluster.center.lat, cluster.center.lon]}
            radius={Math.sqrt(cluster.count) * 3}
            fillColor={getColor(cluster.count)}
            color={getColor(cluster.count)}
            weight={2}
            opacity={0.8}
            fillOpacity={0.3}
          >
            <Popup>
              <div>
                <strong>Lightning Cluster</strong><br />
                Strikes: {cluster.count}<br />
                Center: {cluster.center.lat.toFixed(2)}, {cluster.center.lon.toFixed(2)}
              </div>
            </Popup>
          </CircleMarker>
        ))}
      </MapContainer>

      <div className="control-panel">
        <h3 style={{ marginBottom: '10px', color: '#60a5fa' }}>CMPSC 463 Heatmap</h3>
        <div className="stats">
          <div>Strikes: {stats.strikes}</div>
          <div>Clusters: {stats.clusters}</div>
          <div>Last: {stats.lastUpdate}</div>
          <div style={{ marginTop: '8px', fontSize: '10px', color: '#64748b' }}>
            Algorithms: BFS • Greedy • Prim's MST
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;