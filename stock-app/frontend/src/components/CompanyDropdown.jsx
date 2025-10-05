// src/components/CompanyDashboard.jsx
import React, { useState, useRef } from "react";
import CompanyDropdown from "./CompanyDropdown";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";

// If you set a Vite proxy to http://localhost:8000 under "/api", use API_BASE = "/api"
// Otherwise set API_BASE = "http://localhost:8000"
const API_BASE = "/api";

export default function CompanyDashboard() {
  const [selected, setSelected] = useState(null);
  const [data, setData] = useState([]);
  const [phase, setPhase] = useState("idle"); // idle | loading | triggering | waiting | ready | error
  const [message, setMessage] = useState("");
  const pollTimer = useRef(null);

  const POLL_INTERVAL_MS = 3000;   // poll /status every 3s
  const POLL_TIMEOUT_MS = 2 * 60 * 1000; // stop after 2 minutes

  function clearPoll() {
    if (pollTimer.current) {
      clearInterval(pollTimer.current);
      pollTimer.current = null;
    }
  }

  async function fetchGraphs(ticker) {
    setPhase("loading");
    setMessage("Fetching time series…");
    setData([]);

    const res = await fetch(`${API_BASE}/graphs?ticker=${encodeURIComponent(ticker)}`);
    if (res.ok) {
      const json = await res.json();
      setData(json.points || []);
      setPhase("ready");
      setMessage("");
      return;
    }

    if (res.status === 409) {
      // Table not ready → trigger pipeline and poll
      setPhase("triggering");
      setMessage("Analytics table not ready. Triggering pipeline…");
      const tRes = await fetch(`${API_BASE}/trigger`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ticker }), // this will set Airflow variable yf_tickers=["<ticker>"] and start DAG
      });

      if (!tRes.ok) {
        setPhase("error");
        setMessage("Failed to trigger pipeline.");
        return;
      }

      // Start polling /status until table exists, then re-fetch graphs
      setPhase("waiting");
      setMessage("Pipeline running. Waiting for table to be created…");

      const start = Date.now();
      clearPoll();
      pollTimer.current = setInterval(async () => {
        // timeout guard
        if (Date.now() - start > POLL_TIMEOUT_MS) {
          clearPoll();
          setPhase("error");
          setMessage("Timed out waiting for analytics table.");
          return;
        }
        try {
          const sRes = await fetch(`${API_BASE}/status`);
          if (!sRes.ok) return; // keep polling
          const sJson = await sRes.json();
          if (sJson.table_exists) {
            clearPoll();
            // table exists → get data
            fetchGraphs(ticker);
          }
        } catch {
          // ignore transient errors; keep polling
        }
      }, POLL_INTERVAL_MS);

      return;
    }

    // Any other error
    setPhase("error");
    setMessage(`Failed to load data (HTTP ${res.status}).`);
  }

  return (
    <div className="p-8 space-y-6 max-w-5xl mx-auto">
      <h1 className="text-2xl font-bold text-center">Company Analytics Dashboard</h1>

      <CompanyDropdown
        apiUrl="/companies.json"
        onSelect={(company) => {
          clearPoll();
          setSelected(company);
          fetchGraphs(company.ticker);
        }}
      />

      {selected && (
        <div className="text-center text-sm text-gray-600">
          Selected: <strong>{selected.name}</strong> ({selected.ticker})
        </div>
      )}

      {message && (
        <div className="rounded-xl border bg-white p-3 text-sm shadow-sm">
          {message}
        </div>
      )}

      {phase === "ready" && data.length > 0 && (
        <div className="h-96 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="close" stroke="#4F46E5" name="Actual Close" dot={false} />
              <Line type="monotone" dataKey="close_predicted" stroke="#22C55E" name="Predicted Close" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {phase === "ready" && data.length === 0 && selected && (
        <p className="text-center text-gray-600">
          No data available for {selected.name} ({selected.ticker}).
        </p>
      )}
    </div>
  );
}
