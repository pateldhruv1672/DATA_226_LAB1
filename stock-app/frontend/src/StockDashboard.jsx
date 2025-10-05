// src/StockDashboard.jsx
import React, { useRef, useState } from "react";
import CompanySearch from "./components/CompanySearch";
import StockChart from "./components/StockChart";

const API_BASE = "/api"; // set Vite proxy to backend

async function fetchJson(url, opts) {
  const res = await fetch(url, opts);
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { json = null; }
  return { res, json };
}

export default function StockDashboard() {
  const [selected, setSelected] = useState(null);
  const [phase, setPhase] = useState("idle"); // idle|loading|waiting|ready|error
  const [msg, setMsg] = useState("");
  const [data, setData] = useState([]);
  const pollRef = useRef(null);

  function stopPoll() {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
  }

  async function getGraphsFor(ticker, name) {
    setPhase("loading"); setMsg("Fetching time series…"); setData([]); stopPoll();

    // 1) Try your consolidated backend: /graphs may do everything
    const { res, json } = await fetchJson(`${API_BASE}/graphs?ticker=${encodeURIComponent(ticker)}`);
    if (res.ok && json?.points) {
      setPhase("ready"); setMsg(""); setData(json.points); return;
    }

    // 2) Fallback flow (old behavior): 409 == table missing → trigger + poll + refetch
    if (res.status === 409) {
      setPhase("waiting");
      setMsg("Table not found. Triggering pipeline and waiting for data…");

      const body = JSON.stringify({ ticker });
      const trig = await fetch(`${API_BASE}/trigger`, { method: "POST", headers: { "Content-Type": "application/json" }, body });
      if (!trig.ok) { setPhase("error"); setMsg("Failed to trigger pipeline."); return; }

      const start = Date.now();
      const TIMEOUT = 5 * 60 * 1000; // 5m
      const INTERVAL = 3000;

      pollRef.current = setInterval(async () => {
        if (Date.now() - start > TIMEOUT) {
          stopPoll(); setPhase("error"); setMsg("Timed out waiting for the table."); return;
        }
        try {
          const s = await fetchJson(`${API_BASE}/status?ticker=${encodeURIComponent(ticker)}`);
          if (s.res.ok && s.json?.table_exists) {
            stopPoll();
            const again = await fetchJson(`${API_BASE}/graphs?ticker=${encodeURIComponent(ticker)}`);
            if (again.res.ok && again.json?.points) {
              setPhase("ready"); setMsg(""); setData(again.json.points);
            } else {
              setPhase("error"); setMsg(`Failed to fetch data after table was ready (HTTP ${again.res.status}).`);
            }
          }
        } catch { /* ignore transient errors during poll */ }
      }, INTERVAL);

      return;
    }

    setPhase("error");
    setMsg(json?.detail ? `Error: ${json.detail}` : `HTTP ${res.status}`);
  }

  return (
    <div className="p-8 space-y-6 max-w-5xl mx-auto">
      <h1 className="text-2xl font-bold text-center">Stock Forecasting</h1>

      <CompanySearch
        onGetGraphs={(c) => {
          setSelected(c);
          getGraphsFor(c.ticker, c.name);
        }}
      />

      {selected && (
        <div className="text-center text-sm text-gray-600">
          Selected: <b>{selected.name}</b> ({selected.ticker})
        </div>
      )}

      {msg && (
        <div className="rounded-xl border bg-white p-3 text-sm shadow-sm">{msg}</div>
      )}

      {phase === "ready" && <StockChart data={data} title={`${selected?.name} (${selected?.ticker})`} />}
    </div>
  );
}
