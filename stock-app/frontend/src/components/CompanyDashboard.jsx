// src/components/CompanyDashboard.jsx
import React, { useState } from "react";
import CompanyDropdown from "./CompanyDropdown";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";

export default function CompanyDashboard() {
  const [selected, setSelected] = useState(null);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  async function fetchGraph(ticker) {
    try {
      setLoading(true);
      setError(null);
      const res = await fetch(`http://localhost:8000/graphs?ticker=${ticker}`);
      if (!res.ok) throw new Error(`Backend returned ${res.status}`);
      const json = await res.json();
      setData(json.points);
    } catch (err) {
      console.error(err);
      setError("Failed to load graph data");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="p-8 space-y-6 max-w-4xl mx-auto">
      <h1 className="text-2xl font-bold text-center">Company Analytics Dashboard</h1>

      <CompanyDropdown
        apiUrl="http://localhost:8000/companies"
        onSelect={(c) => {
          setSelected(c);
          fetchGraph(c.ticker);
        }}
      />

      {loading && <p className="text-gray-500">Loading chart...</p>}
      {error && <p className="text-red-600">{error}</p>}

      {!loading && data.length > 0 && (
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
    </div>
  );
}
