// src/components/StockChart.jsx
import React from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";

export default function StockChart({ data, title }) {
  if (!data?.length) return null;
  return (
    <div className="h-96 w-full">
      <div className="mb-2 text-center font-semibold">{title}</div>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Line type="monotone" dataKey="close" name="Actual Close" stroke="#4F46E5" dot={false} />
          <Line type="monotone" dataKey="close_predicted" name="Predicted Close" stroke="#22C55E" dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
