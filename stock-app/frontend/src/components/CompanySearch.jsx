// src/components/CompanySearch.jsx
import React, { useEffect, useMemo, useState } from "react";
import Fuse from "fuse.js";

export default function CompanySearch({ onGetGraphs, sourceUrl = "/companies.json" }) {
  const [companies, setCompanies] = useState([]); // [{name, ticker}]
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState(null);
  const [results, setResults] = useState([]);

  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(sourceUrl);
        const data = await res.json();
        const list = Array.isArray(data)
          ? data
          : Object.entries(data).map(([name, ticker]) => ({ name, ticker }));
        setCompanies(list);
        setResults(list.slice(0, 20));
      } catch (e) {
        console.error("Failed to load companies.json", e);
        setCompanies([]); setResults([]);
      }
    })();
  }, [sourceUrl]);

  const fuse = useMemo(() => new Fuse(companies, {
    keys: [{ name: "name", weight: 0.7 }, { name: "ticker", weight: 0.3 }],
    threshold: 0.35,
    ignoreLocation: true,
    includeScore: true,
  }), [companies]);

  function onChange(e) {
    const q = e.target.value;
    setQuery(q);
    setSelected(null);
    if (!q.trim()) { setResults(companies.slice(0, 20)); return; }
    const hits = fuse.search(q).slice(0, 20).map(h => h.item);
    setResults(hits);
  }

  function pick(item) {
    setSelected(item);
    setQuery(`${item.name} (${item.ticker})`);
  }

  return (
    <div className="w-full max-w-2xl space-y-3">
      <div className="flex gap-2">
        <input
          value={query}
          onChange={onChange}
          placeholder="Search company name or ticker..."
          className="flex-1 rounded-xl border px-4 py-2 shadow-sm outline-none"
        />
        <button
          onClick={() => selected && onGetGraphs(selected)}
          disabled={!selected}
          className={`rounded-xl px-4 py-2 text-white ${selected ? "bg-indigo-600 hover:bg-indigo-700" : "bg-gray-400 cursor-not-allowed"}`}
        >
          Get Graphs
        </button>
      </div>

      {/* suggestions list */}
      {results.length > 0 && (
        <div className="rounded-xl border bg-white shadow-sm">
          {results.map((item) => (
            <div
              key={`${item.ticker}-${item.name}`}
              onClick={() => pick(item)}
              className="cursor-pointer px-4 py-2 text-sm hover:bg-indigo-50 flex justify-between"
            >
              <span>{item.name}</span>
              <span className="text-gray-500">{item.ticker}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
