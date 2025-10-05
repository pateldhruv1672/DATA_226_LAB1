import { useEffect, useMemo, useState } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { fetchCompanies, searchCompanies, getStatus, getGraph, triggerPipeline } from "./api";

import { useEffect, useRef, useState } from "react";
import { triggerStream, getGraph } from "./api";

function useDagSse(ticker, onReady, setMsg) {
  const esRef = useRef(null);

  useEffect(() => {
    if (!ticker) return;

    // Close previous stream if any
    if (esRef.current) {
      esRef.current.close();
      esRef.current = null;
    }

    const es = triggerStream(ticker);
    esRef.current = es;

    es.addEventListener("started", (e) => {
      setMsg?.("Pipeline started…");
    });

    es.onmessage = (e) => {
      try {
        const { state, elapsed_sec } = JSON.parse(e.data);
        if (state) setMsg?.(`DAG state: ${state} (${elapsed_sec}s)`);
      } catch {}
    };

    es.addEventListener("ready", async () => {
      setMsg?.("Data ready — loading chart…");
      try {
        const { points } = await getGraph(ticker);
        onReady?.(points);
      } catch (err) {
        setMsg?.(`Fetch chart failed: ${String(err)}`);
      }
    });

    es.addEventListener("failed", (e) => {
      setMsg?.(`Pipeline failed: ${e.data || ""}`);
    });

    es.addEventListener("done", () => {
      es.close();
      esRef.current = null;
    });

    es.onerror = () => {
      setMsg?.("SSE connection error — retrying or please try again.");
      // EventSource will auto-retry based on 'retry:' hint we send.
    };

    return () => {
      es.close();
      esRef.current = null;
    };
  }, [ticker, onReady, setMsg]);
}

export function CompaniesUI() {
  const [companies, setCompanies] = useState([]);
  const [q, setQ] = useState("");
  const [selected, setSelected] = useState(null);
  const [series, setSeries] = useState([]);
  const [loading, setLoading] = useState(false);
    const [pipelineMsg, setPipelineMsg] = useState("");

  useEffect(() => {
        fetchCompanies().then(setCompanies);
  }, []);

  const filtered = useMemo(() => {
    if (!q) return companies;
        return companies.filter(c => c.name.toLowerCase().includes(q.toLowerCase()) || c.ticker.toLowerCase().includes(q.toLowerCase()));
  }, [companies, q]);

  async function onSemanticSearch() {
    if (!q) return;
      const res = await searchCompanies(q);
      setCompanies(res);
  }

  async function loadGraph(ticker) {
    setLoading(true);
        setPipelineMsg("");
    try {
      const status = await getStatus();
      if (!status.table_exists) {
                const trig = await triggerPipeline(ticker);
                setPipelineMsg("Preparing data via Airflow… polling for readiness…");
                // poll every 5s until ready
        let tries = 0;
                while (tries < 60) { // up to ~5 minutes
                    await new Promise(r => setTimeout(r, 5000));
          const st = await getStatus();
          if (st.table_exists) break;
          tries++;
        }
      }
      const g = await getGraph(ticker);
            setSeries(g.points);
    } catch (e) {
            setPipelineMsg(String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen p-6 max-w-6xl mx-auto">
      <h1 className="text-3xl font-semibold mb-4">Companies</h1>
      <div className="flex gap-2 mb-4">
                <input className="border rounded px-3 py-2 flex-1" placeholder="Search companies (semantic or keyword)…" value={q} onChange={e => setQ(e.target.value)} />
                <button className="px-4 py-2 rounded bg-black text-white" onClick={onSemanticSearch}>Semantic Search</button>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        <div className="md:col-span-1 border rounded p-3 overflow-auto h-[70vh]">
          <ul className="space-y-2">
            {filtered.map((c) => (
              <li key={c.ticker}>
                                <button className={`w-full text-left border rounded p-2 ${selected?.ticker === c.ticker ? 'bg-gray-100' : ''}`} onClick={() => { setSelected(c); loadGraph(c.ticker); }}>
                  <div className="font-medium">{c.name}</div>
                  <div className="text-sm text-gray-600">{c.ticker}</div>
                </button>
              </li>
            ))}
          </ul>
        </div>
        <div className="md:col-span-2 border rounded p-3">
          {!selected && <div className="text-gray-600">Select a company to view charts.</div>}
          {selected && (
            <div>
                            <h2 className="text-2xl font-semibold mb-2">{selected.name} <span className="text-gray-500">({selected.ticker})</span></h2>
                            {pipelineMsg && <div className="text-sm text-gray-700 mb-2">{pipelineMsg}</div>}
              {loading && <div className="text-sm">Loading…</div>}
              {!loading && series.length > 0 && (
                <div className="h-[420px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={series} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="date" minTickGap={32} />
                      <YAxis />
                      <Tooltip />
                      <Legend />
                      <Line type="monotone" dataKey="close" dot={false} />
                      <Line type="monotone" dataKey="close_predicted" dot={false} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
