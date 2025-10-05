export const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";


export async function fetchCompanies() {
  const r = await fetch(`${API_BASE}/companies`);
  return r.json();
}


export async function searchCompanies(q) {
    const r = await fetch(`${API_BASE}/search?q=` + encodeURIComponent(q));
  return r.json();
}


export async function getStatus() {
  const r = await fetch(`${API_BASE}/status`);
    return r.json();
}


export async function getGraph(ticker) {
    const r = await fetch(`${API_BASE}/graphs?ticker=` + encodeURIComponent(ticker));
  if (!r.ok) throw new Error(await r.text());
    return r.json();
}


export async function triggerAndWait(ticker) {
  const r = await fetch(`${API_BASE}/trigger-and-wait`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ticker }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json(); // GraphResponse
}

// in your UI, replace the old trigger+poll with:
setLoading(true);
setPipelineMsg("Running pipeline…");
triggerAndWait(selected.ticker)
  .then(({ points }) => setSeries(points))
  .catch(e => setPipelineMsg(String(e)))
  .finally(() => setLoading(false));