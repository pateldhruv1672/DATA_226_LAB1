import { useEffect, useRef } from "react";
import { triggerStream, getGraph } from "../api";

export function useDagSse(ticker, onReady, setMsg) {
  const esRef = useRef(null);

  useEffect(() => {
    if (!ticker) return;

    // Close any previous stream
    if (esRef.current) {
      esRef.current.close();
      esRef.current = null;
    }

    const es = triggerStream(ticker);
    esRef.current = es;

    es.addEventListener("started", () => setMsg?.("Pipeline started…"));

    es.onmessage = (e) => {
      try {
        const { state, elapsed_sec } = JSON.parse(e.data);
        if (state) setMsg?.(`DAG state: ${state} (${elapsed_sec}s)`);
      } catch {
        // ignore parse errors on heartbeats
      }
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
      setMsg?.("SSE connection error — will retry automatically.");
      // EventSource auto-retries as per server's `retry:` hint
    };

    return () => {
      es.close();
      esRef.current = null;
    };
  }, [ticker, onReady, setMsg]);
}
