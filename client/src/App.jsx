import { useEffect, useState } from "react";


const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";


export default function App() {
  const [apiStatus, setApiStatus] = useState("Checking backend...");

  useEffect(() => {
    async function checkBackend() {
      try {
        const response = await fetch(`${API_BASE_URL}/api/health`);
        if (!response.ok) throw new Error("Health check failed");

        const health = await response.json();
        setApiStatus(`Backend ready · MongoDB ${health.database}`);
      } catch {
        setApiStatus("Backend is not running");
      }
    }

    checkBackend();
  }, []);

  return (
    <main className="page-shell">
      <section className="agent-card">
        <p className="eyebrow">Courier delivery assistant</p>
        <h1>CallSaul</h1>
        <p className="description">
          Reschedule a delivery by speaking naturally with the voice agent.
        </p>

        <div className="status-row" role="status">
          <span className="status-dot" aria-hidden="true" />
          {apiStatus}
        </div>

        <button type="button" disabled>
          Voice setup coming next
        </button>
      </section>
    </main>
  );
}

