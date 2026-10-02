import { useEffect, useState } from "react";

import { useWebSocket } from "./hooks/useWebSocket.js";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";


export default function App() {
  const [apiStatus, setApiStatus] = useState("Checking backend...");
  const [input, setInput] = useState("");
  const { connect, disconnect, error, messages, sendMessage, status } =
    useWebSocket();

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

  function handleSubmit(event) {
    event.preventDefault();
    if (sendMessage(input)) setInput("");
  }

  const isConnected = status === "connected";

  return (
    <main className="page-shell">
      <section className="agent-card">
        <p className="eyebrow">Courier delivery assistant</p>
        <h1>CallSaul</h1>
        <p className="description">
          Verify the browser and FastAPI WebSocket connection before adding voice logic.
        </p>

        <div className="status-grid">
          <div className="status-row" role="status">
            <span className="status-dot" aria-hidden="true" />
            {apiStatus}
          </div>
          <div className="session-details">
            <span>WebSocket</span>
            <strong>{status}</strong>
          </div>
        </div>

        {!isConnected ? (
          <button className="primary-button" type="button" onClick={connect}>
            {status === "connecting" ? "Connecting..." : "Connect WebSocket"}
          </button>
        ) : (
          <>
            <div className="conversation" aria-live="polite">
              {messages.map((message) => (
                <div className={`message ${message.role}`} key={message.id}>
                  <span>{message.role === "server" ? "Server" : "You"}</span>
                  <p>{message.text}</p>
                </div>
              ))}
            </div>

            <form className="message-form" onSubmit={handleSubmit}>
              <label htmlFor="message">Your message</label>
              <div className="message-controls">
                <input
                  id="message"
                  maxLength={300}
                  onChange={(event) => setInput(event.target.value)}
                  placeholder="Type a test message"
                  value={input}
                />
                <button className="send-button" type="submit" disabled={!input.trim()}>
                  Send
                </button>
              </div>
            </form>

            <button className="secondary-button" type="button" onClick={disconnect}>
              Disconnect
            </button>
          </>
        )}

        {error && <p className="error-message">{error}</p>}
      </section>
    </main>
  );
}
