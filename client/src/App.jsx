import { useEffect, useState } from "react";

import { useMicrophone } from "./hooks/useMicrophone.js";
import { useWebSocket } from "./hooks/useWebSocket.js";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";


export default function App() {
  const [apiStatus, setApiStatus] = useState("Checking backend...");
  const [input, setInput] = useState("");
  const {
    audioStats,
    connect,
    disconnect,
    error: websocketError,
    finalTranscripts,
    liveTranscript,
    llmStatus,
    messages,
    sendAudio,
    sendControl,
    sendMessage,
    status,
    sttStatus,
    turnEvent,
  } = useWebSocket();
  const {
    error: microphoneError,
    start: startMicrophone,
    status: microphoneStatus,
    stop: stopMicrophone,
  } = useMicrophone({ sendAudio, sendControl });

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
  const isRecording = microphoneStatus === "recording";

  useEffect(() => {
    if (!isConnected && isRecording) stopMicrophone();
  }, [isConnected, isRecording, stopMicrophone]);

  function handleDisconnect() {
    stopMicrophone();
    disconnect();
  }

  return (
    <main className="page-shell">
      <section className="agent-card">
        <p className="eyebrow">Courier delivery assistant</p>
        <h1>CallSaul</h1>
        <p className="description">
          Speak or type a message and receive a response from OpenAI.
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
            <div className="audio-panel">
              <div>
                <span>Microphone</span>
                <strong>{microphoneStatus.replaceAll("_", " ")}</strong>
              </div>
              <div className="audio-stats">
                <span>{audioStats.chunks} chunks</span>
                <span>{audioStats.bytes.toLocaleString()} bytes received</span>
              </div>
              <div className="audio-stats">
                <span>Deepgram: {sttStatus}</span>
                <span>Turn: {turnEvent}</span>
              </div>
              <div className="audio-stats">
                <span>OpenAI: {llmStatus}</span>
              </div>
              <button
                className={isRecording ? "stop-recording-button" : "recording-button"}
                type="button"
                onClick={isRecording ? stopMicrophone : startMicrophone}
                disabled={microphoneStatus === "requesting_permission"}
              >
                {isRecording ? "Stop microphone" : "Start microphone"}
              </button>
              <p>Audio is sent to Deepgram for live transcription and is not stored.</p>
            </div>

            <div className="transcript-panel" aria-live="polite">
              <span>Live transcript</span>
              {finalTranscripts.map((transcript) => (
                <p className="final-transcript" key={transcript.id}>
                  {transcript.text}
                </p>
              ))}
              {liveTranscript && <p className="live-transcript">{liveTranscript}</p>}
              {!liveTranscript && finalTranscripts.length === 0 && (
                <p className="transcript-placeholder">
                  Start the microphone and speak to see a transcript.
                </p>
              )}
            </div>

            <div className="conversation" aria-live="polite">
              {messages.map((message) => (
                <div className={`message ${message.role}`} key={message.id}>
                  <span>
                    {message.role === "server"
                      ? "Server"
                      : message.role === "assistant"
                        ? "Assistant"
                        : "You"}
                  </span>
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

            <button
              className="secondary-button"
              type="button"
              onClick={handleDisconnect}
              disabled={isRecording}
            >
              Disconnect
            </button>
          </>
        )}

        {(websocketError || microphoneError) && (
          <p className="error-message">{websocketError || microphoneError}</p>
        )}
      </section>
    </main>
  );
}
