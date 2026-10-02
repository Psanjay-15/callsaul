import { useCallback, useEffect, useState } from "react";

import { useAudioPlayer } from "./hooks/useAudioPlayer.js";
import { useMicrophone } from "./hooks/useMicrophone.js";
import { useWebSocket } from "./hooks/useWebSocket.js";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";


function clearSavedSessionReference() {
  localStorage.removeItem("callsaul_session_id");
  const pageUrl = new URL(window.location.href);
  if (!pageUrl.searchParams.has("session_id")) return;

  pageUrl.searchParams.delete("session_id");
  window.history.replaceState({}, "", pageUrl);
}


function formatActivity(value) {
  if (!value) return "New chat";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}


export default function App() {
  const [input, setInput] = useState("");
  const [history, setHistory] = useState([]);
  const [historyError, setHistoryError] = useState("");
  const [deletingSessionId, setDeletingSessionId] = useState("");
  const loadHistory = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/history`);
      if (!response.ok) throw new Error("History request failed");
      const data = await response.json();
      setHistory(data.sessions);
      setHistoryError("");
    } catch {
      setHistoryError("Could not load chat history.");
    }
  }, []);
  const {
    appendAudio,
    prepare: prepareAudio,
    setSampleRate,
    status: playbackStatus,
    stop: stopAudio,
  } = useAudioPlayer();
  const {
    audioStats,
    backendStatus,
    connect,
    conversationStage,
    disconnect,
    error: websocketError,
    finalTranscripts,
    liveTranscript,
    llmStatus,
    messages,
    sendAudio,
    sendControl,
    sendMessage,
    sessionId,
    sessionResumed,
    status,
    sttStatus,
    ttsStatus,
    turnEvent,
  } = useWebSocket({
    onAudio: appendAudio,
    onBargeIn: stopAudio,
    onConversationUpdated: loadHistory,
    onTtsStart: setSampleRate,
  });
  const {
    error: microphoneError,
    start: startMicrophone,
    status: microphoneStatus,
    stop: stopMicrophone,
  } = useMicrophone({ sendAudio, sendControl });

  useEffect(() => {
    async function loadPage() {
      clearSavedSessionReference();
      await loadHistory();
    }

    loadPage();
  }, [loadHistory]);

  const isConnected = status === "connected";
  const isRecording = microphoneStatus === "recording";
  const activeSession = history.find(
    (session) => session.session_id === sessionId,
  );

  useEffect(() => {
    if (!isConnected && isRecording) stopMicrophone();
  }, [isConnected, isRecording, stopMicrophone]);

  function handleSubmit(event) {
    event.preventDefault();
    if (sendMessage(input)) setInput("");
  }

  function stopCurrentChat() {
    stopMicrophone();
    stopAudio();
    disconnect();
  }

  async function openChat(targetSessionId) {
    stopCurrentChat();
    await prepareAudio();
    const connected = await connect(targetSessionId);
    if (connected) await startMicrophone();
  }

  async function startNewChat() {
    clearSavedSessionReference();
    stopCurrentChat();
    await prepareAudio();
    const connected = await connect("");
    if (connected) await startMicrophone();
  }

  async function deleteChat(session) {
    const confirmed = window.confirm(
      `Delete "${session.title}" and its conversation history?`,
    );
    if (!confirmed) return;

    const isActiveChat = session.session_id === sessionId;
    if (isActiveChat) {
      clearSavedSessionReference();
      stopCurrentChat();
    }

    setDeletingSessionId(session.session_id);
    setHistoryError("");

    try {
      const response = await fetch(
        `${API_BASE_URL}/api/history/${encodeURIComponent(session.session_id)}`,
        { method: "DELETE" },
      );
      if (!response.ok) throw new Error("Delete request failed");
      setHistory((current) =>
        current.filter((item) => item.session_id !== session.session_id),
      );
    } catch {
      await loadHistory();
      setHistoryError("Could not delete this conversation.");
    } finally {
      setDeletingSessionId("");
    }
  }

  return (
    <main className="page-shell">
      <section className="chat-layout">
        <aside className="history-sidebar">
          <div className="brand-block">
            <p className="eyebrow">Courier assistant</p>
            <h1>CallSaul</h1>
          </div>

          <button className="new-chat-button" type="button" onClick={startNewChat}>
            <span aria-hidden="true">+</span>
            New chat
          </button>

          <div className="history-heading">
            <span>History</span>
            <button type="button" onClick={loadHistory} aria-label="Refresh history">
              Refresh
            </button>
          </div>

          <nav className="history-list" aria-label="Chat history">
            {history.map((session) => (
              <div
                className={`history-item-row ${
                  session.session_id === sessionId ? "active" : ""
                }`}
                key={session.session_id}
              >
                <button
                  className="history-item"
                  type="button"
                  onClick={() => openChat(session.session_id)}
                >
                  <strong>{session.title}</strong>
                  <span>{formatActivity(session.last_message_at)}</span>
                  <small>{session.stage.replaceAll("_", " ")}</small>
                </button>
                <button
                  className="delete-history-button"
                  type="button"
                  aria-label={`Delete ${session.title}`}
                  title="Delete conversation"
                  disabled={deletingSessionId === session.session_id}
                  onClick={() => deleteChat(session)}
                >
                  {deletingSessionId === session.session_id ? "Deleting…" : "Delete"}
                </button>
              </div>
            ))}
            {history.length === 0 && !historyError && (
              <p className="empty-history">No conversations yet.</p>
            )}
            {historyError && <p className="history-error">{historyError}</p>}
          </nav>

        </aside>

        <section className="agent-card">
          <header className="chat-header">
            <div>
              <p className="eyebrow">Delivery rescheduling</p>
              <h2>{activeSession?.title || "Voice delivery assistant"}</h2>
            </div>
            <div className="connection-status">
              <span className={`connection-dot ${status}`} aria-hidden="true" />
              {status}
            </div>
          </header>

          {!isConnected ? (
            <div className="empty-chat">
              <div className="empty-chat-icon" aria-hidden="true">CS</div>
              <h3>Choose a conversation</h3>
              <p>
                Select a chat from the history or start a new delivery conversation.
              </p>
              <button
                className="secondary-button compact"
                type="button"
                onClick={startNewChat}
              >
                Start new chat
              </button>
            </div>
          ) : (
            <>
              <div className="session-strip">
                <span>{sessionResumed ? "Resumed session" : "New session"}</span>
                <code>{sessionId}</code>
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

              <div className="live-voice-bar">
                <div>
                  <span>Live transcript</span>
                  <p>
                    {liveTranscript ||
                      finalTranscripts.at(-1)?.text ||
                      "Start the microphone and speak."}
                  </p>
                </div>
                <button
                  className={isRecording ? "stop-recording-button" : "recording-button"}
                  type="button"
                  onClick={isRecording ? stopCurrentChat : startMicrophone}
                  disabled={microphoneStatus === "requesting_permission"}
                >
                  {isRecording ? "Listening · Stop" : "Start listening"}
                </button>
              </div>

              <form className="message-form" onSubmit={handleSubmit}>
                <div className="message-controls">
                  <input
                    id="message"
                    aria-label="Your message"
                    maxLength={300}
                    onChange={(event) => setInput(event.target.value)}
                    placeholder="Type a message..."
                    value={input}
                  />
                  <button className="send-button" type="submit" disabled={!input.trim()}>
                    Send
                  </button>
                </div>
              </form>

              <details className="technical-status">
                <summary>Connection details</summary>
                <div className="technical-grid">
                  <span>Microphone: {microphoneStatus.replaceAll("_", " ")}</span>
                  <span>STT: {sttStatus}</span>
                  <span>Turn: {turnEvent}</span>
                  <span>OpenAI: {llmStatus}</span>
                  <span>TTS: {ttsStatus}</span>
                  <span>Playback: {playbackStatus}</span>
                  <span>Stage: {conversationStage.replaceAll("_", " ")}</span>
                  <span>Backend: {backendStatus}</span>
                  <span>{audioStats.chunks} audio chunks</span>
                </div>
              </details>

              <button
                className="disconnect-button"
                type="button"
                onClick={stopCurrentChat}
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
      </section>
    </main>
  );
}
