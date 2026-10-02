import { useCallback, useEffect, useRef, useState } from "react";


const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";


function getWebSocketUrl(sessionId) {
  const url = new URL(API_BASE_URL);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = "/ws";
  url.search = "";
  if (sessionId) url.searchParams.set("session_id", sessionId);
  return url.toString();
}


export function useWebSocket({
  onAudio,
  onBargeIn,
  onConversationUpdated,
  onTtsStart,
} = {}) {
  const [status, setStatus] = useState("disconnected");
  const [sessionId, setSessionId] = useState("");
  const [sessionResumed, setSessionResumed] = useState(false);
  const [messages, setMessages] = useState([]);
  const [audioStats, setAudioStats] = useState({ chunks: 0, bytes: 0 });
  const [finalTranscripts, setFinalTranscripts] = useState([]);
  const [liveTranscript, setLiveTranscript] = useState("");
  const [sttStatus, setSttStatus] = useState("disconnected");
  const [llmStatus, setLlmStatus] = useState("idle");
  const [ttsStatus, setTtsStatus] = useState("idle");
  const [backendStatus, setBackendStatus] = useState("idle");
  const [conversationStage, setConversationStage] = useState("not_started");
  const [turnEvent, setTurnEvent] = useState("idle");
  const [error, setError] = useState("");
  const socketRef = useRef(null);
  const assistantMessageIdRef = useRef(null);

  const disconnect = useCallback(() => {
    const socket = socketRef.current;
    socketRef.current = null;

    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: "stop" }));
    } else {
      socket?.close();
    }

    setStatus("disconnected");
    setSttStatus("disconnected");
    setTtsStatus("disconnected");
    setBackendStatus("idle");
    setConversationStage("not_started");
  }, []);

  const connect = useCallback((requestedSessionId = "") => {
    if (socketRef.current) return Promise.resolve(false);

    setError("");
    setMessages([]);
    setAudioStats({ chunks: 0, bytes: 0 });
    setFinalTranscripts([]);
    setLiveTranscript("");
    setSttStatus("disconnected");
    setLlmStatus("idle");
    setTtsStatus("idle");
    setBackendStatus("idle");
    setConversationStage("not_started");
    setTurnEvent("idle");
    assistantMessageIdRef.current = null;
    setStatus("connecting");

    const socket = new WebSocket(getWebSocketUrl(requestedSessionId));
    socket.binaryType = "arraybuffer";
    socketRef.current = socket;

    return new Promise((resolve) => {
      let connectionSettled = false;
      const settleConnection = (connected) => {
        if (connectionSettled) return;
        connectionSettled = true;
        resolve(connected);
      };

      socket.onopen = () => {
        setStatus("connected");
        settleConnection(true);
      };

      socket.onmessage = (event) => {
      if (event.data instanceof ArrayBuffer) {
        onAudio?.(event.data);
        return;
      }

      const payload = JSON.parse(event.data);

      if (payload.type === "session") {
        setSessionId(payload.session_id);
        setSessionResumed(payload.resumed);
        onConversationUpdated?.();
      }

      if (payload.type === "conversation_history") {
        setMessages(payload.messages);
      }

      if (payload.type === "message") {
        setMessages((current) => [
          ...current,
          { id: crypto.randomUUID(), role: "server", text: payload.message },
        ]);
      }

      if (payload.type === "conversation_state") {
        setConversationStage(payload.stage);
      }

      if (payload.type === "backend_status") {
        setBackendStatus(payload.status);
      }

      if (payload.type === "barge_in") {
        onBargeIn?.();
      }

      if (payload.type === "llm_response") {
        setMessages((current) => [
          ...current,
          { id: crypto.randomUUID(), role: "assistant", text: payload.message },
        ]);
      }

      if (payload.type === "llm_start") {
        const messageId = crypto.randomUUID();
        assistantMessageIdRef.current = messageId;
        setMessages((current) => [
          ...current,
          { id: messageId, role: "assistant", text: "" },
        ]);
      }

      if (payload.type === "llm_delta") {
        const messageId = assistantMessageIdRef.current;
        if (messageId) {
          setMessages((current) =>
            current.map((message) =>
              message.id === messageId
                ? { ...message, text: message.text + payload.delta }
                : message,
            ),
          );
        }
      }

      if (payload.type === "llm_done") {
        assistantMessageIdRef.current = null;
        onConversationUpdated?.();
      }

      if (payload.type === "llm_status") setLlmStatus(payload.status);

      if (payload.type === "llm_error") {
        setError(payload.message);
        setLlmStatus("error");
      }

      if (payload.type === "tts_audio_start") {
        onTtsStart?.(payload.sample_rate);
      }

      if (payload.type === "tts_status") setTtsStatus(payload.status);

      if (payload.type === "tts_error") {
        setError(payload.message);
        setTtsStatus("error");
      }

      if (payload.type === "error") setError(payload.message);

      if (payload.type === "stt_error") {
        setError(payload.message);
        setSttStatus("error");
      }

      if (payload.type === "stt_status") setSttStatus(payload.status);

      if (payload.type === "transcript") {
        setTurnEvent(payload.event);

        if (payload.is_final) {
          if (payload.text.trim()) {
            setFinalTranscripts((current) => [
              ...current,
              { id: crypto.randomUUID(), text: payload.text.trim() },
            ]);
            setMessages((current) => [
              ...current,
              { id: crypto.randomUUID(), role: "user", text: payload.text.trim() },
            ]);
          }
          setLiveTranscript("");
        } else {
          setLiveTranscript(payload.text);
        }
      }

      if (payload.type === "audio_received" || payload.type === "audio_stopped") {
        setAudioStats({ chunks: payload.chunks, bytes: payload.bytes });
      }
      };

      socket.onerror = () => {
        setError("Could not connect to the WebSocket server.");
        settleConnection(false);
      };

      socket.onclose = () => {
        settleConnection(false);
        if (socketRef.current !== socket) return;
        socketRef.current = null;
        setStatus("disconnected");
        setSttStatus("disconnected");
        setTtsStatus("disconnected");
        setBackendStatus("idle");
        setConversationStage("not_started");
      };
    });
  }, [onAudio, onBargeIn, onConversationUpdated, onTtsStart]);

  const sendMessage = useCallback((text) => {
    const cleanText = text.trim();
    const socket = socketRef.current;
    if (!cleanText || socket?.readyState !== WebSocket.OPEN) return false;

    setMessages((current) => [
      ...current,
      { id: crypto.randomUUID(), role: "user", text: cleanText },
    ]);
    socket.send(JSON.stringify({ type: "message", text: cleanText }));
    return true;
  }, []);

  const sendControl = useCallback((type, details = {}) => {
    const socket = socketRef.current;
    if (socket?.readyState !== WebSocket.OPEN) return false;

    socket.send(JSON.stringify({ type, ...details }));
    return true;
  }, []);

  const sendAudio = useCallback((audio) => {
    const socket = socketRef.current;
    if (socket?.readyState !== WebSocket.OPEN) return false;

    socket.send(audio);
    return true;
  }, []);

  useEffect(() => disconnect, [disconnect]);

  return {
    audioStats,
    backendStatus,
    connect,
    conversationStage,
    disconnect,
    error,
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
  };
}
