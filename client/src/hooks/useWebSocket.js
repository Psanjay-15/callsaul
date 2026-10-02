import { useCallback, useEffect, useRef, useState } from "react";


const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";


function getWebSocketUrl() {
  const url = new URL(API_BASE_URL);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = "/ws";
  url.search = "";
  return url.toString();
}


export function useWebSocket({ onAudio, onTtsStart } = {}) {
  const [status, setStatus] = useState("disconnected");
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
  }, []);

  const connect = useCallback(() => {
    if (socketRef.current) return;

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

    const socket = new WebSocket(getWebSocketUrl());
    socket.binaryType = "arraybuffer";
    socketRef.current = socket;

    socket.onopen = () => setStatus("connected");

    socket.onmessage = (event) => {
      if (event.data instanceof ArrayBuffer) {
        onAudio?.(event.data);
        return;
      }

      const payload = JSON.parse(event.data);

      if (payload.type === "connected" || payload.type === "message") {
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

    socket.onerror = () => setError("Could not connect to the WebSocket server.");

    socket.onclose = () => {
      socketRef.current = null;
      setStatus("disconnected");
      setSttStatus("disconnected");
      setTtsStatus("disconnected");
      setBackendStatus("idle");
      setConversationStage("not_started");
    };
  }, [onAudio, onTtsStart]);

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
    status,
    sttStatus,
    ttsStatus,
    turnEvent,
  };
}
