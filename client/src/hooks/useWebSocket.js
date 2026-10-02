import { useCallback, useEffect, useRef, useState } from "react";


const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";


function getWebSocketUrl() {
  const url = new URL(API_BASE_URL);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = "/ws";
  url.search = "";
  return url.toString();
}


export function useWebSocket() {
  const [status, setStatus] = useState("disconnected");
  const [messages, setMessages] = useState([]);
  const [audioStats, setAudioStats] = useState({ chunks: 0, bytes: 0 });
  const [error, setError] = useState("");
  const socketRef = useRef(null);

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
    setStatus("connecting");

    const socket = new WebSocket(getWebSocketUrl());
    socketRef.current = socket;

    socket.onopen = () => setStatus("connected");

    socket.onmessage = (event) => {
      const payload = JSON.parse(event.data);

      if (payload.type === "connected" || payload.type === "message") {
        setMessages((current) => [
          ...current,
          { id: crypto.randomUUID(), role: "server", text: payload.message },
        ]);
      }

      if (payload.type === "error") setError(payload.message);

      if (payload.type === "audio_received" || payload.type === "audio_stopped") {
        setAudioStats({ chunks: payload.chunks, bytes: payload.bytes });
      }
    };

    socket.onerror = () => setError("Could not connect to the WebSocket server.");

    socket.onclose = () => {
      socketRef.current = null;
      setStatus("disconnected");
    };
  }, []);

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
    connect,
    disconnect,
    error,
    messages,
    sendAudio,
    sendControl,
    sendMessage,
    status,
  };
}
