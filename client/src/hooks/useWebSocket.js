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

  useEffect(() => disconnect, [disconnect]);

  return { connect, disconnect, error, messages, sendMessage, status };
}

