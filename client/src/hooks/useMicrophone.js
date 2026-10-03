import { useCallback, useEffect, useRef, useState } from "react";


const AUDIO_CHUNK_MS = 80;
const MIME_TYPES = [
  "audio/webm;codecs=opus",
  "audio/webm",
];


function supportedMimeType() {
  return MIME_TYPES.find((type) => MediaRecorder.isTypeSupported(type));
}


export function useMicrophone({ sendAudio, sendControl }) {
  const [status, setStatus] = useState("stopped");
  const [error, setError] = useState("");
  const recorderRef = useRef(null);
  const streamRef = useRef(null);
  const stopPromiseRef = useRef(null);
  const stopResolverRef = useRef(null);
  const recordingGenerationRef = useRef(0);

  const stop = useCallback(() => {
    const recorder = recorderRef.current;
    const stream = streamRef.current;

    if (recorder && recorder.state !== "inactive") {
      if (stopPromiseRef.current) return stopPromiseRef.current;

      recordingGenerationRef.current += 1;
      setStatus("stopped");
      stopPromiseRef.current = new Promise((resolve) => {
        stopResolverRef.current = resolve;
      });
      try {
        recorder.stop();
      } catch {
        stopResolverRef.current?.();
        stopResolverRef.current = null;
        stopPromiseRef.current = null;
      }
      return stopPromiseRef.current || Promise.resolve();
    }

    recordingGenerationRef.current += 1;
    recorderRef.current = null;
    streamRef.current = null;
    stream?.getTracks().forEach((track) => track.stop());
    setStatus("stopped");
    return Promise.resolve();
  }, []);

  const start = useCallback(async () => {
    if (recorderRef.current) return;

    setError("");
    setStatus("requesting_permission");

    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        },
      });
      const mimeType = supportedMimeType();
      if (!mimeType) {
        stream.getTracks().forEach((track) => track.stop());
        throw new Error("This browser cannot record WebM/Opus audio. Please use Chrome.");
      }
      const recorder = new MediaRecorder(
        stream,
        { mimeType },
      );

      streamRef.current = stream;
      recorderRef.current = recorder;
      const recordingGeneration = recordingGenerationRef.current + 1;
      recordingGenerationRef.current = recordingGeneration;

      recorder.ondataavailable = async (event) => {
        if (event.data.size === 0) return;
        const audio = await event.data.arrayBuffer();
        if (recordingGenerationRef.current === recordingGeneration) {
          sendAudio(audio);
        }
      };

      recorder.onstop = () => {
        stream.getTracks().forEach((track) => track.stop());
        recorderRef.current = null;
        streamRef.current = null;
        sendControl("audio_stop");
        setStatus("stopped");
        stopResolverRef.current?.();
        stopResolverRef.current = null;
        stopPromiseRef.current = null;
      };

      recorder.onerror = () => {
        setError("The browser could not record microphone audio.");
        void stop();
      };

      sendControl("audio_start", { mime_type: recorder.mimeType || "unknown" });
      recorder.start(AUDIO_CHUNK_MS);
      setStatus("recording");
    } catch (microphoneError) {
      setStatus("stopped");
      setError(microphoneError?.message || "Microphone permission was not granted.");
    }
  }, [sendAudio, sendControl, stop]);

  useEffect(() => () => {
    void stop();
  }, [stop]);

  return { error, start, status, stop };
}
