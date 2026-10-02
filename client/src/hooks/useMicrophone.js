import { useCallback, useEffect, useRef, useState } from "react";


const AUDIO_CHUNK_MS = 100;
const MIME_TYPES = [
  "audio/webm;codecs=opus",
  "audio/webm",
  "audio/mp4",
];


function supportedMimeType() {
  return MIME_TYPES.find((type) => MediaRecorder.isTypeSupported(type));
}


export function useMicrophone({ sendAudio, sendControl }) {
  const [status, setStatus] = useState("stopped");
  const [error, setError] = useState("");
  const recorderRef = useRef(null);
  const streamRef = useRef(null);

  const stop = useCallback(() => {
    const recorder = recorderRef.current;
    const stream = streamRef.current;

    if (recorder && recorder.state !== "inactive") {
      recorder.stop();
      return;
    }

    recorderRef.current = null;
    streamRef.current = null;
    stream?.getTracks().forEach((track) => track.stop());
    setStatus("stopped");
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
      const recorder = new MediaRecorder(
        stream,
        mimeType ? { mimeType } : undefined,
      );

      streamRef.current = stream;
      recorderRef.current = recorder;

      recorder.ondataavailable = async (event) => {
        if (event.data.size > 0) sendAudio(await event.data.arrayBuffer());
      };

      recorder.onstop = () => {
        stream.getTracks().forEach((track) => track.stop());
        recorderRef.current = null;
        streamRef.current = null;
        sendControl("audio_stop");
        setStatus("stopped");
      };

      recorder.onerror = () => {
        setError("The browser could not record microphone audio.");
        stop();
      };

      sendControl("audio_start", { mime_type: recorder.mimeType || "unknown" });
      recorder.start(AUDIO_CHUNK_MS);
      setStatus("recording");
    } catch (microphoneError) {
      setStatus("stopped");
      setError(microphoneError?.message || "Microphone permission was not granted.");
    }
  }, [sendAudio, sendControl, stop]);

  useEffect(() => stop, [stop]);

  return { error, start, status, stop };
}
