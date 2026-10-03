import { useCallback, useEffect, useRef, useState } from "react";


// Keep a little audio queued so normal network jitter does not create audible
// gaps. The delay is only added when playback starts or the queue runs empty.
const PLAYBACK_BUFFER_SECONDS = 0.12;


export function useAudioPlayer() {
  const [status, setStatus] = useState("idle");
  const audioContextRef = useRef(null);
  const nextStartTimeRef = useRef(0);
  const sampleRateRef = useRef(24000);
  const sourcesRef = useRef(new Set());
  const acceptAudioRef = useRef(true);

  const prepare = useCallback(async () => {
    if (!audioContextRef.current) {
      audioContextRef.current = new AudioContext();
    }

    if (audioContextRef.current.state === "suspended") {
      await audioContextRef.current.resume();
    }

    setStatus("ready");
  }, []);

  const setSampleRate = useCallback((sampleRate) => {
    acceptAudioRef.current = true;
    if (Number.isFinite(sampleRate) && sampleRate > 0) {
      sampleRateRef.current = sampleRate;
    }
  }, []);

  const appendAudio = useCallback((arrayBuffer) => {
    const audioContext = audioContextRef.current;
    if (
      !acceptAudioRef.current ||
      !audioContext ||
      arrayBuffer.byteLength < 2
    ) return;

    const sampleCount = Math.floor(arrayBuffer.byteLength / 2);
    const pcm = new Int16Array(arrayBuffer, 0, sampleCount);
    const audioBuffer = audioContext.createBuffer(
      1,
      sampleCount,
      sampleRateRef.current,
    );
    const channel = audioBuffer.getChannelData(0);

    for (let index = 0; index < sampleCount; index += 1) {
      channel[index] = pcm[index] / 32768;
    }

    const source = audioContext.createBufferSource();
    source.buffer = audioBuffer;
    source.connect(audioContext.destination);

    const queueHasRunEmpty = nextStartTimeRef.current <= audioContext.currentTime;
    const startTime = queueHasRunEmpty
      ? audioContext.currentTime + PLAYBACK_BUFFER_SECONDS
      : nextStartTimeRef.current;
    source.start(startTime);
    nextStartTimeRef.current = startTime + audioBuffer.duration;
    sourcesRef.current.add(source);
    setStatus("playing");

    source.onended = () => {
      sourcesRef.current.delete(source);
      if (sourcesRef.current.size === 0) setStatus("ready");
    };
  }, []);

  const stop = useCallback(() => {
    acceptAudioRef.current = false;
    for (const source of sourcesRef.current) {
      try {
        source.stop();
      } catch {
        // The source may already have finished playing.
      }
    }

    sourcesRef.current.clear();
    nextStartTimeRef.current = 0;
    setStatus(audioContextRef.current ? "ready" : "idle");
  }, []);

  useEffect(() => {
    const sources = sourcesRef.current;

    return () => {
      for (const source of sources) {
        try {
          source.stop();
        } catch {
          // The source may already have finished playing.
        }
      }
      audioContextRef.current?.close();
    };
  }, []);

  return { appendAudio, prepare, setSampleRate, status, stop };
}
