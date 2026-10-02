import asyncio
import json
from uuid import uuid4
from contextlib import suppress

from deepgram.core.events import EventType
from fastapi import WebSocket, WebSocketDisconnect

from app.clients.deepgram import create_deepgram_client
from app.config.settings import settings
from app.conversation import ConversationStage, ConversationState
from app.services.llm import LLMService
from app.services.slots import book_delivery_slot, get_available_slots
from app.services.tracking import (
    find_delivery,
    is_valid_tracking_id,
    normalize_tracking_id,
    tracking_id_for_speech,
)
from app.services.tts import TTSService


class TranscriptionService:
    def __init__(self, websocket: WebSocket) -> None:
        self.websocket = websocket
        self.send_lock = asyncio.Lock()
        self.conversation_lock = asyncio.Lock()
        self.user_tasks: set[asyncio.Task] = set()
        self.response_generation = 0
        self.state = ConversationState()
        self.llm = LLMService()
        self.tts = TTSService(self.send_audio, self.send)
        self.connection_context = None
        self.connection = None
        self.listener_task: asyncio.Task | None = None
        self.audio_active = False
        self.audio_chunks = 0
        self.audio_bytes = 0

    async def run(self) -> None:
        await self.send(
            {
                "type": "connected",
                "message": "WebSocket connection established.",
            }
        )
        await self.send_conversation_state()
        await self.send_assistant_message(
            "Hello. I can help reschedule your delivery. "
            "Please say your tracking ID. It has two letters followed by six digits."
        )

        try:
            while True:
                event = await self.websocket.receive()
                if event.get("type") == "websocket.disconnect":
                    break

                audio = event.get("bytes")
                if audio is not None:
                    await self.forward_audio(audio)
                    continue

                text = event.get("text")
                if text is not None and await self.handle_control(text):
                    break
        except WebSocketDisconnect:
            pass
        finally:
            for task in self.user_tasks:
                task.cancel()
            if self.user_tasks:
                await asyncio.gather(*self.user_tasks, return_exceptions=True)
            await self.close_deepgram_stream()
            await self.tts.close()

    async def handle_control(self, raw_message: str) -> bool:
        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError:
            await self.send(
                {"type": "error", "message": "WebSocket message must be valid JSON."}
            )
            return False

        message_type = message.get("type")

        if message_type == "audio_start":
            await self.start_deepgram_stream(message.get("mime_type", "unknown"))
        elif message_type == "audio_stop":
            await self.stop_audio()
        elif message_type == "ping":
            await self.send({"type": "pong"})
        elif message_type == "message":
            await self.handle_user_input(message.get("text"))
        elif message_type == "stop":
            await self.websocket.close(code=1000)
            return True
        else:
            await self.send(
                {"type": "error", "message": "Unsupported WebSocket message type."}
            )

        return False

    async def start_deepgram_stream(self, mime_type: str) -> None:
        await self.close_deepgram_stream()
        self.audio_active = True
        self.audio_chunks = 0
        self.audio_bytes = 0

        try:
            client = create_deepgram_client()
            self.connection_context = client.listen.v2.connect(
                model=settings.deepgram_stt_model,
                numerals="true",
            )
            self.connection = await self.connection_context.__aenter__()
            self.connection.on(EventType.MESSAGE, self.handle_deepgram_message)
            self.connection.on(EventType.ERROR, self.handle_deepgram_error)
            self.listener_task = asyncio.create_task(
                self.connection.start_listening()
            )

            await self.send({"type": "stt_status", "status": "connected"})
            await self.send({"type": "audio_started", "mime_type": mime_type})
        except Exception as error:
            await self.send(
                {
                    "type": "stt_error",
                    "message": f"Could not connect to Deepgram: {error}",
                }
            )
            await self.close_deepgram_stream()

    async def forward_audio(self, audio: bytes) -> None:
        if not self.audio_active:
            await self.send(
                {
                    "type": "error",
                    "message": "Start the microphone before sending audio.",
                }
            )
            return

        self.audio_chunks += 1
        self.audio_bytes += len(audio)

        if self.connection is not None:
            try:
                await self.connection.send_media(audio)
            except Exception as error:
                await self.send(
                    {
                        "type": "stt_error",
                        "message": f"Could not send audio to Deepgram: {error}",
                    }
                )
                await self.close_deepgram_stream()

        if self.audio_chunks == 1 or self.audio_chunks % 10 == 0:
            await self.send(
                {
                    "type": "audio_received",
                    "chunks": self.audio_chunks,
                    "bytes": self.audio_bytes,
                }
            )

    async def handle_deepgram_message(self, message) -> None:
        if getattr(message, "type", None) != "TurnInfo":
            return

        turn_event = str(message.event)

        if turn_event == "StartOfTurn":
            await self.handle_barge_in()

        await self.send(
            {
                "type": "transcript",
                "event": turn_event,
                "text": message.transcript,
                "is_final": turn_event == "EndOfTurn",
                "turn_index": message.turn_index,
            }
        )

        if turn_event == "EndOfTurn" and message.transcript.strip():
            task = asyncio.create_task(self.handle_user_input(message.transcript))
            self.user_tasks.add(task)
            task.add_done_callback(self.user_tasks.discard)

    async def handle_barge_in(self) -> None:
        self.response_generation += 1
        await self.send({"type": "barge_in"})

        try:
            await self.tts.interrupt()
        except Exception as error:
            await self.send(
                {
                    "type": "tts_error",
                    "message": f"Could not interrupt Deepgram speech: {error}",
                }
            )

    async def handle_deepgram_error(self, error) -> None:
        await self.send(
            {
                "type": "stt_error",
                "message": f"Deepgram transcription failed: {error}",
            }
        )

    async def stop_audio(self) -> None:
        self.audio_active = False
        await self.close_deepgram_stream()
        await self.send(
            {
                "type": "audio_stopped",
                "chunks": self.audio_chunks,
                "bytes": self.audio_bytes,
            }
        )

    async def close_deepgram_stream(self) -> None:
        connection = self.connection
        listener_task = self.listener_task
        connection_context = self.connection_context

        self.connection = None
        self.listener_task = None
        self.connection_context = None

        if connection is not None:
            with suppress(Exception):
                await connection.send_close_stream()

        if listener_task is not None:
            try:
                await asyncio.wait_for(listener_task, timeout=2)
            except TimeoutError:
                listener_task.cancel()
                with suppress(asyncio.CancelledError):
                    await listener_task

        if connection_context is not None:
            with suppress(Exception):
                await connection_context.__aexit__(None, None, None)

    async def send_to_llm(self, text) -> None:
        if not isinstance(text, str) or not text.strip():
            await self.send({"type": "error", "message": "Message cannot be empty."})
            return

        await self.send({"type": "llm_status", "status": "streaming"})
        await self.send({"type": "llm_start"})
        self.response_generation += 1
        response_generation = self.response_generation

        tts_ready = False
        try:
            await self.tts.connect()
            tts_ready = True
            await self.send(
                {
                    "type": "tts_audio_start",
                    "sample_rate": settings.deepgram_tts_sample_rate,
                }
            )
        except Exception as error:
            await self.send(
                {
                    "type": "tts_error",
                    "message": f"Could not connect to Deepgram TTS: {error}",
                }
            )

        full_response = ""
        interrupted = False
        try:
            async for delta in self.llm.stream_response(text.strip()):
                if response_generation != self.response_generation:
                    interrupted = True
                    break

                full_response += delta
                await self.send({"type": "llm_delta", "delta": delta})

                if tts_ready:
                    try:
                        await self.tts.send_text(delta)
                    except Exception as error:
                        tts_ready = False
                        await self.send(
                            {
                                "type": "tts_error",
                                "message": f"Could not stream text to Deepgram TTS: {error}",
                            }
                        )

            if interrupted:
                await self.send(
                    {"type": "llm_done", "message": full_response.strip()}
                )
                return

            if tts_ready:
                try:
                    await self.tts.flush()
                except Exception as error:
                    await self.send(
                        {
                            "type": "tts_error",
                            "message": f"Could not finish Deepgram speech: {error}",
                        }
                    )

            await self.send(
                {"type": "llm_done", "message": full_response.strip()}
            )
        except Exception as error:
            await self.send(
                {
                    "type": "llm_error",
                    "message": f"Could not get an OpenAI response: {error}",
                }
            )
        finally:
            await self.send({"type": "llm_status", "status": "idle"})

    async def handle_user_input(self, text) -> None:
        if not isinstance(text, str) or not text.strip():
            await self.send({"type": "error", "message": "Message cannot be empty."})
            return

        async with self.conversation_lock:
            if self.state.stage == ConversationStage.COLLECTING_TRACKING_ID:
                await self.collect_tracking_id(text.strip())
            elif self.state.stage == ConversationStage.CONFIRMING_TRACKING_ID:
                await self.confirm_tracking_id(text.strip())
            elif self.state.stage == ConversationStage.OFFERING_SLOTS:
                await self.select_slot(text.strip())
            elif self.state.stage == ConversationStage.CONFIRMING_SLOT:
                await self.confirm_slot(text.strip())
            else:
                await self.send_to_llm(text.strip())

    async def collect_tracking_id(self, text: str) -> None:
        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            decision = await self.llm.understand_tracking_id(
                text,
                self.state.tracking_id,
            )
        except Exception as error:
            await self.send(
                {
                    "type": "llm_error",
                    "message": f"Could not understand the tracking ID: {error}",
                }
            )
            return
        finally:
            await self.send({"type": "llm_status", "status": "idle"})

        if decision["tool"] != "capture_tracking_id":
            message = decision["message"] or (
                "Please say your tracking ID. It has two letters followed by six digits."
            )
            await self.send_assistant_message(message)
            return

        await self.check_tracking_id(decision["arguments"].get("tracking_id", ""))

    async def confirm_tracking_id(self, text: str) -> None:
        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            decision = await self.llm.understand_tracking_confirmation(
                text,
                self.state.tracking_id,
            )
        except Exception as error:
            await self.send(
                {
                    "type": "llm_error",
                    "message": f"Could not understand the confirmation: {error}",
                }
            )
            return
        finally:
            await self.send({"type": "llm_status", "status": "idle"})

        if decision["tool"] == "confirm_tracking_id":
            self.state.tracking_id_confirmed = True
            self.state.stage = ConversationStage.OFFERING_SLOTS
            await self.send_conversation_state()
            await self.offer_available_slots()
            return

        if decision["tool"] == "replace_tracking_id":
            await self.check_tracking_id(
                decision["arguments"].get("tracking_id", "")
            )
            return

        if decision["tool"] == "reject_tracking_id":
            self.state.tracking_id = None
            self.state.tracking_id_confirmed = False
            self.state.stage = ConversationStage.COLLECTING_TRACKING_ID
            await self.send_conversation_state()
            await self.send_assistant_message(
                "No problem. Please say the complete tracking ID again."
            )
            return

        message = decision["message"] or (
            "Please say yes if the tracking ID is correct, "
            "or tell me what needs to be changed."
        )
        await self.send_assistant_message(message)

    async def check_tracking_id(self, candidate: str) -> None:
        tracking_id = normalize_tracking_id(candidate)

        if not is_valid_tracking_id(tracking_id):
            self.state.tracking_id = tracking_id or None
            self.state.tracking_id_confirmed = False
            self.state.stage = ConversationStage.COLLECTING_TRACKING_ID
            await self.send_conversation_state()
            await self.send_assistant_message(
                f"I heard {tracking_id or 'an incomplete tracking ID'}, but tracking IDs "
                "must have two letters followed by six digits. Please say it again."
            )
            return

        try:
            delivery = await find_delivery(tracking_id)
        except Exception:
            await self.send_assistant_message(
                "I cannot access the delivery system right now. Please try again."
            )
            return

        if delivery is None:
            self.state.tracking_id = tracking_id
            self.state.tracking_id_confirmed = False
            self.state.stage = ConversationStage.COLLECTING_TRACKING_ID
            await self.send_conversation_state()
            await self.send_assistant_message(
                f"I could not find tracking ID {tracking_id_for_speech(tracking_id)}. "
                "Please check it and say it again."
            )
            return

        self.state.tracking_id = tracking_id
        self.state.tracking_id_confirmed = False
        self.state.stage = ConversationStage.CONFIRMING_TRACKING_ID
        await self.send_conversation_state()
        await self.send_assistant_message(
            f"I found tracking ID {tracking_id_for_speech(tracking_id)}. "
            "Is that correct?"
        )

    async def offer_available_slots(self) -> None:
        slot_task = asyncio.create_task(
            self.run_backend_operation(
                get_available_slots(),
                progress_messages=[
                    "I am still checking the available slots. "
                    "This is taking a little longer than usual.",
                    "The delivery system is still responding. Thank you for waiting.",
                ],
            )
        )
        await self.send_assistant_message(
            "Your tracking ID is confirmed. I am checking the available slots now."
        )

        try:
            slots = await slot_task
        except Exception:
            await self.send_assistant_message(
                "I cannot check the available delivery slots right now. "
                "Please try again."
            )
            return

        self.state.available_slots = slots
        self.state.selected_slot = None
        self.state.slot_confirmed = False
        self.state.stage = ConversationStage.OFFERING_SLOTS
        await self.send_conversation_state()

        if not slots:
            await self.send_assistant_message(
                "I am sorry, but there are no delivery slots available right now."
            )
            return

        position_names = ["First", "Second", "Third"]
        options = " ".join(
            f"{position_names[index]}, {slot['label']}."
            for index, slot in enumerate(slots)
        )
        await self.send_assistant_message(
            f"I found these delivery slots. {options} Which one works best for you?"
        )

    async def select_slot(self, text: str) -> None:
        if not self.state.available_slots:
            await self.offer_available_slots()
            return

        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            decision = await self.llm.understand_slot_selection(
                text,
                self.state.available_slots,
            )
        except Exception as error:
            await self.send(
                {
                    "type": "llm_error",
                    "message": f"Could not understand the slot selection: {error}",
                }
            )
            return
        finally:
            await self.send({"type": "llm_status", "status": "idle"})

        if decision["tool"] != "select_delivery_slot":
            message = decision["message"] or (
                "Please choose one of the available delivery slots."
            )
            await self.send_assistant_message(message)
            return

        slot = self.find_offered_slot(decision["arguments"].get("slot_id"))
        if slot is None:
            await self.send_assistant_message(
                "That slot was not one of the available options. Please choose again."
            )
            return

        await self.ask_to_confirm_slot(slot)

    async def confirm_slot(self, text: str) -> None:
        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            decision = await self.llm.understand_slot_confirmation(
                text,
                self.state.selected_slot,
                self.state.available_slots,
            )
        except Exception as error:
            await self.send(
                {
                    "type": "llm_error",
                    "message": f"Could not understand the slot confirmation: {error}",
                }
            )
            return
        finally:
            await self.send({"type": "llm_status", "status": "idle"})

        if decision["tool"] == "confirm_delivery_slot":
            self.state.slot_confirmed = True
            self.state.stage = ConversationStage.BOOKING
            await self.send_conversation_state()
            await self.send_assistant_message(
                f"Thank you. I will book {self.state.selected_slot['label']} now."
            )
            await self.book_selected_slot()
            return

        if decision["tool"] == "change_delivery_slot":
            slot = self.find_offered_slot(decision["arguments"].get("slot_id"))
            if slot is not None:
                await self.ask_to_confirm_slot(slot)
                return

        if decision["tool"] == "reject_delivery_slot":
            self.state.selected_slot = None
            self.state.slot_confirmed = False
            self.state.booking_idempotency_key = None
            self.state.stage = ConversationStage.OFFERING_SLOTS
            await self.send_conversation_state()
            await self.send_assistant_message(
                "No problem. Please choose another available delivery slot."
            )
            return

        message = decision["message"] or (
            "Please say yes if that slot works, or choose another available slot."
        )
        await self.send_assistant_message(message)

    def find_offered_slot(self, slot_id: str | None) -> dict | None:
        return next(
            (
                slot
                for slot in self.state.available_slots
                if slot["slot_id"] == slot_id
            ),
            None,
        )

    async def ask_to_confirm_slot(self, slot: dict) -> None:
        current_slot_id = (
            self.state.selected_slot.get("slot_id")
            if self.state.selected_slot
            else None
        )
        if current_slot_id != slot["slot_id"]:
            self.state.booking_idempotency_key = str(uuid4())

        self.state.selected_slot = slot
        self.state.slot_confirmed = False
        self.state.stage = ConversationStage.CONFIRMING_SLOT
        await self.send_conversation_state()
        await self.send_assistant_message(
            f"You selected {slot['label']}. Will that work for you?"
        )

    async def book_selected_slot(self) -> None:
        try:
            result = await self.run_backend_operation(
                book_delivery_slot(
                    tracking_id=self.state.tracking_id,
                    slot_id=self.state.selected_slot["slot_id"],
                    idempotency_key=self.state.booking_idempotency_key,
                ),
                progress_messages=[
                    "I am still booking your delivery slot. "
                    "This is taking a little longer than usual.",
                    "The booking system is still responding. Thank you for waiting.",
                ],
            )
        except Exception:
            self.state.stage = ConversationStage.CONFIRMING_SLOT
            self.state.slot_confirmed = False
            await self.send_conversation_state()
            await self.send_assistant_message(
                "I could not complete the booking. Your selected slot is saved. "
                "Would you like me to try again?"
            )
            return

        if result["status"] == "unavailable":
            self.state.available_slots = []
            self.state.selected_slot = None
            self.state.slot_confirmed = False
            self.state.booking_idempotency_key = None
            self.state.stage = ConversationStage.OFFERING_SLOTS
            await self.send_conversation_state()
            await self.send_assistant_message(
                "That slot was just taken. I will check the available slots again."
            )
            await self.offer_available_slots()
            return

        if result["status"] == "already_booked":
            booked_slot = result["slot"]
            self.state.stage = ConversationStage.COMPLETED
            await self.send_conversation_state()
            await self.send_assistant_message(
                f"This delivery is already scheduled for {booked_slot['label']}."
            )
            return

        self.state.stage = ConversationStage.COMPLETED
        await self.send_conversation_state()
        await self.send_assistant_message(
            f"Done. Your delivery has been rescheduled to "
            f"{self.state.selected_slot['label']}."
        )

    async def run_backend_operation(
        self,
        operation,
        progress_messages: list[str],
    ):
        task = asyncio.create_task(operation)
        await self.send({"type": "backend_status", "status": "working"})

        try:
            for message in progress_messages:
                try:
                    return await asyncio.wait_for(
                        asyncio.shield(task),
                        timeout=settings.fake_backend_progress_seconds,
                    )
                except TimeoutError:
                    await self.send({"type": "backend_status", "status": "slow"})
                    await self.send_assistant_message(message)

            try:
                return await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=settings.fake_backend_progress_seconds,
                )
            except TimeoutError as error:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
                raise RuntimeError("The backend operation timed out") from error
        finally:
            await self.send({"type": "backend_status", "status": "idle"})

    async def send_assistant_message(self, message: str) -> None:
        await self.send({"type": "llm_start"})
        await self.send({"type": "llm_delta", "delta": message})
        await self.send({"type": "llm_done", "message": message})

        try:
            await self.tts.connect()
            await self.send(
                {
                    "type": "tts_audio_start",
                    "sample_rate": settings.deepgram_tts_sample_rate,
                }
            )
            await self.tts.send_text(message)
            await self.tts.flush()
        except Exception as error:
            await self.send(
                {
                    "type": "tts_error",
                    "message": f"Could not generate speech: {error}",
                }
            )

    async def send(self, payload: dict) -> None:
        async with self.send_lock:
            await self.websocket.send_json(payload)

    async def send_conversation_state(self) -> None:
        await self.send(
            {
                "type": "conversation_state",
                **self.state.to_dict(),
            }
        )

    async def send_audio(self, audio: bytes) -> None:
        async with self.send_lock:
            await self.websocket.send_bytes(audio)
