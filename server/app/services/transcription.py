import asyncio
import json
from uuid import uuid4
from contextlib import suppress

from deepgram.core.events import EventType
from fastapi.encoders import jsonable_encoder
from fastapi import WebSocket, WebSocketDisconnect

from app.clients.deepgram import create_deepgram_client
from app.config.settings import settings
from app.conversation import ConversationStage, ConversationState
from app.services.llm import LLMService
from app.services.sessions import (
    append_chat_message,
    create_chat_session,
    get_chat_session,
    list_chat_messages,
    message_for_client,
    messages_for_llm,
    save_conversation_state,
)
from app.services.slots import (
    book_delivery_slot,
    get_available_slots,
    get_booked_slot,
)
from app.services.tracking import (
    find_delivery,
    is_valid_tracking_id,
    normalize_tracking_id,
    tracking_id_for_speech,
)
from app.services.tts import TTSService


class TranscriptionService:
    def __init__(
        self,
        websocket: WebSocket,
        session_id: str | None = None,
    ) -> None:
        self.websocket = websocket
        self.requested_session_id = session_id
        self.session_id = ""
        self.history: list[dict] = []
        self.send_lock = asyncio.Lock()
        self.conversation_lock = asyncio.Lock()
        self.user_tasks: set[asyncio.Task] = set()
        self.response_generation = 0
        self.user_is_speaking = False
        self.state = ConversationState()
        self.llm = LLMService()
        self.tts = TTSService(self.send_audio, self.send)
        self.connection_context = None
        self.connection = None
        self.listener_task: asyncio.Task | None = None
        self.idle_task: asyncio.Task | None = None
        self.last_activity_at = 0.0
        self.last_user_activity_at = 0.0
        self.websocket_open = True
        self.audio_active = False
        self.audio_chunks = 0
        self.audio_bytes = 0

    async def run(self) -> None:
        try:
            resumed = await self.initialize_session()
        except LookupError:
            await self.send(
                {
                    "type": "error",
                    "message": "That session ID was not found.",
                }
            )
            await self.websocket.close(code=4404)
            return
        resume_message = self.get_resume_message() if resumed else None
        await self.send(
            {
                "type": "session",
                "session_id": self.session_id,
                "resumed": resumed,
            }
        )
        if resumed:
            await self.send(
                {
                    "type": "conversation_history",
                    "messages": [
                        message_for_client(message) for message in self.history
                    ],
                }
            )
        await self.send_conversation_state(persist=resumed)

        if resumed:
            await self.send_assistant_message(resume_message)
        else:
            await self.send_assistant_message(
                "Hello. I can help reschedule your delivery. "
                "Please say your tracking ID. It has two letters followed by six digits."
            )

        self.mark_activity()
        self.idle_task = asyncio.create_task(self.monitor_inactivity())

        try:
            while True:
                event = await self.websocket.receive()
                if event.get("type") == "websocket.disconnect":
                    self.websocket_open = False
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
            if self.idle_task is not None:
                self.idle_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self.idle_task
            for task in self.user_tasks:
                task.cancel()
            if self.user_tasks:
                await asyncio.gather(*self.user_tasks, return_exceptions=True)
            await self.close_deepgram_stream()
            await self.tts.close()

    async def initialize_session(self) -> bool:
        session = None
        if self.requested_session_id:
            session = await get_chat_session(self.requested_session_id)

            if session is None:
                raise LookupError("Chat session not found")

        if session is None:
            session = await create_chat_session()
            self.session_id = session["session_id"]
            self.state = ConversationState()
            self.history = []
            return False

        self.session_id = session["session_id"]
        self.state = ConversationState.from_dict(session.get("state"))
        self.history = await list_chat_messages(self.session_id)

        if (
            self.state.stage == ConversationStage.COMPLETED
            and self.state.tracking_id
        ):
            booked_slot = await get_booked_slot(self.state.tracking_id)
            if booked_slot is not None:
                self.state.selected_slot = booked_slot
                self.state.slot_confirmed = True
                await save_conversation_state(self.session_id, self.state)

        return True

    def get_resume_message(self) -> str:
        if self.state.stage == ConversationStage.CONFIRMING_TRACKING_ID:
            if not self.state.tracking_id:
                self.state.stage = ConversationStage.COLLECTING_TRACKING_ID
                return "Welcome back. Please say your complete tracking ID again."
            return (
                "Welcome back. I still have tracking ID "
                f"{tracking_id_for_speech(self.state.tracking_id)}. Is that correct?"
            )
        if self.state.stage == ConversationStage.OFFERING_SLOTS:
            if self.state.available_slots:
                options = " ".join(
                    f"{index}. {slot['label']}."
                    for index, slot in enumerate(
                        self.state.available_slots,
                        start=1,
                    )
                )
                return f"Welcome back. The available slots were: {options}"
            return "Welcome back. I can continue checking delivery slots for you."
        if self.state.stage == ConversationStage.CONFIRMING_SLOT:
            if not self.state.selected_slot:
                self.state.stage = ConversationStage.OFFERING_SLOTS
                return "Welcome back. Please choose an available delivery slot."
            return (
                "Welcome back. You selected "
                f"{self.state.selected_slot['label']}. Will that work for you?"
            )
        if self.state.stage == ConversationStage.BOOKING:
            self.state.stage = ConversationStage.CONFIRMING_SLOT
            self.state.slot_confirmed = False
            return (
                "Welcome back. I saved your selected slot, but the booking was "
                "interrupted. Please confirm if you want me to try booking it again."
            )
        if self.state.stage == ConversationStage.COMPLETED:
            if not self.state.selected_slot:
                return "Welcome back. How can I help with your delivery?"
            return (
                "Welcome back. Your delivery is scheduled for "
                f"{self.state.selected_slot['label']}. How can I help?"
            )
        return (
            "Welcome back. Please say your tracking ID. "
            "It has two letters followed by six digits."
        )

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
            self.websocket_open = False
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
                eot_threshold=str(settings.deepgram_eot_threshold),
                eot_timeout_ms=str(settings.deepgram_eot_timeout_ms),
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
            self.mark_user_activity()
            self.user_is_speaking = True
            await self.handle_barge_in()
        elif turn_event == "EndOfTurn":
            self.user_is_speaking = False

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
        self.user_is_speaking = False
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
            self.tts.begin_turn()
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
            async for delta in self.llm.stream_response(self.llm_history()):
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
                if full_response.strip():
                    await self.record_message("assistant", full_response.strip())
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

            if full_response.strip():
                await self.record_message("assistant", full_response.strip())
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
            self.mark_activity()
            await self.send({"type": "llm_status", "status": "idle"})

    async def handle_user_input(self, text) -> None:
        if not isinstance(text, str) or not text.strip():
            await self.send({"type": "error", "message": "Message cannot be empty."})
            return

        self.mark_user_activity()
        async with self.conversation_lock:
            await self.record_message("user", text.strip())
            if self.state.stage == ConversationStage.COLLECTING_TRACKING_ID:
                await self.collect_tracking_id(text.strip())
            elif self.state.stage == ConversationStage.CONFIRMING_TRACKING_ID:
                await self.confirm_tracking_id(text.strip())
            elif self.state.stage == ConversationStage.OFFERING_SLOTS:
                await self.select_slot(text.strip())
            elif self.state.stage == ConversationStage.CONFIRMING_SLOT:
                await self.confirm_slot(text.strip())
            elif self.state.stage == ConversationStage.COMPLETED:
                await self.handle_completed_conversation(text.strip())
            else:
                await self.send_to_llm(text.strip())

    async def handle_completed_conversation(self, text: str) -> None:
        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            decision = await self.llm.understand_completed_request(
                text,
                self.state.selected_slot,
                self.state.tracking_id,
            )
        except Exception as error:
            await self.send(
                {
                    "type": "llm_error",
                    "message": f"Could not understand the request: {error}",
                }
            )
            return
        finally:
            await self.send({"type": "llm_status", "status": "idle"})

        if decision["tool"] == "start_delivery_reschedule":
            await self.start_delivery_reschedule()
            return

        if decision["tool"] == "provide_tracking_id":
            if self.state.tracking_id:
                await self.send_assistant_message(
                    "Your tracking ID is "
                    f"{tracking_id_for_speech(self.state.tracking_id)}."
                )
            else:
                await self.send_assistant_message(
                    "I do not have a tracking ID saved for this conversation."
                )
            return

        message = decision["message"] or (
            "Your delivery is already scheduled. "
            "Tell me if you would like to change the delivery time."
        )
        await self.send_assistant_message(message)

    async def start_delivery_reschedule(self) -> None:
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
            "I will check the available slots so we can change your delivery time."
        )

        try:
            slots = await slot_task
        except Exception:
            await self.send_assistant_message(
                "I cannot check the available delivery slots right now. "
                "Your current booking has not changed. Please try again."
            )
            return

        if not slots:
            await self.send_assistant_message(
                "There are no other delivery slots available right now. "
                "Your current booking has not changed."
            )
            return

        self.state.available_slots = slots
        self.state.slot_confirmed = False
        self.state.booking_idempotency_key = None
        self.state.stage = ConversationStage.OFFERING_SLOTS
        await self.send_conversation_state()

        position_names = ["First", "Second", "Third"]
        options = " ".join(
            f"{position_names[index]}, {slot['label']}."
            for index, slot in enumerate(slots)
        )
        await self.send_assistant_message(
            f"I found these delivery slots. {options} Which one works best for you?"
        )

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
            position_names = ["First", "Second", "Third"]
            options = " ".join(
                f"{position_names[index]}, {slot['label']}."
                for index, slot in enumerate(self.state.available_slots)
            )
            await self.send_assistant_message(
                f"No problem. The available slots are: {options} "
                "Which one works best for you?"
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
            self.state.selected_slot = booked_slot
            self.state.slot_confirmed = True
            self.state.stage = ConversationStage.COMPLETED
            await self.send_conversation_state()
            await self.send_assistant_message(
                f"This delivery is already scheduled for {booked_slot['label']}."
            )
            return

        self.state.selected_slot = result["slot"]
        self.state.slot_confirmed = True
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
        await self.record_message("assistant", message)
        await self.send({"type": "llm_done", "message": message})

        if self.user_is_speaking:
            await self.send({"type": "tts_status", "status": "interrupted"})
            return

        try:
            await self.tts.connect()
            self.tts.begin_turn()
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
        finally:
            self.mark_activity()

    def mark_activity(self) -> None:
        self.last_activity_at = asyncio.get_running_loop().time()

    def mark_user_activity(self) -> None:
        now = asyncio.get_running_loop().time()
        self.last_activity_at = now
        self.last_user_activity_at = now

    async def monitor_inactivity(self) -> None:
        timeout = settings.conversation_idle_timeout_seconds

        while self.websocket_open:
            idle_for = asyncio.get_running_loop().time() - self.last_activity_at
            remaining = timeout - idle_for
            if remaining > 0:
                await asyncio.sleep(remaining)
                continue

            previous_user_activity = self.last_user_activity_at
            await self.send_assistant_message(
                "I have not heard anything for a while, so I will end this conversation now."
            )
            await self.tts.wait_until_finished()
            if self.last_user_activity_at > previous_user_activity:
                continue

            self.websocket_open = False
            with suppress(RuntimeError):
                await self.websocket.close(code=1000, reason="Idle timeout")
            return

    async def send(self, payload: dict) -> None:
        if not self.websocket_open:
            return

        async with self.send_lock:
            if not self.websocket_open:
                return
            try:
                await self.websocket.send_json(jsonable_encoder(payload))
            except RuntimeError:
                self.websocket_open = False

    async def send_conversation_state(self, persist: bool = True) -> None:
        if persist:
            await save_conversation_state(self.session_id, self.state)
        await self.send(
            {
                "type": "conversation_state",
                **self.state.to_dict(),
            }
        )

    async def record_message(self, role: str, content: str) -> None:
        message = await append_chat_message(
            self.session_id,
            role,
            content,
        )
        self.history.append(message)

    def llm_history(self) -> list[dict]:
        return messages_for_llm(self.history)

    async def send_audio(self, audio: bytes) -> None:
        if not self.websocket_open:
            return

        async with self.send_lock:
            if not self.websocket_open:
                return
            try:
                await self.websocket.send_bytes(audio)
            except RuntimeError:
                self.websocket_open = False
