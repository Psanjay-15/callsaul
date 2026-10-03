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
        self.conversation_ending = False
        self.pending_voice_transcript = ""
        self.pending_voice_commit_task: asyncio.Task | None = None
        self.state = ConversationState()
        self.llm = LLMService()
        self.tts = TTSService(self.send_audio, self.send)
        self.connection_context = None
        self.connection = None
        self.listener_task: asyncio.Task | None = None
        self.idle_task: asyncio.Task | None = None
        self.start_message: str | None = None
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
        self.start_message = (
            "I am listening. What would you like to do with this delivery?"
            if resumed
            else (
                "Hello, I am Saul, CallSaul's delivery assistant. I can help "
                "reschedule your delivery. Please say your tracking ID. It has two "
                "letters followed by six digits."
            )
        )
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
            await self.stop_idle_monitor()
            self.cancel_pending_voice_turn()
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

        if self.state.stage == ConversationStage.BOOKING:
            self.state.stage = ConversationStage.CONFIRMING_SLOT
            self.state.slot_confirmed = False
            await save_conversation_state(self.session_id, self.state)

        return True

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
            # Establish listening first. Opening STT and TTS handshakes at exactly
            # the same time can make one of the provider connections time out.
            await self.start_deepgram_stream(message.get("mime_type", "unknown"))
            if self.connection is None:
                return False
            self.mark_activity()
            await self.start_idle_monitor()

            if self.start_message:
                message_to_send = self.start_message
                self.start_message = None
                greeting_task = asyncio.create_task(
                    self.send_assistant_message(message_to_send)
                )
                self.user_tasks.add(greeting_task)
                greeting_task.add_done_callback(self.user_tasks.discard)
        elif message_type == "audio_stop":
            await self.stop_idle_monitor()
            await self.handle_barge_in()
            self.cancel_pending_voice_turn(clear_transcript=True)
            await self.stop_audio()
        elif message_type == "ping":
            await self.send({"type": "pong"})
        elif message_type == "message":
            self.start_message = None
            await self.handle_user_input(message.get("text"))
        elif message_type == "stop":
            await self.stop_idle_monitor()
            await self.handle_barge_in()
            for task in list(self.user_tasks):
                task.cancel()
            if self.user_tasks:
                await asyncio.gather(*self.user_tasks, return_exceptions=True)
            await self.stop_audio()
            await self.tts.close()
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
            self.audio_active = False
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
            # Flux may end a turn during a short thinking pause. Keep the text
            # from that provisional turn and join it to the speech that follows.
            self.cancel_pending_voice_turn()
            self.mark_user_activity()
            self.user_is_speaking = True
            await self.handle_barge_in()
        elif turn_event == "EndOfTurn":
            self.user_is_speaking = False

        transcript = message.transcript.strip()
        if turn_event == "EndOfTurn" and transcript:
            self.pending_voice_transcript = self.merge_voice_transcript(
                self.pending_voice_transcript,
                transcript,
            )

        await self.send(
            {
                "type": "transcript",
                "event": turn_event,
                "text": (
                    self.pending_voice_transcript
                    if turn_event == "EndOfTurn"
                    else message.transcript
                ),
                # EndOfTurn is provisional. The transcript becomes final only
                # after the caller has stayed quiet for the commit delay.
                "is_final": False,
                "turn_index": message.turn_index,
            }
        )

        if (
            turn_event == "EndOfTurn"
            and self.audio_active
            and transcript
        ):
            self.cancel_pending_voice_turn()
            task = asyncio.create_task(self.commit_pending_voice_turn())
            self.pending_voice_commit_task = task
            self.user_tasks.add(task)
            task.add_done_callback(self.user_tasks.discard)

    @staticmethod
    def merge_voice_transcript(current: str, incoming: str) -> str:
        current = current.strip()
        incoming = incoming.strip()
        if not current:
            return incoming
        if not incoming or incoming == current:
            return current
        if incoming.startswith(current):
            return incoming
        return f"{current} {incoming}"

    def cancel_pending_voice_turn(self, *, clear_transcript: bool = False) -> None:
        task = self.pending_voice_commit_task
        self.pending_voice_commit_task = None
        if task is not None and not task.done():
            task.cancel()
        if clear_transcript:
            self.pending_voice_transcript = ""

    async def commit_pending_voice_turn(self) -> None:
        word_count = len(self.pending_voice_transcript.split())
        delay_ms = (
            settings.voice_short_turn_commit_delay_ms
            if word_count <= 2
            else settings.voice_turn_commit_delay_ms
        )
        try:
            await asyncio.sleep(delay_ms / 1000)
        except asyncio.CancelledError:
            return

        # Clear the cancellable timer before processing. Once processing starts,
        # a later utterance may interrupt speech but cannot cancel a DB write.
        if self.pending_voice_commit_task is asyncio.current_task():
            self.pending_voice_commit_task = None

        transcript = self.pending_voice_transcript.strip()
        self.pending_voice_transcript = ""
        if not transcript or not self.audio_active:
            return

        await self.send(
            {
                "type": "transcript",
                "event": "CommittedTurn",
                "text": transcript,
                "is_final": True,
            }
        )
        await self.handle_user_input(transcript)

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
            except Exception:
                # A provider WebSocket can fail its keepalive while it is closing.
                # The stream is already unusable, so cleanup should continue.
                pass

        if connection_context is not None:
            with suppress(Exception):
                await connection_context.__aexit__(None, None, None)

    async def send_to_llm(self, text) -> None:
        if not isinstance(text, str) or not text.strip():
            await self.send({"type": "error", "message": "Message cannot be empty."})
            return

        await self.stream_llm_response(
            self.llm.stream_response(self.llm_history())
        )

    async def send_grounded_response(
        self,
        response_type: str,
        facts: dict,
    ) -> None:
        await self.stream_llm_response(
            self.llm.stream_grounded_response(
                self.llm_history(),
                facts,
                response_type,
            )
        )

    async def stream_llm_response(self, response_stream) -> None:

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
            async for delta in response_stream:
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
        if self.conversation_ending:
            return

        self.mark_user_activity()
        async with self.conversation_lock:
            if self.conversation_ending:
                return
            await self.record_message("user", text.strip())
            if self.state.stage == ConversationStage.COLLECTING_TRACKING_ID:
                await self.collect_tracking_id(text.strip())
            elif self.state.stage == ConversationStage.CONFIRMING_TRACKING_ID:
                await self.confirm_tracking_id(text.strip())
            elif self.state.stage == ConversationStage.OFFERING_SLOTS:
                await self.select_slot(text.strip())
            elif self.state.stage == ConversationStage.NO_SLOTS:
                await self.handle_no_slots_follow_up(text.strip())
            elif self.state.stage == ConversationStage.CONFIRMING_SLOT:
                await self.confirm_slot(text.strip())
            elif self.state.stage == ConversationStage.COMPLETED:
                await self.handle_completed_conversation(text.strip())
            else:
                await self.send_to_llm(text.strip())

    async def handle_end_conversation(self, decision: dict) -> bool:
        if decision.get("tool") != "end_conversation":
            return False
        if self.conversation_ending:
            return True

        self.conversation_ending = True
        self.start_message = None
        await self.stop_idle_monitor()
        try:
            await self.send_grounded_response(
                "conversation_ended",
                facts={"conversation_ended": True},
            )
            await self.tts.wait_until_finished()
        finally:
            await self.stop_audio()
            await self.tts.close()
            await self.send({"type": "conversation_ended"})
        return True

    async def handle_completed_conversation(self, text: str) -> None:
        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            decision = await self.llm.understand_completed_request(
                text,
                self.state.selected_slot,
                self.state.tracking_id,
                history=self.llm_history(),
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

        if await self.handle_end_conversation(decision):
            return

        if decision["tool"] == "start_delivery_reschedule":
            await self.start_delivery_reschedule()
            return

        if decision["tool"] == "provide_tracking_id":
            await self.provide_saved_tracking_id()
            return

        if decision["tool"] == "provide_delivery_detail":
            await self.provide_delivery_detail(
                decision["arguments"].get("field")
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
                operation_name="check_available_slots",
            )
        )

        try:
            slots = await slot_task
        except Exception:
            await self.send_grounded_response(
                "reschedule_lookup_failed",
                facts={
                    "operation": "check_available_slots",
                    "status": "failed",
                    "current_booking": self.state.selected_slot,
                    "delivery_changed": False,
                },
            )
            return

        if not slots:
            await self.send_grounded_response(
                "no_alternative_slots",
                facts={
                    "operation": "check_available_slots",
                    "status": "success",
                    "available_slots": [],
                    "current_booking": self.state.selected_slot,
                    "delivery_changed": False,
                },
            )
            return

        self.state.available_slots = slots
        self.state.slot_confirmed = False
        self.state.booking_idempotency_key = None

        if len(slots) == 1:
            await self.ask_to_confirm_slot(slots[0])
            return

        self.state.stage = ConversationStage.OFFERING_SLOTS
        await self.send_conversation_state()

        await self.send_grounded_response(
            "alternative_slot_options",
            facts={
                "operation": "check_available_slots",
                "status": "success",
                "available_slots": [
                    {"slot_id": slot["slot_id"], "label": slot["label"]}
                    for slot in slots
                ],
                "current_booking": self.state.selected_slot,
                "delivery_changed": False,
            },
        )

    async def collect_tracking_id(self, text: str) -> None:
        direct_candidate = normalize_tracking_id(text)
        if is_valid_tracking_id(direct_candidate):
            await self.check_tracking_id(direct_candidate)
            return

        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            decision = await self.llm.understand_tracking_id(
                text,
                self.state.tracking_id,
                history=self.llm_history(),
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

        if await self.handle_end_conversation(decision):
            return

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
                history=self.llm_history(),
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

        if await self.handle_end_conversation(decision):
            return

        if decision["tool"] == "provide_tracking_id":
            await self.provide_saved_tracking_id()
            return

        if decision["tool"] == "provide_delivery_detail":
            await self.provide_delivery_detail(
                decision["arguments"].get("field")
            )
            return

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
            await self.send_grounded_response(
                "tracking_lookup_failed",
                facts={
                    "operation": "find_delivery",
                    "status": "failed",
                    "tracking_id": tracking_id,
                },
            )
            return

        if delivery is None:
            self.state.tracking_id = tracking_id
            self.state.tracking_id_confirmed = False
            self.state.stage = ConversationStage.COLLECTING_TRACKING_ID
            await self.send_conversation_state()
            await self.send_grounded_response(
                "tracking_not_found",
                facts={
                    "operation": "find_delivery",
                    "status": "success",
                    "delivery_found": False,
                    "tracking_id": tracking_id,
                    "tracking_id_for_speech": tracking_id_for_speech(tracking_id),
                },
            )
            return

        self.state.tracking_id = tracking_id
        self.state.tracking_id_confirmed = False
        self.state.stage = ConversationStage.CONFIRMING_TRACKING_ID
        await self.send_conversation_state()
        await self.send_grounded_response(
            "tracking_found",
            facts={
                "operation": "find_delivery",
                "status": "success",
                "delivery_found": True,
                "tracking_id": tracking_id,
                "tracking_id_for_speech": tracking_id_for_speech(tracking_id),
            },
        )

    async def offer_available_slots(self) -> None:
        slot_task = asyncio.create_task(
            self.run_backend_operation(
                get_available_slots(),
                operation_name="check_available_slots",
            )
        )

        try:
            slots = await slot_task
        except Exception:
            await self.send_grounded_response(
                "slot_lookup_failed",
                facts={
                    "operation": "check_available_slots",
                    "status": "failed",
                    "delivery_changed": False,
                },
            )
            return

        self.state.available_slots = slots
        self.state.selected_slot = None
        self.state.slot_confirmed = False

        if not slots:
            self.state.stage = ConversationStage.NO_SLOTS
            await self.send_conversation_state()
            await self.send_grounded_response(
                "no_slots",
                facts={
                    "operation": "check_available_slots",
                    "status": "success",
                    "available_slots": [],
                    "delivery_changed": False,
                },
            )
            return

        if len(slots) == 1:
            await self.ask_to_confirm_slot(slots[0])
            return

        self.state.stage = ConversationStage.OFFERING_SLOTS
        await self.send_conversation_state()

        await self.send_grounded_response(
            "slot_options",
            facts={
                "operation": "check_available_slots",
                "status": "success",
                "available_slots": [
                    {"slot_id": slot["slot_id"], "label": slot["label"]}
                    for slot in slots
                ],
                "delivery_changed": False,
            },
        )

    async def select_slot(self, text: str) -> None:
        if not self.state.available_slots:
            await self.handle_no_slots_follow_up(text)
            return

        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            decision = await self.llm.understand_slot_selection(
                text,
                self.state.available_slots,
                history=self.llm_history(),
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

        if await self.handle_end_conversation(decision):
            return

        if decision["tool"] == "provide_tracking_id":
            await self.provide_saved_tracking_id()
            return

        if decision["tool"] == "provide_delivery_detail":
            await self.provide_delivery_detail(
                decision["arguments"].get("field")
            )
            return

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

    async def handle_no_slots_follow_up(self, text: str) -> None:
        if self.state.stage != ConversationStage.NO_SLOTS:
            self.state.stage = ConversationStage.NO_SLOTS
            await self.send_conversation_state()

        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            decision = await self.llm.understand_no_slots_follow_up(
                text,
                self.state.tracking_id,
                self.state.selected_slot,
                history=self.llm_history(),
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

        if await self.handle_end_conversation(decision):
            return

        if decision["tool"] == "check_available_slots":
            await self.offer_available_slots()
            return

        if decision["tool"] == "provide_tracking_id":
            await self.provide_saved_tracking_id()
            return

        if decision["tool"] == "provide_delivery_detail":
            await self.provide_delivery_detail(
                decision["arguments"].get("field")
            )
            return

        message = decision["message"] or (
            "There are no available slots right now, so I have not changed your "
            "delivery. I can check again if you would like, or you can try later."
        )
        await self.send_assistant_message(message)

    async def confirm_slot(self, text: str) -> None:
        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            decision = await self.llm.understand_slot_confirmation(
                text,
                self.state.selected_slot,
                self.state.available_slots,
                tracking_id=self.state.tracking_id,
                history=self.llm_history(),
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

        if await self.handle_end_conversation(decision):
            return

        if decision["tool"] == "provide_tracking_id":
            await self.provide_saved_tracking_id()
            return

        if decision["tool"] == "provide_delivery_detail":
            await self.provide_delivery_detail(
                decision["arguments"].get("field")
            )
            return

        if decision["tool"] == "confirm_delivery_slot":
            self.state.slot_confirmed = True
            self.state.stage = ConversationStage.BOOKING
            await self.send_conversation_state()
            await self.book_selected_slot()
            return

        if decision["tool"] == "change_delivery_slot":
            slot = self.find_offered_slot(decision["arguments"].get("slot_id"))
            if slot is not None:
                await self.ask_to_confirm_slot(slot)
                return

        if decision["tool"] == "reject_delivery_slot":
            if len(self.state.available_slots) == 1:
                booked_slot = (
                    await get_booked_slot(self.state.tracking_id)
                    if self.state.tracking_id
                    else None
                )
                self.state.available_slots = []
                self.state.selected_slot = booked_slot
                self.state.slot_confirmed = False
                self.state.booking_idempotency_key = None
                self.state.stage = ConversationStage.COMPLETED
                await self.send_conversation_state()
                await self.send_grounded_response(
                    "slot_rejected_no_alternatives",
                    facts={
                        "selected_slot_rejected": True,
                        "available_alternative_slots": [],
                        "current_booking": booked_slot,
                        "delivery_changed": False,
                    },
                )
                return

            self.state.selected_slot = None
            self.state.slot_confirmed = False
            self.state.booking_idempotency_key = None
            self.state.stage = ConversationStage.OFFERING_SLOTS
            await self.send_conversation_state()
            await self.send_grounded_response(
                "slot_rejected_with_alternatives",
                facts={
                    "selected_slot_rejected": True,
                    "available_slots": [
                        {"slot_id": slot["slot_id"], "label": slot["label"]}
                        for slot in self.state.available_slots
                    ],
                    "delivery_changed": False,
                },
            )
            return

        message = decision["message"] or (
            "Please say yes if that slot works, or choose another available slot."
        )
        await self.send_assistant_message(message)

    async def provide_saved_tracking_id(self) -> None:
        if self.state.tracking_id:
            await self.send_grounded_response(
                "saved_tracking_id",
                facts={
                    "tracking_id": self.state.tracking_id,
                    "tracking_id_for_speech": tracking_id_for_speech(
                        self.state.tracking_id
                    ),
                },
            )
            return

        await self.send_grounded_response(
            "missing_tracking_id",
            facts={"tracking_id": None},
        )

    async def provide_delivery_detail(self, field: str | None) -> None:
        if not self.state.tracking_id:
            await self.send_grounded_response(
                "tracking_required",
                facts={"tracking_id": None, "delivery_details_available": False},
            )
            return

        try:
            delivery = await find_delivery(self.state.tracking_id)
        except Exception:
            await self.send_grounded_response(
                "delivery_lookup_failed",
                facts={
                    "operation": "find_delivery",
                    "status": "failed",
                    "tracking_id": self.state.tracking_id,
                },
            )
            return

        if delivery is None:
            await self.send_grounded_response(
                "delivery_not_found",
                facts={
                    "operation": "find_delivery",
                    "status": "success",
                    "delivery_found": False,
                    "tracking_id": self.state.tracking_id,
                },
            )
            return

        slot = self.state.selected_slot
        if field in {"delivery_time", "summary"} and slot is None:
            slot = await get_booked_slot(self.state.tracking_id)

        await self.send_grounded_response(
            "delivery_detail",
            facts={
                "requested_field": field,
                "tracking_id": self.state.tracking_id,
                "tracking_id_for_speech": tracking_id_for_speech(
                    self.state.tracking_id
                ),
                "customer_name": delivery.get("customer_name"),
                "customer_number": delivery.get("customer_number"),
                "delivery_status": str(delivery.get("status") or "").replace(
                    "_", " "
                ),
                "delivery_slot": slot,
                "slot_confirmed": self.state.slot_confirmed,
            },
        )

    def find_offered_slot(self, slot_id: str | None) -> dict | None:
        return next(
            (
                slot
                for slot in self.state.available_slots
                if slot["slot_id"] == slot_id
            ),
            None,
        )

    async def ask_to_confirm_slot(
        self,
        slot: dict,
    ) -> None:
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
        await self.send_grounded_response(
            "confirm_selected_slot",
            facts={
                "selected_slot": {
                    "slot_id": slot["slot_id"],
                    "label": slot["label"],
                },
                "slot_confirmed": False,
                "delivery_changed": False,
            },
        )

    async def book_selected_slot(self) -> None:
        try:
            result = await self.run_backend_operation(
                book_delivery_slot(
                    tracking_id=self.state.tracking_id,
                    slot_id=self.state.selected_slot["slot_id"],
                    idempotency_key=self.state.booking_idempotency_key,
                ),
                operation_name="book_delivery_slot",
            )
        except Exception:
            self.state.stage = ConversationStage.CONFIRMING_SLOT
            self.state.slot_confirmed = False
            await self.send_conversation_state()
            await self.send_grounded_response(
                "booking_failed",
                facts={
                    "operation": "book_delivery_slot",
                    "status": "failed",
                    "selected_slot": self.state.selected_slot,
                    "delivery_changed": False,
                },
            )
            return

        if result["status"] == "unavailable":
            self.state.available_slots = []
            self.state.selected_slot = None
            self.state.slot_confirmed = False
            self.state.booking_idempotency_key = None
            self.state.stage = ConversationStage.OFFERING_SLOTS
            await self.send_conversation_state()
            await self.offer_available_slots()
            return

        if result["status"] == "already_booked":
            booked_slot = result["slot"]
            self.state.selected_slot = booked_slot
            self.state.slot_confirmed = True
            self.state.stage = ConversationStage.COMPLETED
            await self.send_conversation_state()
            await self.send_grounded_response(
                "already_booked",
                facts={
                    "operation": "book_delivery_slot",
                    "status": "already_booked",
                    "booked_slot": booked_slot,
                    "delivery_changed": False,
                },
            )
            return

        self.state.selected_slot = result["slot"]
        self.state.slot_confirmed = True
        self.state.stage = ConversationStage.COMPLETED
        await self.send_conversation_state()
        await self.send_grounded_response(
            "booking_succeeded",
            facts={
                "operation": "book_delivery_slot",
                "status": "booked",
                "booked_slot": self.state.selected_slot,
                "delivery_changed": True,
            },
        )

    async def run_backend_operation(
        self,
        operation,
        operation_name: str,
    ):
        task = asyncio.create_task(operation)
        await self.send({"type": "backend_status", "status": "working"})

        try:
            for update_number in range(1, 3):
                try:
                    return await asyncio.wait_for(
                        asyncio.shield(task),
                        timeout=settings.fake_backend_progress_seconds,
                    )
                except TimeoutError:
                    await self.send({"type": "backend_status", "status": "slow"})
                    await self.send_grounded_response(
                        "backend_progress",
                        facts={
                            "operation": operation_name,
                            "status": "still_working",
                            "progress_update": update_number,
                        },
                    )

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
        tts_connect_task = None
        if not self.user_is_speaking:
            tts_connect_task = asyncio.create_task(self.tts.connect())

        await self.record_message("assistant", message)
        await self.send({"type": "llm_done", "message": message})

        if self.user_is_speaking:
            if tts_connect_task is not None:
                await asyncio.gather(tts_connect_task, return_exceptions=True)
            await self.send({"type": "tts_status", "status": "interrupted"})
            return

        try:
            if tts_connect_task is not None:
                await tts_connect_task
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

    async def start_idle_monitor(self) -> None:
        if self.conversation_ending:
            return
        await self.stop_idle_monitor()
        self.idle_task = asyncio.create_task(self.monitor_inactivity())

    async def stop_idle_monitor(self) -> None:
        task = self.idle_task
        self.idle_task = None
        if task is None or task is asyncio.current_task():
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def monitor_inactivity(self) -> None:
        timeout = settings.conversation_idle_timeout_seconds

        while (
            self.websocket_open
            and self.audio_active
            and not self.conversation_ending
        ):
            idle_for = asyncio.get_running_loop().time() - self.last_activity_at
            remaining = timeout - idle_for
            if remaining > 0:
                await asyncio.sleep(remaining)
                continue

            if self.conversation_ending:
                return

            previous_user_activity = self.last_user_activity_at
            await self.send_assistant_message(
                "I have not heard anything for a while, so I will pause listening. "
                "Press Start listening whenever you are ready."
            )
            await self.tts.wait_until_finished()
            if self.last_user_activity_at > previous_user_activity:
                continue

            await self.send({"type": "conversation_paused"})
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
