import json
from collections.abc import AsyncIterator

from app.clients.openai import create_openai_client
from app.config.settings import settings


RECENT_MESSAGE_LIMIT = 12


def provide_tracking_id_tool() -> dict:
    return {
        "type": "function",
        "name": "provide_tracking_id",
        "description": "Return the tracking ID already saved in this conversation.",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
        "strict": True,
    }


def provide_delivery_detail_tool() -> dict:
    return {
        "type": "function",
        "name": "provide_delivery_detail",
        "description": (
            "Return a factual detail about the delivery already identified in this "
            "conversation. Use this for the customer name, customer number, delivery "
            "status, or currently selected delivery time."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "field": {
                    "type": "string",
                    "enum": [
                        "customer_name",
                        "customer_number",
                        "delivery_status",
                        "delivery_time",
                    ],
                }
            },
            "required": ["field"],
            "additionalProperties": False,
        },
        "strict": True,
    }


class LLMService:
    def __init__(self) -> None:
        self.client = create_openai_client()

    async def stream_response(self, history: list[dict]) -> AsyncIterator[str]:
        stream = await self.client.responses.create(
            model=settings.openai_model,
            instructions=(
                "You are a helpful voice assistant. "
                "Answer clearly and keep the response brief."
            ),
            input=history[-RECENT_MESSAGE_LIMIT:],
            stream=True,
        )

        async for event in stream:
            if event.type == "response.output_text.delta":
                yield event.delta

    async def understand_tracking_id(
        self,
        transcription: str,
        current_tracking_id: str | None = None,
        history: list[dict] | None = None,
    ) -> dict:
        previous_candidate = (
            f"The previously heard candidate was {current_tracking_id}. "
            "If the caller is correcting it, return the complete corrected candidate. "
            if current_tracking_id
            else ""
        )

        return await self._get_tool_decision(
            instructions=(
                "The caller is being asked for a courier tracking ID. "
                "A tracking ID should contain two letters followed by six digits. "
                f"{previous_candidate}"
                "If the caller provides or spells a candidate ID, call "
                "capture_tracking_id. Understand letter names, NATO phonetic words "
                "such as Bravo, and spoken digit words. Return exactly what the caller "
                "provided without inventing missing characters. If they do not provide "
                "an ID, answer their question briefly and ask for the tracking ID again."
            ),
            transcription=transcription,
            history=history,
            tools=[
                {
                    "type": "function",
                    "name": "capture_tracking_id",
                    "description": "Capture the tracking ID candidate spoken by the caller.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "tracking_id": {
                                "type": "string",
                                "description": (
                                    "The complete candidate using letters and digits only."
                                ),
                            }
                        },
                        "required": ["tracking_id"],
                        "additionalProperties": False,
                    },
                    "strict": True,
                }
            ],
        )

    async def understand_tracking_confirmation(
        self,
        transcription: str,
        current_tracking_id: str,
        history: list[dict] | None = None,
    ) -> dict:
        return await self._get_tool_decision(
            instructions=(
                f"The caller is confirming tracking ID {current_tracking_id}. "
                "Call confirm_tracking_id only for a clear yes. Call reject_tracking_id "
                "for a clear no without a replacement. If the caller corrects any "
                "letter or digit, call replace_tracking_id with the complete corrected "
                "tracking ID. Understand corrections such as 'B as in Bravo, not D'. "
                "If the caller asks what tracking ID is saved, call provide_tracking_id. "
                "If they ask for the customer name, customer number, delivery status, "
                "or delivery time, call provide_delivery_detail. Answer their direct "
                "delivery question before returning to confirmation. "
                "If the answer is unclear or is a question, respond briefly and ask "
                "whether the current tracking ID is correct."
            ),
            transcription=transcription,
            history=history,
            tools=[
                {
                    "type": "function",
                    "name": "confirm_tracking_id",
                    "description": "Confirm that the current tracking ID is correct.",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
                {
                    "type": "function",
                    "name": "reject_tracking_id",
                    "description": (
                        "Reject the current tracking ID when no replacement was given."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
                {
                    "type": "function",
                    "name": "replace_tracking_id",
                    "description": (
                        "Replace the current tracking ID using the caller's correction."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "tracking_id": {
                                "type": "string",
                                "description": "The complete corrected tracking ID.",
                            }
                        },
                        "required": ["tracking_id"],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
                provide_tracking_id_tool(),
                provide_delivery_detail_tool(),
            ],
        )

    async def understand_slot_selection(
        self,
        transcription: str,
        available_slots: list[dict],
        history: list[dict] | None = None,
    ) -> dict:
        slot_ids = [slot["slot_id"] for slot in available_slots]
        slot_list = "\n".join(
            f"{index}. {slot['slot_id']}: {slot['label']}"
            for index, slot in enumerate(available_slots, start=1)
        )

        return await self._get_tool_decision(
            instructions=(
                "The caller is choosing from the delivery slots listed below. "
                "If they clearly select a slot by number, date, or time, call "
                "select_delivery_slot with the matching slot ID. Never select a slot "
                "that is not in the list. If they ask a question, want the options "
                "repeated, or are unclear, answer briefly without calling the tool. "
                "If they ask what tracking ID is saved, call provide_tracking_id.\n"
                "If they ask for the customer name, customer number, delivery status, "
                "or delivery time, call provide_delivery_detail.\n"
                f"Available slots:\n{slot_list}"
            ),
            transcription=transcription,
            history=history,
            tools=[
                {
                    "type": "function",
                    "name": "select_delivery_slot",
                    "description": "Select one slot from the offered delivery slots.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "slot_id": {
                                "type": "string",
                                "enum": slot_ids,
                                "description": "The selected offered slot ID.",
                            }
                        },
                        "required": ["slot_id"],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
                provide_tracking_id_tool(),
                provide_delivery_detail_tool(),
            ],
        )

    async def understand_slot_confirmation(
        self,
        transcription: str,
        selected_slot: dict,
        available_slots: list[dict],
        history: list[dict] | None = None,
    ) -> dict:
        slot_ids = [slot["slot_id"] for slot in available_slots]
        slot_list = "\n".join(
            f"{slot['slot_id']}: {slot['label']}" for slot in available_slots
        )
        single_slot_instruction = (
            "There is only one available slot. If the caller asks for another slot "
            "or asks whether other options are available, call reject_delivery_slot. "
            if len(available_slots) == 1
            else ""
        )

        return await self._get_tool_decision(
            instructions=(
                f"The caller is confirming the delivery slot {selected_slot['label']}. "
                f"{single_slot_instruction}"
                "Call confirm_delivery_slot only for a clear yes. Call reject_delivery_slot "
                "for a clear no without another choice. If they choose a different offered "
                "slot in this current statement, call change_delivery_slot. Never infer a "
                "slot from an earlier statement. If they only say they want to change, call "
                "reject_delivery_slot so the options can be presented again. If the answer "
                "asks what tracking ID is saved, call provide_tracking_id. If it "
                "asks for the customer name, customer number, delivery status, or delivery "
                "time, call provide_delivery_detail. Otherwise, if it "
                "is unclear or is a question, respond briefly and ask whether the selected "
                "slot will work.\n"
                f"Offered slots:\n{slot_list}"
            ),
            transcription=transcription,
            history=history,
            tools=[
                {
                    "type": "function",
                    "name": "confirm_delivery_slot",
                    "description": "Confirm that the selected delivery slot will work.",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
                {
                    "type": "function",
                    "name": "reject_delivery_slot",
                    "description": "Reject the selected slot without choosing another.",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
                {
                    "type": "function",
                    "name": "change_delivery_slot",
                    "description": "Change to another slot from the offered list.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "slot_id": {
                                "type": "string",
                                "enum": slot_ids,
                                "description": "The newly selected offered slot ID.",
                            }
                        },
                        "required": ["slot_id"],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
                provide_tracking_id_tool(),
                provide_delivery_detail_tool(),
            ],
        )

    async def understand_completed_request(
        self,
        transcription: str,
        current_slot: dict | None,
        tracking_id: str | None,
        history: list[dict] | None = None,
    ) -> dict:
        current_slot_label = (
            current_slot.get("label", "the current time")
            if current_slot
            else "the current time"
        )
        return await self._get_tool_decision(
            instructions=(
                f"The caller's delivery is currently scheduled for {current_slot_label}. "
                f"The saved tracking ID is {tracking_id or 'not available'}. "
                "If the caller asks for their tracking ID, call provide_tracking_id. "
                "If they ask for the customer name, customer number, delivery status, "
                "or delivery time, call provide_delivery_detail. "
                "If the caller wants to change, move, or reschedule the delivery time, "
                "call start_delivery_reschedule. This includes requests that name a new "
                "time, such as 'change it to 10 AM'. Do not claim that a requested time "
                "is available yet. For questions or unrelated conversation, answer "
                "briefly without calling the tool."
            ),
            transcription=transcription,
            history=history,
            tools=[
                provide_tracking_id_tool(),
                provide_delivery_detail_tool(),
                {
                    "type": "function",
                    "name": "start_delivery_reschedule",
                    "description": (
                        "Start another delivery-slot selection for the current booking."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                        "additionalProperties": False,
                    },
                    "strict": True,
                }
            ],
        )

    async def _get_tool_decision(
        self,
        instructions: str,
        transcription: str,
        tools: list[dict],
        history: list[dict] | None = None,
    ) -> dict:
        input_messages = [
            {
                "role": message["role"],
                "content": message["content"],
            }
            for message in (history or [])[-RECENT_MESSAGE_LIMIT:]
            if message.get("role") in {"user", "assistant"}
            and message.get("content")
        ]
        current_message_is_present = (
            input_messages
            and input_messages[-1]["role"] == "user"
            and input_messages[-1]["content"].strip() == transcription.strip()
        )
        if not current_message_is_present:
            input_messages.append({"role": "user", "content": transcription})
        input_messages = input_messages[-RECENT_MESSAGE_LIMIT:]

        response = await self.client.responses.create(
            model=settings.openai_model,
            instructions=instructions,
            input=input_messages,
            tools=tools,
            tool_choice="auto",
            parallel_tool_calls=False,
        )

        for item in response.output:
            if item.type == "function_call":
                return {
                    "tool": item.name,
                    "arguments": json.loads(item.arguments),
                    "message": "",
                }

        return {
            "tool": None,
            "arguments": {},
            "message": response.output_text.strip(),
        }
