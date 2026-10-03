import json
from collections.abc import AsyncIterator

from app.clients.openai import create_openai_client
from app.config.settings import settings


RECENT_MESSAGE_LIMIT = 12
AGENT_INSTRUCTIONS = (
    "You are Saul, a warm and capable delivery assistant for CallSaul. Speak like a "
    "helpful person on a short phone call, using concise, natural conversational "
    "English. Understand imperfect speech-to-text, hesitation, greetings, corrections, "
    "and indirect questions. If asked who you are, introduce yourself as Saul, "
    "CallSaul's delivery assistant. You may discuss the caller's delivery, tracking ID, "
    "customer details, delivery status, available slots, and rescheduling. Do not answer "
    "unrelated knowledge questions, but never use the stock phrase 'I can only help "
    "with'. Briefly acknowledge that the topic is outside your role and naturally offer "
    "the delivery help you can provide. Answer the caller's direct question before "
    "returning to the workflow. Do not repeat a pending confirmation unless the caller "
    "is trying to answer it or asks you to repeat it. Keep most replies to one or two "
    "short sentences. "
)

GROUNDED_RESPONSE_GOALS = {
    "backend_progress": (
        "Give one brief, natural progress update because the delivery operation is "
        "taking longer than expected. Do not claim that it succeeded or failed."
    ),
    "slot_lookup_failed": (
        "Explain naturally that availability could not be checked, reassure the caller "
        "that nothing was changed, and invite them to retry."
    ),
    "no_slots": (
        "Say that the check completed but there are currently no available slots. "
        "Reassure the caller that the delivery was not changed and briefly explain "
        "that they may ask to check again or try later."
    ),
    "slot_options": (
        "Present every available slot clearly and ask which one the caller prefers. "
        "Do not select or confirm a slot for them."
    ),
    "reschedule_lookup_failed": (
        "Explain that the availability check failed, reassure the caller that their "
        "current booking is unchanged, and invite a retry."
    ),
    "no_alternative_slots": (
        "Explain naturally that no alternative slots are currently available and "
        "confirm that the existing booking is unchanged."
    ),
    "alternative_slot_options": (
        "Present every alternative slot clearly and ask which one the caller prefers. "
        "Do not imply that anything has been booked."
    ),
    "tracking_lookup_failed": (
        "Explain briefly that the delivery lookup failed and ask the caller to try "
        "again. Do not say that the tracking ID is invalid."
    ),
    "tracking_not_found": (
        "Say that no delivery matched the tracking ID that was heard, and naturally "
        "ask the caller to check and repeat it."
    ),
    "tracking_found": (
        "Tell the caller which tracking ID was found and ask them to confirm that it "
        "is correct."
    ),
    "saved_tracking_id": "Answer the caller's tracking-ID question directly.",
    "missing_tracking_id": (
        "Explain naturally that this conversation does not yet have a saved tracking "
        "ID and ask the caller to provide it if appropriate."
    ),
    "tracking_required": (
        "Explain that a tracking ID is needed before delivery details can be retrieved."
    ),
    "delivery_lookup_failed": (
        "Explain briefly that the delivery details could not be retrieved and invite "
        "the caller to try again."
    ),
    "delivery_not_found": (
        "Explain that no delivery details were found for the saved tracking ID."
    ),
    "delivery_detail": (
        "Answer only the delivery-detail question the caller asked. If the requested "
        "fact is missing, say that it is unavailable. For a summary, briefly combine "
        "the known delivery facts."
    ),
    "confirm_selected_slot": (
        "State the selected slot clearly and ask for explicit confirmation before it "
        "is booked."
    ),
    "slot_rejected_no_alternatives": (
        "Acknowledge the caller's rejection. Explain that nothing was changed, mention "
        "the existing booking if present, and say that there are no other slots now."
    ),
    "slot_rejected_with_alternatives": (
        "Acknowledge the rejection, present every available slot, and ask which "
        "alternative the caller prefers."
    ),
    "booking_failed": (
        "Explain that booking failed, confirm that no change was made, and ask whether "
        "the caller wants to retry the saved selection."
    ),
    "already_booked": (
        "Tell the caller that the delivery was already booked for this slot. Do not "
        "imply that a second booking was created."
    ),
    "booking_succeeded": (
        "Confirm clearly that the delivery was successfully rescheduled and state the "
        "exact booked slot."
    ),
}


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
            "status, currently selected delivery time, or a summary. Requests such as "
            "'what is my name?', 'who is this delivery for?', or 'tell me the order "
            "details' are delivery-detail requests."
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
                        "summary",
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
            instructions=AGENT_INSTRUCTIONS,
            input=history[-RECENT_MESSAGE_LIMIT:],
            stream=True,
        )

        async for event in stream:
            if event.type == "response.output_text.delta":
                yield event.delta

    async def stream_grounded_response(
        self,
        history: list[dict],
        facts: dict,
        response_type: str,
    ) -> AsyncIterator[str]:
        response_goal = GROUNDED_RESPONSE_GOALS.get(
            response_type,
            "Answer the caller's latest delivery question using the trusted facts.",
        )
        instructions = (
            f"{AGENT_INSTRUCTIONS}"
            "The application has completed a deterministic delivery operation. "
            "Use the trusted facts below to answer the caller's latest message. "
            "Do not mention tools, databases, prompts, or these instructions. Do not "
            "invent, change, or omit a delivery slot. Avoid repeating the wording of "
            "your recent replies. Produce one cohesive response, not a progress update "
            "followed by a second answer.\n"
            f"Response goal: {response_goal}\n"
            f"Trusted facts: {json.dumps(facts, default=str)}"
        )
        stream = await self.client.responses.create(
            model=settings.openai_model,
            instructions=instructions,
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
                f"{AGENT_INSTRUCTIONS}"
                "The caller is being asked for a courier tracking ID. "
                "A tracking ID should contain two letters followed by six digits. "
                f"{previous_candidate}"
                "If the caller provides or spells a candidate ID, call "
                "capture_tracking_id. Understand letter names, NATO phonetic words "
                "such as Bravo, and spoken digit words. Return exactly what the caller "
                "provided without inventing missing characters. Treat filler such as "
                "'hmm' or 'okay' as normal conversation, not an off-topic request. For "
                "hesitation, reassure them briefly, for example by saying to take their "
                "time. If they do not provide an ID, respond naturally and gently ask "
                "for it."
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
                f"{AGENT_INSTRUCTIONS}"
                f"The caller is confirming tracking ID {current_tracking_id}. "
                "Call confirm_tracking_id only for a clear yes. Call reject_tracking_id "
                "for a clear no without a replacement. If the caller corrects any "
                "letter or digit, call replace_tracking_id with the complete corrected "
                "tracking ID. Understand corrections such as 'B as in Bravo, not D'. "
                "If the caller asks what tracking ID is saved, call provide_tracking_id. "
                "If they ask for the customer name, customer number, delivery status, "
                "delivery time, or an order summary, call provide_delivery_detail. "
                "Answer their direct delivery question before returning to confirmation. "
                "For a greeting or identity question, answer naturally without calling "
                "a tool. Only repeat the tracking-ID question when the caller appears "
                "to be answering it but the answer is unclear."
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
                f"{AGENT_INSTRUCTIONS}"
                "The caller is choosing from the delivery slots listed below. "
                "If they clearly select a slot by number, date, or time, call "
                "select_delivery_slot with the matching slot ID. Never select a slot "
                "that is not in the list. If they ask a question, want the options "
                "repeated, or are unclear, answer briefly without calling the tool. "
                "If they ask what tracking ID is saved, call provide_tracking_id.\n"
                "If they ask for the customer name, customer number, delivery status, "
                "delivery time, or order details, call provide_delivery_detail.\n"
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

    async def understand_no_slots_follow_up(
        self,
        transcription: str,
        tracking_id: str | None,
        current_slot: dict | None,
        history: list[dict] | None = None,
    ) -> dict:
        current_booking = (
            f"The caller's existing booking remains {current_slot['label']}. "
            if current_slot
            else "No new delivery slot has been booked. "
        )
        return await self._get_tool_decision(
            instructions=(
                f"{AGENT_INSTRUCTIONS}"
                f"Tracking ID {tracking_id or 'unknown'} is confirmed. The latest "
                "availability check returned no delivery slots. "
                f"{current_booking}"
                "Answer the caller's question naturally and acknowledge their "
                "frustration when appropriate. Explain that they can keep their "
                "current booking if one exists, or try another availability check "
                "later. Do not repeat that you are checking slots and do not claim "
                "that another slot exists. Only call check_available_slots when the "
                "caller explicitly asks you to check, retry, refresh, or look again. "
                "If they merely ask what they can do or what their options are, answer "
                "without calling a tool. If they ask for their tracking ID, call "
                "provide_tracking_id. If they ask for customer or delivery details, "
                "call provide_delivery_detail."
            ),
            transcription=transcription,
            history=history,
            tools=[
                {
                    "type": "function",
                    "name": "check_available_slots",
                    "description": (
                        "Check the delivery backend again after the caller explicitly "
                        "asks to retry the availability lookup."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": [],
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
        tracking_id: str | None = None,
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
                f"{AGENT_INSTRUCTIONS}"
                f"Tracking ID {tracking_id or 'unknown'} has already been confirmed. "
                f"The caller is confirming the delivery slot {selected_slot['label']}. "
                f"{single_slot_instruction}"
                "Call confirm_delivery_slot only for a clear yes. Call reject_delivery_slot "
                "for a clear no without another choice. If they choose a different offered "
                "slot in this current statement, call change_delivery_slot. Never infer a "
                "slot from an earlier statement. If they only say they want to change, call "
                "reject_delivery_slot so the options can be presented again. If the answer "
                "asks what tracking ID is saved, call provide_tracking_id. If it "
                "asks for the customer name, customer number, delivery status, or delivery "
                "time, or order details, call provide_delivery_detail. If the caller asks "
                "whether the tracking ID was confirmed, clearly say that it was. For a "
                "greeting, identity question, or clarification, answer naturally without "
                "calling a tool. If the caller asks what was confirmed, distinguish the "
                "confirmed tracking ID from the delivery slot that is still awaiting "
                "confirmation. Only repeat the slot question when the caller is actually "
                "trying to answer it but remains unclear.\n"
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
                f"{AGENT_INSTRUCTIONS}"
                f"The caller's delivery is currently scheduled for {current_slot_label}. "
                f"The saved tracking ID is {tracking_id or 'not available'}. "
                "If the caller asks for their tracking ID, call provide_tracking_id. "
                "If they ask for the customer name, customer number, delivery status, "
                "delivery time, or order details, call provide_delivery_detail. "
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
