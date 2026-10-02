import json
from collections.abc import AsyncIterator

from app.clients.openai import create_openai_client
from app.config.settings import settings


class LLMService:
    def __init__(self) -> None:
        self.client = create_openai_client()

    async def stream_response(self, transcription: str) -> AsyncIterator[str]:
        stream = await self.client.responses.create(
            model=settings.openai_model,
            instructions=(
                "You are a helpful voice assistant. "
                "Answer clearly and keep the response brief."
            ),
            input=transcription,
            stream=True,
        )

        async for event in stream:
            if event.type == "response.output_text.delta":
                yield event.delta

    async def understand_tracking_id(
        self,
        transcription: str,
        current_tracking_id: str | None = None,
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
    ) -> dict:
        return await self._get_tool_decision(
            instructions=(
                f"The caller is confirming tracking ID {current_tracking_id}. "
                "Call confirm_tracking_id only for a clear yes. Call reject_tracking_id "
                "for a clear no without a replacement. If the caller corrects any "
                "letter or digit, call replace_tracking_id with the complete corrected "
                "tracking ID. Understand corrections such as 'B as in Bravo, not D'. "
                "If the answer is unclear or is a question, respond briefly and ask "
                "whether the current tracking ID is correct."
            ),
            transcription=transcription,
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
            ],
        )

    async def _get_tool_decision(
        self,
        instructions: str,
        transcription: str,
        tools: list[dict],
    ) -> dict:
        response = await self.client.responses.create(
            model=settings.openai_model,
            instructions=instructions,
            input=transcription,
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
