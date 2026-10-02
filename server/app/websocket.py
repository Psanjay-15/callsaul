from fastapi import APIRouter, WebSocket, WebSocketDisconnect


router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    await websocket.send_json(
        {
            "type": "connected",
            "message": "WebSocket connection established.",
        }
    )

    try:
        while True:
            message = await websocket.receive_json()
            message_type = message.get("type")

            if message_type == "ping":
                await websocket.send_json({"type": "pong"})
                continue

            if message_type == "message":
                text = message.get("text")
                if not isinstance(text, str) or not text.strip() or len(text.strip()) > 300:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "message": "Messages must contain between 1 and 300 characters.",
                        }
                    )
                    continue

                await websocket.send_json(
                    {
                        "type": "message",
                        "message": f"Server received: {text.strip()}",
                    }
                )
                continue

            if message_type == "stop":
                await websocket.close(code=1000)
                return

            await websocket.send_json(
                {"type": "error", "message": "Unsupported WebSocket message type."}
            )
    except WebSocketDisconnect:
        return
