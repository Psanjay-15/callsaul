import asyncio
import logging

from app.config.settings import settings


logger = logging.getLogger(__name__)

_call_count = 0


class FakeBackendError(RuntimeError):
    pass


async def simulate_backend_behavior(operation: str) -> None:
    behavior = get_next_behavior()
    logger.info("Fake backend behavior for %s: %s", operation, behavior)

    if behavior == "slow":
        await asyncio.sleep(settings.fake_backend_slow_seconds)
    elif behavior == "fail":
        await asyncio.sleep(0.5)
        raise FakeBackendError(f"Fake backend failed during {operation}")


def get_next_behavior() -> str:
    global _call_count

    mode = settings.fake_backend_mode.lower()
    if mode in {"normal", "slow", "fail"}:
        return mode

    if mode == "cycle":
        behaviors = ["normal", "slow", "fail"]
        behavior = behaviors[_call_count % len(behaviors)]
        _call_count += 1
        return behavior

    logger.warning("Unknown FAKE_BACKEND_MODE '%s'; using normal", mode)
    return "normal"
