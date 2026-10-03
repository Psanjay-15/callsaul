from dataclasses import asdict, dataclass, field
from dataclasses import fields


class ConversationStage:
    COLLECTING_TRACKING_ID = "collecting_tracking_id"
    CONFIRMING_TRACKING_ID = "confirming_tracking_id"
    OFFERING_SLOTS = "offering_slots"
    NO_SLOTS = "no_slots"
    CONFIRMING_SLOT = "confirming_slot"
    BOOKING = "booking"
    COMPLETED = "completed"


@dataclass
class ConversationState:
    stage: str = ConversationStage.COLLECTING_TRACKING_ID
    tracking_id: str | None = None
    tracking_id_confirmed: bool = False
    available_slots: list[dict] = field(default_factory=list)
    selected_slot: dict | None = None
    slot_confirmed: bool = False
    booking_idempotency_key: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict | None) -> "ConversationState":
        if not data:
            return cls()

        allowed_fields = {item.name for item in fields(cls)}
        values = {
            key: value
            for key, value in data.items()
            if key in allowed_fields
        }
        return cls(**values)
