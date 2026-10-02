from dataclasses import asdict, dataclass, field


class ConversationStage:
    COLLECTING_TRACKING_ID = "collecting_tracking_id"
    CONFIRMING_TRACKING_ID = "confirming_tracking_id"
    OFFERING_SLOTS = "offering_slots"
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

    def to_dict(self) -> dict:
        return asdict(self)
