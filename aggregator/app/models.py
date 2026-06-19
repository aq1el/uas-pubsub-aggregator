"""Model & validasi skema event (Bab 4: Naming -> setiap event punya (topic, event_id)
yang menjadi identitas unik untuk keperluan deduplication)."""
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class Event(BaseModel):
    topic: str = Field(..., min_length=1, max_length=200)
    event_id: str = Field(..., min_length=1, max_length=200)
    timestamp: str
    source: str = Field(..., min_length=1, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def validate_iso8601(cls, v: str) -> str:
        try:
            # Terima format ISO8601, termasuk yang berakhiran 'Z'
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("timestamp harus berformat ISO8601, contoh: 2026-06-19T10:00:00Z") from exc
        return v
