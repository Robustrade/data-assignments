"""Time source. Everything that needs "now" takes a clock so tests are deterministic."""

import time
from datetime import UTC, datetime, timedelta


def to_iso(moment: datetime) -> str:
    """Format a datetime as ISO-8601 UTC text with millisecond precision."""
    moment = moment.astimezone(UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def parse_iso(text: str) -> datetime:
    """Parse the ISO-8601 text produced by :func:`to_iso` (tz-aware UTC)."""
    return datetime.fromisoformat(text).astimezone(UTC)


class Clock:
    """The real clock."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    def iso(self) -> str:
        return to_iso(self.now())

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class FakeClock(Clock):
    """A clock that only moves when told to. ``sleep`` advances it and is recorded."""

    def __init__(self, start: datetime | None = None):
        self._now = start or datetime(2026, 1, 1, 9, 0, 0, tzinfo=UTC)
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float = 0, **kwargs) -> datetime:
        self._now = self._now + timedelta(seconds=seconds, **kwargs)
        return self._now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.advance(seconds)
