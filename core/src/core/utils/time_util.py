from datetime import datetime, timezone


def get_current_time() -> datetime:
    """
    Returns the current time in UTC.
    """
    return datetime.now(timezone.utc)


def get_timestamp_str(timestamp: datetime) -> str:
    return timestamp.strftime("%Y%m%d_%H%M%S")
