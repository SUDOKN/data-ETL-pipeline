from datetime import datetime, timezone


def get_current_time() -> datetime:
    """
    Returns the current time in UTC.
    """
    return datetime.now(timezone.utc)
