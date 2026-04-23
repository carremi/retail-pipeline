"""Helpers to inject realistic dirtiness into simulated data."""
import random
from datetime import datetime, timedelta, timezone


def maybe_none(value, probability: float = 0.05):
    """Return None with given probability."""
    return None if random.random() < probability else value


def maybe_dirty_sku(sku: str, probability: float = 0.08) -> str:
    """Occasionally add whitespace or case inconsistencies to a SKU."""
    if random.random() >= probability:
        return sku
    choice = random.choice(["leading_space", "trailing_space", "lower", "upper"])
    if choice == "leading_space":
        return f" {sku}"
    if choice == "trailing_space":
        return f"{sku} "
    if choice == "lower":
        return sku.lower()
    if choice == "upper":
        return sku.upper()
    return sku


def random_date_in_window(days_back: int = 30) -> datetime:
    """Pick a random datetime within the past `days_back` days (UTC)."""
    now = datetime.now(timezone.utc)
    delta_seconds = random.randint(0, days_back * 24 * 3600)
    return now - timedelta(seconds=delta_seconds)


def should_duplicate(probability: float = 0.02) -> bool:
    """Roll for whether to emit a duplicate row."""
    return random.random() < probability
