
from typing import Optional


def classify_record(
    previous_hash: Optional[str],
    current_hash: str,
) -> str:
    if previous_hash is None:
        return "new"

    if previous_hash == current_hash:
        return "unchanged"

    return "changed"
