from __future__ import annotations

import re

_SPECIAL_LABEL_MAP = {
    "Inquiry": "cpi",
}


def entity_type_for_label(label: str | None) -> str:
    if not label:
        return "unknown"
    if label in _SPECIAL_LABEL_MAP:
        return _SPECIAL_LABEL_MAP[label]

    words = re.findall(r"[A-Z]+(?=[A-Z][a-z]|$)|[A-Z]?[a-z]+|[0-9]+", label)
    if not words:
        return label.lower()
    return words[0].lower() + "".join(word.capitalize() for word in words[1:])
