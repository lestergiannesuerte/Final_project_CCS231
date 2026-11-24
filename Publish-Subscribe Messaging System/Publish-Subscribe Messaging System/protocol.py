import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict

ANSI_COLORS = {
    "reset": "\033[0m",
    "bold": "\033[1m",
    "dim": "\033[2m",
    "cyan": "\033[36m",
    "yellow": "\033[33m",
    "green": "\033[32m",
    "magenta": "\033[35m",
    "red": "\033[31m",
    "blue": "\033[34m",
}


def colors_enabled(force_disable: bool = False) -> bool:
    if force_disable or os.environ.get("NO_COLOR"):
        return False
    return sys.stdout.isatty() or os.environ.get("FORCE_COLOR") == "1"


def paint(text: str, *styles: str, enabled: bool = True) -> str:
    if not enabled or not styles:
        return text
    prefix = "".join(ANSI_COLORS.get(s, "") for s in styles)
    return f"{prefix}{text}{ANSI_COLORS['reset']}"


def time_tag() -> str:
    return datetime.now().strftime("%H:%M:%S")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def encode_frame(obj: Dict[str, Any]) -> bytes:
    """Serialize a protocol frame as a newline-delimited UTF-8 JSON object."""
    return (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")


def topic_matches(pattern: str, topic: str) -> bool:
    """
    Match a hierarchical '/' topic against a subscription pattern:
      '+' : matches exactly one topic segment (e.g., store/+/low_stock)
      '#' : matches zero or more remaining segments (must be the final segment)
    """
    p_parts = pattern.split("/")
    t_parts = topic.split("/")

    for i, p in enumerate(p_parts):
        if p == "#":
            return True
        if i >= len(t_parts):
            return False
        if p != "+" and p != t_parts[i]:
            return False

    return len(t_parts) == len(p_parts)


def validate_pattern(pattern: str) -> bool:
    if not pattern:
        return False
    parts = pattern.split("/")
    for idx, part in enumerate(parts):
        if "#" in part and (part != "#" or idx != len(parts) - 1):
            return False
        if "+" in part and part != "+":
            return False
    return True