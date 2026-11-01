"""How a comparison prints: to the console, and as plain text for an assertion message or a log."""

import io
from typing import Any

from rich.console import Console


def show(renderable: Any) -> None:
    """Print to the terminal or notebook. Long lines are left whole, so a diff line keeps its marker when it wraps."""
    Console().print(renderable, soft_wrap=True)


def plain_text(renderable: Any) -> str:
    buffer = io.StringIO()
    Console(file=buffer, width=120, color_system=None, force_terminal=False, force_jupyter=False).print(renderable, soft_wrap=True)
    return buffer.getvalue()


def cell(value: Any) -> str:
    """A value as a report shows it. A string keeps its quotes, so a stray space or a case change is visible."""
    if value is None:
        return 'null'
    if isinstance(value, str):
        return repr(value)
    return str(value)


def count(value: int) -> str:
    return f'{value:,}'


def counted(value: int, noun: str) -> str:
    return f'{value:,} {noun}' if value == 1 else f'{value:,} {noun}s'
