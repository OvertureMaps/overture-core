"""Terminal banner for the ``overture-datasets`` CLI, in Overture brand colors."""

from __future__ import annotations

from rich.color import Color
from rich.style import Style
from rich.text import Text

# Brand palette: blue-violet -> teal -> light cyan.
_GRADIENT = ((0x40, 0x51, 0xCC), (0x0E, 0xC1, 0xBD), (0x4E, 0xDA, 0xD8))

_LETTERS = {
    "O": [" ██████╗ ", "██╔═══██╗", "██║   ██║", "██║   ██║", "╚██████╔╝", " ╚═════╝ "],
    "V": ["██╗   ██╗", "██║   ██║", "██║   ██║", "╚██╗ ██╔╝", " ╚████╔╝ ", "  ╚═══╝  "],
    "E": ["███████╗", "██╔════╝", "█████╗  ", "██╔══╝  ", "███████╗", "╚══════╝"],
    "R": ["██████╗ ", "██╔══██╗", "██████╔╝", "██╔══██╗", "██║  ██║", "╚═╝  ╚═╝"],
    "T": ["████████╗", "╚══██╔══╝", "   ██║   ", "   ██║   ", "   ██║   ", "   ╚═╝   "],
    "U": ["██╗   ██╗", "██║   ██║", "██║   ██║", "██║   ██║", "╚██████╔╝", " ╚═════╝ "],
    "D": ["██████╗ ", "██╔══██╗", "██║  ██║", "██║  ██║", "██████╔╝", "╚═════╝ "],
    "A": [" █████╗ ", "██╔══██╗", "███████║", "██╔══██║", "██║  ██║", "╚═╝  ╚═╝"],
    "S": ["███████╗", "██╔════╝", "███████╗", "╚════██║", "███████║", "╚══════╝"],
}


def _word(word: str) -> list[str]:
    glyphs = []
    for ch in word:
        rows = _LETTERS[ch]
        width = max(len(r) for r in rows)
        glyphs.append([r.ljust(width) for r in rows])
    return ["".join(g[i] for g in glyphs).rstrip() for i in range(len(glyphs[0]))]


def _blend(t: float) -> Color:
    pos = t * (len(_GRADIENT) - 1)
    i = min(int(pos), len(_GRADIENT) - 2)
    frac = pos - i
    a, b = _GRADIENT[i], _GRADIENT[i + 1]
    return Color.from_rgb(*(round(a[k] + (b[k] - a[k]) * frac) for k in range(3)))


def render_banner() -> Text:
    """The banner with a left-to-right brand gradient.

    Rich downgrades the colors for limited terminals and drops them entirely
    when color is off, so callers just print the result.
    """
    art = _word("OVERTURE") + [""] + _word("DATASETS")
    width = max(len(r) for r in art)
    text = Text()
    for row in art:
        for x, ch in enumerate(row):
            style = Style(color=_blend(x / max(width - 1, 1))) if ch != " " else None
            text.append(ch, style=style)
        text.append("\n")
    text.rstrip()
    return text
