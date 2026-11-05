"""Incremental PII redaction for token streams.

The trick is the `regex` module's partial matching: with partial=True a search
also reports a match that ran into the end of the string and *could* complete
if more text arrived. Everything before the earliest such position is final and
can be sent right away; only the tail from there on is held back. In practice
that's the last word or digit group, so the client sees text almost as fast as
the model produces it.
"""
import regex

MASK = "[REDACTED]"

_EMAIL = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}"
_SSN = r"(?<![\d-])\d{3}-\d{2}-\d{4}(?![\d-])"
# 13-19 digits, optionally grouped with single spaces or dashes. Luhn is checked afterwards.
_CARD = r"(?<![\d-])\d(?:[ -]?\d){12,18}(?!\d)"

PII = regex.compile(rf"(?P<email>{_EMAIL})|(?P<ssn>{_SSN})|(?P<card>{_CARD})")

# enough left context for the lookbehinds above
_CTX = 8


def luhn_ok(s: str) -> bool:
    digits = [int(c) for c in s if c.isdigit()]
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


class StreamRedactor:
    def __init__(self, max_hold: int = 256):
        self.max_hold = max_hold
        self._buf = ""
        self._ctx = ""  # tail of what was already emitted, only used for lookbehind

    @property
    def pending(self) -> int:
        return len(self._buf)

    def feed(self, text: str) -> str:
        if not text:
            return ""
        self._buf += text
        # final=True when the tail grows past the cap: memory stays bounded, at the
        # cost of possibly missing a single absurdly long "email"
        return self._drain(final=len(self._buf) > self.max_hold)

    def flush(self) -> str:
        return self._drain(final=True)

    def _hold_point(self, text: str, start: int) -> int:
        # Earliest i where text[i:] is a match or the beginning of one, i.e. where more
        # input could still change the outcome. finditer(partial=True) isn't enough
        # here: it reports a full match further right before a partial one that
        # starts earlier ("hello4111...world.a@b.c" -> card first, email from 0 never).
        for i in range(start, len(text)):
            if PII.fullmatch(text, i, partial=True):
                return i
        return len(text)

    def _drain(self, final: bool) -> str:
        text = self._ctx + self._buf
        start = len(self._ctx)
        hold = len(text) if final else self._hold_point(text, start)
        out = []
        pos = start

        for m in PII.finditer(text, pos=start):
            if m.start() >= hold:
                break
            if m.end() > hold:
                hold = m.start()
                break
            if m.lastgroup == "card" and not luhn_ok(m.group()):
                continue
            out.append(text[pos:m.start()])
            out.append(MASK)
            pos = m.end()

        out.append(text[pos:hold])
        self._ctx = text[max(0, hold - _CTX):hold]
        self._buf = text[hold:]
        return "".join(out)


def redact_text(text: str) -> str:
    r = StreamRedactor(max_hold=len(text) + 1)
    return r.feed(text) + r.flush()
