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
# "(?<!\d)(?<!\d-)": not glued to another number, but "SSN-078-05-1120" is fine
_SSN = r"(?<!\d)(?<!\d-)\d{3}-\d{2}-\d{4}(?!\d)(?!-\d)"
# 13-19 digits, optionally grouped with single spaces or dashes. Luhn is checked afterwards.
_CARD = r"(?<!\d)(?<!\d-)\d(?:[ -]?\d){12,18}(?!\d)"

PII = regex.compile(rf"(?P<email>{_EMAIL})|(?P<ssn>{_SSN})|(?P<card>{_CARD})")

# enough left context for the lookbehinds above
_CTX = 8
# what stays held when the cap forces a release; longer than any card (19 digits +
# 18 separators) or SSN, so the forced cut can't land inside one still being typed
_KEEP = 40


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


def _card_length(s: str) -> int:
    """Length of the longest Luhn-valid prefix of a card candidate, 0 if none.

    The regex is greedy, so "4242 4242 4242 4242 078-05-1120" comes in as 19 digits.
    Prefixes are only cut where a digit group ends.
    """
    ends, digits = [], 0
    for i, c in enumerate(s):
        if c.isdigit():
            digits += 1
            if 13 <= digits and (i + 1 == len(s) or not s[i + 1].isdigit()):
                ends.append(i + 1)
    for end in reversed(ends):
        if luhn_ok(s[:end]):
            return end
    return 0


def pii_spans(text: str, pos: int = 0):
    while (m := PII.search(text, pos)) is not None:
        if m.lastgroup != "card":
            yield m.start(), m.end()
            pos = m.end()
            continue
        n = _card_length(m.group())
        if n:
            yield m.start(), m.start() + n
            pos = m.start() + n
        else:
            pos = m.start() + 1  # something shorter may still start inside, e.g. an SSN


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
        return self._drain(final=False)

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
        # past the cap, release all but the last _KEEP chars no matter what: memory stays
        # bounded, at the cost of maybe missing one absurdly long run of email-ish text
        forced = not final and len(self._buf) > self.max_hold
        if final:
            hold = len(text)
        elif forced:
            hold = max(start, len(text) - _KEEP)
        else:
            hold = self._hold_point(text, start)
        out = []
        pos = start

        for s, e in pii_spans(text, start):
            if s >= hold:
                break
            if e > hold:
                if forced:
                    # a complete match across the forced cut: mask all of it
                    out += [text[pos:s], MASK]
                    pos = hold = e
                else:
                    hold = s
                break
            out += [text[pos:s], MASK]
            pos = e

        out.append(text[pos:hold])
        self._ctx = text[max(0, hold - _CTX):hold]
        self._buf = text[hold:]
        return "".join(out)


def redact_text(text: str) -> str:
    r = StreamRedactor(max_hold=len(text) + 1)
    return r.feed(text) + r.flush()
