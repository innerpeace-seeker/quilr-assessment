import pytest

from task3_pii_stream.redact import StreamRedactor, redact_text

R = "[REDACTED]"
EMAIL = "jane.doe+billing@corp.example.com"
SSN = "123-45-6789"
CARD = "4111 1111 1111 1111"  # Luhn-valid test number
SENTENCE = f"Contact {EMAIL} or use SSN {SSN}; card {CARD}. Thanks!"
PII = [EMAIL, SSN, CARD, "4111", "jane.doe"]


@pytest.mark.parametrize("text,expected", [
    (f"mail me at {EMAIL}.", f"mail me at {R}."),
    (f"ssn: {SSN}", f"ssn: {R}"),
    (f"card {CARD} ok", f"card {R} ok"),
    ("card 4111-1111-1111-1111 ok", f"card {R} ok"),
    ("card 4111111111111111 ok", f"card {R} ok"),
    ("amex 378282246310005", f"amex {R}"),
])
def test_one_shot(text, expected):
    assert redact_text(text) == expected


@pytest.mark.parametrize("text", [
    "order 1234567812345678 shipped",   # 16 digits, fails Luhn
    "call 555-1234 now",
    "version 1.2.3 released",
    "ratio 123-45-67890",                # too many digits for an SSN
    "id 1123-45-6789",                   # SSN shape preceded by another digit
    "no at sign here.com",
])
def test_lookalikes_untouched(text):
    assert redact_text(text) == text


def test_sentence_redaction():
    assert redact_text(SENTENCE) == f"Contact {R} or use SSN {R}; card {R}. Thanks!"


def run(pieces, **kw):
    r = StreamRedactor(**kw)
    emitted = [r.feed(p) for p in pieces]
    emitted.append(r.flush())
    return emitted


@pytest.mark.parametrize("cut", range(1, len(SENTENCE)))
def test_every_two_way_split(cut):
    out = run([SENTENCE[:cut], SENTENCE[cut:]])
    assert "".join(out) == redact_text(SENTENCE)
    assert not any(p in "".join(out) for p in PII)


def test_char_by_char():
    out = run(list(SENTENCE))
    joined = "".join(out)
    assert joined == redact_text(SENTENCE)
    for p in PII:
        assert p not in joined


@pytest.mark.parametrize("size", [2, 3, 5, 7, 11])
def test_fixed_size_chunks(size):
    text = SENTENCE * 3
    out = run([text[i:i + size] for i in range(0, len(text), size)])
    assert "".join(out) == redact_text(text)


def test_safe_text_is_released_immediately():
    r = StreamRedactor()
    assert r.feed("Hello there, ") == "Hello there, "
    # a trailing word might be the start of an email, so only that word is held
    assert r.feed("how are you") == "how are "
    assert r.feed(" today") == "you "
    assert r.flush() == "today"


def test_digits_held_until_resolved():
    r = StreamRedactor()
    assert r.feed("ssn 123-4") == "ssn "
    assert r.feed("5-6789") == ""            # complete but could still grow
    assert r.feed(" ok") == R + " "         # "ok" could still be the start of an email
    assert r.flush() == "ok"


def test_email_that_keeps_growing():
    r = StreamRedactor()
    out = r.feed("write to a@b.example.") + r.feed("com.") + r.feed("au please") + r.flush()
    assert out == f"write to {R} please"


def test_buffer_is_bounded():
    r = StreamRedactor(max_hold=64)
    total = ""
    for _ in range(1000):
        total += r.feed("a" * 10)
        assert r.pending <= 64
    total += r.flush()
    assert total == "a" * 10_000


def test_lookbehind_across_emitted_boundary():
    r = StreamRedactor()
    out = r.feed("id 1") + r.feed("x")  # "1x" gets emitted...
    out += r.feed(" 1") + r.feed("123-45-6789 end") + r.flush()
    assert "1123-45-6789" in out        # digit run, not an SSN

    r = StreamRedactor()
    out = r.feed("id 9 ") + r.feed("123-45-6789 end") + r.flush()
    assert out == f"id 9 {R} end"


def test_random_chunking_matches_one_shot():
    import random
    rng = random.Random(1234)
    parts = [EMAIL, SSN, CARD, "4111-1111-1111-1111", "1234567812345678", "555-1234", "a@b.co",
             "hello", "world.", "x.y@z.org.", "  ", "\n", "123", "-", "45", "6789", "tel:", ",",
             "4242 4242 4242 4242", "078-05-1120", "12/26", "Visa-", "SSN-", "1"]
    for _ in range(300):
        text = " ".join(rng.choice(parts) for _ in range(rng.randint(1, 12)))
        if rng.random() < 0.3:
            text = "".join(text.split(" "))  # glue everything together for nastier boundaries
        cuts = sorted(rng.sample(range(1, len(text)), k=min(len(text) - 1, rng.randint(0, 8)))) if len(text) > 1 else []
        pieces = [text[a:b] for a, b in zip([0] + cuts, cuts + [len(text)])]
        assert "".join(run(pieces)) == redact_text(text), (text, pieces)


# --- from final review ---

@pytest.mark.parametrize("text,expected", [
    ("card 4242 4242 4242 4242 078-05-1120 ok", f"card {R} {R} ok"),
    ("4242424242424242 078-05-1120", f"{R} {R}"),
    ("card 4242 4242 4242 4242 12/26", f"card {R} 12/26"),
    ("pay 1 4242 4242 4242 4242 now", f"pay 1 {R} now"),
    ("Visa-4242424242424242", f"Visa-{R}"),
    ("SSN-078-05-1120", f"SSN-{R}"),
])
def test_card_next_to_other_numbers(text, expected):
    assert redact_text(text) == expected


def test_cap_never_releases_part_of_a_card():
    r = StreamRedactor()
    out = r.feed("x" * 250 + "4242 4242") + r.feed(" 4242 4242 done") + r.flush()
    assert "4242" not in out
    assert out.endswith(f"{R} done")


def test_cap_still_bounds_memory_with_digits():
    r = StreamRedactor(max_hold=64)
    for _ in range(500):
        r.feed("1 2 3 4 5 6 7 8 9 ")
        assert r.pending <= 64
