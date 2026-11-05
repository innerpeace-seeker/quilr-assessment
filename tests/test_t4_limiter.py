import asyncio
import sqlite3

import pytest

from task4_router.limiter import Denied, Reservation, TokenLimiter


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def lim(tmp_path, clock):
    return TokenLimiter(tmp_path / "rl.db", limit=50_000, window_s=60, clock=clock)


async def test_under_and_at_limit(lim):
    assert isinstance(await lim.reserve("k1", 30_000), Reservation)
    assert isinstance(await lim.reserve("k1", 20_000), Reservation)  # exactly 50k
    assert await lim.usage("k1") == 50_000


async def test_over_limit_denied_with_retry_after(lim, clock):
    await lim.reserve("k1", 20_000)
    clock.t += 10
    await lim.reserve("k1", 25_000)
    clock.t += 5
    d = await lim.reserve("k1", 10_000)
    assert isinstance(d, Denied)
    assert d.used == 45_000 and d.limit == 50_000
    # the first 20k drop out at t0+60, we're at t0+15
    assert d.retry_after_s == pytest.approx(45.0)


async def test_retry_after_may_need_several_rows_to_expire(lim, clock):
    for _ in range(5):
        await lim.reserve("k1", 10_000)
        clock.t += 1
    d = await lim.reserve("k1", 25_000)
    # need 25k free: rows at t0, t0+1, t0+2 must expire -> t0+62, now is t0+5
    assert d.retry_after_s == pytest.approx(57.0)


async def test_window_slides_and_old_rows_are_deleted(lim, clock, tmp_path):
    await lim.reserve("k1", 50_000)
    assert isinstance(await lim.reserve("k1", 1), Denied)
    clock.t += 60.001
    assert isinstance(await lim.reserve("k1", 50_000), Reservation)
    with sqlite3.connect(tmp_path / "rl.db") as db:
        assert db.execute("select count(*) from usage").fetchone()[0] == 1


async def test_tenants_are_isolated(lim):
    await lim.reserve("k1", 50_000)
    assert isinstance(await lim.reserve("k2", 50_000), Reservation)
    assert isinstance(await lim.reserve("k1", 1), Denied)


async def test_raw_keys_not_stored(lim, tmp_path):
    await lim.reserve("sk-live-supersecret", 10)
    raw = (tmp_path / "rl.db").read_bytes()
    assert b"supersecret" not in raw


async def test_settle_adjusts_to_actual(lim):
    r = await lim.reserve("k1", 40_000)
    await lim.settle(r.id, 1_200)
    assert await lim.usage("k1") == 1_200
    assert isinstance(await lim.reserve("k1", 48_000), Reservation)


async def test_settle_can_exceed_reservation(lim):
    r = await lim.reserve("k1", 100)
    await lim.settle(r.id, 60_000)  # the provider used more than we guessed; count what really happened
    assert await lim.usage("k1") == 60_000
    assert isinstance(await lim.reserve("k1", 1), Denied)


async def test_release_frees_tokens(lim):
    r = await lim.reserve("k1", 50_000)
    await lim.release(r.id)
    assert await lim.usage("k1") == 0


async def test_request_bigger_than_limit(lim):
    d = await lim.reserve("k1", 50_001)
    assert isinstance(d, Denied) and d.retry_after_s is None


async def test_concurrent_reservations_never_overshoot(lim):
    results = await asyncio.gather(*[lim.reserve("k1", 5_000) for _ in range(20)])
    assert sum(isinstance(r, Reservation) for r in results) == 10
    assert await lim.usage("k1") == 50_000


async def test_two_limiters_share_the_file(tmp_path, clock):
    a = TokenLimiter(tmp_path / "rl.db", limit=50_000, window_s=60, clock=clock)
    b = TokenLimiter(tmp_path / "rl.db", limit=50_000, window_s=60, clock=clock)
    results = await asyncio.gather(*[(a if i % 2 else b).reserve("k1", 5_000) for i in range(20)])
    assert sum(isinstance(r, Reservation) for r in results) == 10


async def test_persists_across_restart(tmp_path, clock):
    a = TokenLimiter(tmp_path / "rl.db", limit=50_000, window_s=60, clock=clock)
    await a.reserve("k1", 49_000)
    b = TokenLimiter(tmp_path / "rl.db", limit=50_000, window_s=60, clock=clock)
    assert await b.usage("k1") == 49_000
    assert isinstance(await b.reserve("k1", 2_000), Denied)


async def test_negative_tokens_rejected(lim):
    with pytest.raises(ValueError):
        await lim.reserve("k1", -5)


async def test_cancel_while_waiting_for_lock_does_not_leak(tmp_path):
    # to_thread can't stop the worker: if it commits after we were cancelled, undo it
    lim = TokenLimiter(tmp_path / "rl.db", limit=50_000, window_s=60)
    blocker = sqlite3.connect(tmp_path / "rl.db", isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    t = asyncio.create_task(lim.reserve("k1", 5_000))
    await asyncio.sleep(0.2)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    blocker.execute("COMMIT")
    blocker.close()
    await asyncio.sleep(1.0)  # let the worker thread commit and the undo run
    assert await lim.usage("k1") == 0
