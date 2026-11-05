"""Token-aware sliding window limiter backed by an on-disk sqlite file.

Each request leaves a row (tenant, ts, tokens). A tenant's usage is the sum of its
rows newer than now - window. Check-and-insert happens inside BEGIN IMMEDIATE, which
takes sqlite's write lock up front, so two requests (or two processes sharing the
file) can't both see 45k used and both add 5k.
"""
import asyncio
import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS usage (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant  TEXT    NOT NULL,
    ts      REAL    NOT NULL,
    tokens  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS usage_tenant_ts ON usage (tenant, ts);
CREATE INDEX IF NOT EXISTS usage_ts ON usage (ts);
"""

SWEEP_EVERY = 200


@dataclass(frozen=True)
class Reservation:
    id: int
    tokens: int


@dataclass(frozen=True)
class Denied:
    retry_after_s: float | None  # None: the request can never fit
    used: int
    limit: int


def tenant_key(api_key: str) -> str:
    # don't keep raw API keys lying around in a db file
    return hashlib.sha256(api_key.encode()).hexdigest()


class TokenLimiter:
    def __init__(self, db_path, limit=50_000, window_s=60.0, clock=time.time):
        self.db_path = str(Path(db_path))
        self.limit = limit
        self.window = window_s
        self.clock = clock
        self._calls = 0
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)

    def _connect(self):
        # one short-lived connection per operation: sqlite connections aren't meant to be
        # shared across threads, and opening one is cheap compared to an LLM call
        db = sqlite3.connect(self.db_path, timeout=5.0, isolation_level=None)
        db.execute("PRAGMA synchronous=NORMAL")
        return db

    async def reserve(self, api_key: str, tokens: int) -> Reservation | Denied:
        if tokens < 0:
            raise ValueError("tokens must be >= 0")
        self._calls += 1
        sweep = self._calls % SWEEP_EVERY == 0
        fut = asyncio.ensure_future(asyncio.to_thread(self._reserve, tenant_key(api_key), tokens, sweep))
        try:
            return await asyncio.shield(fut)
        except asyncio.CancelledError:
            # the worker thread can't be stopped and may still commit (e.g. it was waiting
            # on the write lock); nobody will settle that row, so drop it when it lands
            fut.add_done_callback(self._undo)
            raise

    def _undo(self, fut):
        if fut.cancelled() or fut.exception() is not None:
            return
        res = fut.result()
        if isinstance(res, Reservation):
            asyncio.get_running_loop().run_in_executor(None, self._exec, "DELETE FROM usage WHERE id = ?", (res.id,))

    def _reserve(self, tenant, tokens, sweep):
        now = self.clock()
        cutoff = now - self.window
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM usage WHERE tenant = ? AND ts <= ?", (tenant, cutoff))
            if sweep:
                # tenants that went quiet never trigger their own cleanup
                db.execute("DELETE FROM usage WHERE ts <= ?", (cutoff,))
            used = db.execute("SELECT COALESCE(SUM(tokens), 0) FROM usage WHERE tenant = ?",
                              (tenant,)).fetchone()[0]

            if used + tokens > self.limit:
                retry = None
                if tokens <= self.limit:
                    retry = self._retry_after(db, tenant, used, tokens, now)
                db.execute("COMMIT")
                return Denied(retry_after_s=retry, used=used, limit=self.limit)

            cur = db.execute("INSERT INTO usage (tenant, ts, tokens) VALUES (?, ?, ?)", (tenant, now, tokens))
            db.execute("COMMIT")
            return Reservation(id=cur.lastrowid, tokens=tokens)
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
        finally:
            db.close()

    def _retry_after(self, db, tenant, used, tokens, now):
        # walk oldest-first until enough tokens would have aged out
        need = used + tokens - self.limit
        freed = 0
        for ts, n in db.execute("SELECT ts, tokens FROM usage WHERE tenant = ? ORDER BY ts", (tenant,)):
            freed += n
            if freed >= need:
                return max(0.0, ts + self.window - now)
        return self.window

    async def settle(self, reservation_id: int, actual_tokens: int):
        await asyncio.to_thread(self._exec, "UPDATE usage SET tokens = ? WHERE id = ?",
                                (max(0, int(actual_tokens)), reservation_id))

    async def release(self, reservation_id: int):
        await asyncio.to_thread(self._exec, "DELETE FROM usage WHERE id = ?", (reservation_id,))

    async def usage(self, api_key: str) -> int:
        def q():
            db = self._connect()
            try:
                return db.execute("SELECT COALESCE(SUM(tokens), 0) FROM usage WHERE tenant = ? AND ts > ?",
                                  (tenant_key(api_key), self.clock() - self.window)).fetchone()[0]
            finally:
                db.close()
        return await asyncio.to_thread(q)

    def _exec(self, sql, args):
        db = self._connect()
        try:
            db.execute(sql, args)
        finally:
            db.close()
