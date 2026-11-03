"""Exclusive invocation scope and conservative durable evidence recovery."""

import fcntl
import hashlib
import re
import uuid
from pathlib import Path

import duckdb

_ACTIVE = None
_TABLES = ("database_identity", "evidence_clock", "relation_evidence", "invocation")


def _initialize(db):
    # Only reserved evidence tables are replaced; user relations survive recovery.
    for name in _TABLES:
        db.execute("DROP TABLE IF EXISTS _dbt_state." + name)
    db.execute(
        "CREATE TABLE _dbt_state.database_identity (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, generation VARCHAR NOT NULL)"
    )
    db.execute(
        "INSERT INTO _dbt_state.database_identity VALUES (1, 1, ?)", [str(uuid.uuid4())]
    )
    db.execute(
        "CREATE TABLE _dbt_state.evidence_clock (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, ordinal BIGINT NOT NULL)"
    )
    db.execute("INSERT INTO _dbt_state.evidence_clock VALUES (1, 1, 0)")
    db.execute(
        "CREATE TABLE _dbt_state.relation_evidence (name VARCHAR PRIMARY KEY, version INTEGER NOT NULL, schema_hash VARCHAR NOT NULL, content_hash VARCHAR NOT NULL, ordinal BIGINT NOT NULL)"
    )
    db.execute(
        "CREATE TABLE _dbt_state.invocation (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, dirty BOOLEAN NOT NULL)"
    )
    db.execute("INSERT INTO _dbt_state.invocation VALUES (1, 1, false)")


def _valid(db):
    identity = db.execute(
        "SELECT id, version, generation FROM _dbt_state.database_identity"
    ).fetchall()
    clock = db.execute(
        "SELECT id, version, ordinal FROM _dbt_state.evidence_clock"
    ).fetchall()
    marker = db.execute(
        "SELECT id, version, dirty FROM _dbt_state.invocation"
    ).fetchall()
    if len(identity) != 1 or identity[0][:2] != (1, 1):
        return False
    uuid.UUID(identity[0][2])
    if len(clock) != 1 or clock[0][:2] != (1, 1) or not 0 <= clock[0][2] < 2**63 - 1:
        return False
    if marker != [(1, 1, False)]:
        return False
    rows = db.execute(
        "SELECT name, version, schema_hash, content_hash, ordinal FROM _dbt_state.relation_evidence"
    ).fetchall()
    return all(
        name
        and version == 1
        and re.fullmatch("[0-9a-f]{64}", sh)
        and re.fullmatch("[0-9a-f]{64}", ch)
        and 0 < ordinal <= clock[0][2]
        for name, version, sh, ch, ordinal in rows
    )


class Invocation:
    def __init__(self, path, *, reset=False):
        self.path = Path(path).expanduser().resolve()
        if str(path) == ":memory:" or "://" in str(path):
            raise ValueError("State requires a persistent local DuckDB file")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Never unlink this inode: another cooperating process may be waiting on it.
        self.lock = self.path.with_name(self.path.name + ".dbt-state.lock").open("a+b")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            raise RuntimeError(
                "Another State invocation or reset owns this DuckDB file"
            ) from None
        self.uncertain = False
        self.drainers = []
        try:
            if self.path.exists() and self.path.stat().st_nlink != 1:
                raise ValueError(
                    "Hard-linked DuckDB files are unsupported; use a separate file copy"
                )
            with duckdb.connect(str(self.path)) as db:
                db.execute("CREATE SCHEMA IF NOT EXISTS _dbt_state")
                try:
                    valid = _valid(db)
                except (duckdb.Error, ValueError, TypeError):
                    valid = False
                db.execute("BEGIN TRANSACTION")
                if reset or not valid:
                    _initialize(db)
                self.generation = db.execute(
                    "SELECT generation FROM _dbt_state.database_identity"
                ).fetchone()[0]
                db.execute("UPDATE _dbt_state.invocation SET dirty=?", [not reset])
                db.execute("COMMIT")
            stat = self.path.stat()
            scope = f"duckdb-file-v1\0{self.path}\0{stat.st_dev}\0{stat.st_ino}\0{self.generation}"
            self.namespace = hashlib.sha256(scope.encode()).hexdigest()
        except BaseException:
            self.lock.close()
            raise

    def finish(self, success):
        try:
            for drain in self.drainers:
                drain()
            if success and not self.uncertain:
                with duckdb.connect(str(self.path)) as db:
                    db.execute("UPDATE _dbt_state.invocation SET dirty=false")
        finally:
            self.lock.close()


def begin(path):
    global _ACTIVE
    canonical = Path(path).expanduser().resolve()
    if _ACTIVE is None:
        _ACTIVE = Invocation(path)
    elif _ACTIVE.path != canonical:
        raise ValueError("Only one DuckDB file is supported per invocation")
    return _ACTIVE


def finish(success):
    global _ACTIVE
    if _ACTIVE is not None:
        try:
            _ACTIVE.finish(success)
        finally:
            _ACTIVE = None


def reset(path):
    if not Path(path).is_file():
        raise ValueError("Reset requires an existing DuckDB file")
    invocation = Invocation(path, reset=True)
    invocation.finish(True)
    print("DuckDB State evidence reset; user relations preserved.")


def uncertain():
    if _ACTIVE is not None:
        _ACTIVE.uncertain = True


def guard_outcome(original):
    """A failed/uncertain remote completion keeps recovery dirty."""

    def guarded(*args, **kwargs):
        try:
            result = original(*args, **kwargs)
            if not getattr(result, "success", True):
                uncertain()
            return result
        except BaseException:
            uncertain()
            raise

    return guarded
