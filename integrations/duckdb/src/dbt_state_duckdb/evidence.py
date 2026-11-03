"""Version 1 observed-change evidence: globally ordered ordinals, never time.

Each value is NULL or length-framed UTF-8 (integers/booleans/strings), exact
fixed-scale decimal, or raw BLOB. Schema is framed ordinal/name/type/nullability.
Sorted framed rows retain multiplicity, ignore physical order. Unsupported types
have unknown evidence. Scans and completion writes share one durable sequence.
"""

import hashlib
import re
import threading
from decimal import Decimal

import duckdb
from sqlglot import exp, parse_one

_LOCK = threading.RLock()
_TYPES = {
    "BOOLEAN",
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
    "VARCHAR",
    "BLOB",
}


def frame(value):
    return len(value).to_bytes(8, "big") + value


class EvidenceStore:
    def __init__(self, path):
        self.path = str(path)

    def observe(self, table, *, completed_write=False):
        with _LOCK, duckdb.connect(self.path) as db:
            try:
                db.execute("BEGIN TRANSACTION")
                ordinal = self.observe_snapshot(
                    db, table, completed_write=completed_write
                )
                db.execute("COMMIT" if ordinal is not None else "ROLLBACK")
                return ordinal
            except (duckdb.Error, ValueError, TypeError):
                db.execute("ROLLBACK")
                return None

    def observe_snapshot(self, db, table, *, completed_write=False):
        """Observe within the caller's transaction (also used by validated CTAS)."""
        relation = parse_one(table, into=exp.Table, dialect="duckdb")
        kind = db.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_catalog=? AND table_schema=? AND table_name=?",
            [relation.catalog, relation.db, relation.name],
        ).fetchall()
        if kind == [("VIEW",)]:
            from .views import view_fingerprint

            sh, ch = view_fingerprint(db, table)
        elif kind == [("BASE TABLE",)]:
            # DESCRIBE omits column collation. Non-binary comparisons
            # can choose different tied values after a physical reorder,
            # which a relational content fingerprint intentionally ignores.
            ddl = db.execute(
                "SELECT sql FROM duckdb_tables() WHERE database_name=? AND schema_name=? AND table_name=?",
                [relation.catalog, relation.db, relation.name],
            ).fetchall()
            if (
                len(ddl) != 1
                or not ddl[0][0]
                or re.search(r"\bCOLLATE\b", ddl[0][0], re.IGNORECASE)
            ):
                return None
            # DESCRIBE and SELECT use the same committed transaction snapshot.
            schema = db.execute("DESCRIBE SELECT * FROM " + table).fetchall()
            if any(
                t not in _TYPES and not re.fullmatch(r"DECIMAL\(\d+,\d+\)", t)
                for _, t, *_ in schema
            ):
                return None
            sh = hashlib.sha256(
                b"duckdb-schema-v1"
                + b"".join(
                    frame(str(i).encode())
                    + b"".join(frame(str(v).encode()) for v in col)
                    for i, col in enumerate(schema)
                )
            ).hexdigest()
            encoded = []
            for row in db.execute("SELECT * FROM " + table).fetchall():
                fields = []
                for (_, typ, *_), value in zip(schema, row):
                    if value is None:
                        fields.append(b"N")
                    else:
                        if isinstance(value, bytes):
                            payload = value
                        elif isinstance(value, Decimal):
                            scale = int(typ.split(",")[1][:-1])
                            payload = format(value, f".{scale}f").encode()
                        else:
                            payload = str(value).encode("utf-8")
                        fields.append(b"V" + frame(typ.encode()) + frame(payload))
                encoded.append(frame(b"".join(fields)))
            ch = hashlib.sha256(
                b"duckdb-content-v1" + b"".join(sorted(encoded))
            ).hexdigest()
        else:
            return None
        return self._record_snapshot(db, table, sh, ch, completed_write)

    def _record_snapshot(self, db, table, sh, ch, completed_write):
        previous = db.execute(
            "SELECT version, schema_hash, content_hash, ordinal FROM _dbt_state.relation_evidence WHERE name = ?",
            [table],
        ).fetchone()
        clock = db.execute(
            "SELECT ordinal FROM _dbt_state.evidence_clock WHERE id=1 AND version=1"
        ).fetchone()[0]
        if previous and (previous[0] != 1 or not 0 < previous[3] <= clock):
            raise ValueError("Invalid relation evidence")
        if previous and previous[1:3] == (sh, ch) and not completed_write:
            ordinal = previous[3]
        else:
            ordinal = clock + 1
            db.execute("UPDATE _dbt_state.evidence_clock SET ordinal=?", [ordinal])
            db.execute(
                "INSERT OR REPLACE INTO _dbt_state.relation_evidence VALUES (?,1,?,?,?)",
                [table, sh, ch, ordinal],
            )
        return ordinal
