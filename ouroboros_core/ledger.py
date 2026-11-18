"""SQLite WAL-mode audit ledger with SHA-256 cryptographic hash chaining."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

GENESIS_HASH = "0" * 64


def _canonical_json(data: Any) -> str:
    """Serialize data deterministically for hashing and storage."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_hex(raw: str) -> str:
    """Return lowercase SHA-256 hex digest of a UTF-8 string."""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class LedgerRecord:
    """Immutable representation of a single chained audit ledger entry."""

    id: int
    cycle_id: int
    timestamp: str
    status: str
    payload_hash: str
    prev_hash: str
    record_hash: str
    details_json: str

    def details(self) -> dict[str, Any]:
        """Decode stored JSON details into a dictionary."""
        parsed = json.loads(self.details_json)
        return parsed if isinstance(parsed, dict) else {"value": parsed}

    def to_dict(self) -> dict[str, Any]:
        """Convert record to a JSON-serializable dictionary."""
        return {
            "id": self.id,
            "cycle_id": self.cycle_id,
            "timestamp": self.timestamp,
            "status": self.status,
            "payload_hash": self.payload_hash,
            "prev_hash": self.prev_hash,
            "record_hash": self.record_hash,
            "details": self.details(),
        }


class AuditLedger:
    """Append-only SQLite WAL-mode ledger linked via SHA-256 hash chaining."""

    def __init__(self, db_path: str | Path = "ouroboros_state.db") -> None:
        self.db_path = str(db_path)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._initialize()

    def _initialize(self) -> None:
        """Configure SQLite WAL pragmas and ensure ledger schema exists."""
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ledger_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_id INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    prev_hash TEXT NOT NULL,
                    record_hash TEXT NOT NULL,
                    details_json TEXT NOT NULL
                );
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ledger_cycle_id
                ON ledger_records (cycle_id);
                """
            )
            self._conn.commit()

    @staticmethod
    def _compute_record_hash(
        cycle_id: int,
        timestamp: str,
        status: str,
        payload_hash: str,
        prev_hash: str,
        details_json: str,
    ) -> str:
        """Compute deterministic SHA-256 chain hash for a ledger entry."""
        material = "|".join(
            (
                str(cycle_id),
                timestamp,
                status,
                payload_hash,
                prev_hash,
                details_json,
            )
        )
        return _sha256_hex(material)

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> LedgerRecord:
        """Map a SQLite row to a LedgerRecord instance."""
        return LedgerRecord(
            id=int(row["id"]),
            cycle_id=int(row["cycle_id"]),
            timestamp=str(row["timestamp"]),
            status=str(row["status"]),
            payload_hash=str(row["payload_hash"]),
            prev_hash=str(row["prev_hash"]),
            record_hash=str(row["record_hash"]),
            details_json=str(row["details_json"]),
        )

    def record_cycle(
        self,
        cycle_id: int,
        payload: dict[str, Any],
        status: str = "COMMITTED",
        details: dict[str, Any] | None = None,
    ) -> LedgerRecord:
        """Append a cycle entry chained to the previous record's SHA-256 hash."""
        timestamp = datetime.now(timezone.utc).isoformat()
        payload_hash = _sha256_hex(_canonical_json(payload))
        details_json = _canonical_json(details if details is not None else {})

        with self._lock:
            cur = self._conn.execute(
                "SELECT record_hash FROM ledger_records ORDER BY id DESC LIMIT 1;"
            )
            last_row = cur.fetchone()
            prev_hash = str(last_row["record_hash"]) if last_row else GENESIS_HASH
            record_hash = self._compute_record_hash(
                cycle_id=cycle_id,
                timestamp=timestamp,
                status=status,
                payload_hash=payload_hash,
                prev_hash=prev_hash,
                details_json=details_json,
            )
            insert_cur = self._conn.execute(
                """
                INSERT INTO ledger_records (
                    cycle_id, timestamp, status, payload_hash,
                    prev_hash, record_hash, details_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    cycle_id,
                    timestamp,
                    status,
                    payload_hash,
                    prev_hash,
                    record_hash,
                    details_json,
                ),
            )
            self._conn.commit()
            row_id = int(insert_cur.lastrowid)

        return LedgerRecord(
            id=row_id,
            cycle_id=cycle_id,
            timestamp=timestamp,
            status=status,
            payload_hash=payload_hash,
            prev_hash=prev_hash,
            record_hash=record_hash,
            details_json=details_json,
        )

    def fetch_recent(self, limit: int = 20) -> list[LedgerRecord]:
        """Fetch up to `limit` most recent ledger records ordered newest-first."""
        if limit <= 0:
            return []
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, cycle_id, timestamp, status, payload_hash,
                       prev_hash, record_hash, details_json
                FROM ledger_records
                ORDER BY id DESC
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def verify_chain_detailed(self) -> tuple[bool, int, str]:
        """Verify full chain integrity, returning (valid, checked_count, message)."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, cycle_id, timestamp, status, payload_hash,
                       prev_hash, record_hash, details_json
                FROM ledger_records
                ORDER BY id ASC;
                """
            ).fetchall()

        expected_prev = GENESIS_HASH
        for idx, row in enumerate(rows, start=1):
            rec = self._row_to_record(row)
            if rec.prev_hash != expected_prev:
                return (
                    False,
                    idx,
                    f"Chain break at record id={rec.id}: prev_hash mismatch.",
                )
            recomputed = self._compute_record_hash(
                cycle_id=rec.cycle_id,
                timestamp=rec.timestamp,
                status=rec.status,
                payload_hash=rec.payload_hash,
                prev_hash=rec.prev_hash,
                details_json=rec.details_json,
            )
            if rec.record_hash != recomputed:
                return (
                    False,
                    idx,
                    f"Hash mismatch at record id={rec.id}: record_hash tampered.",
                )
            expected_prev = rec.record_hash

        return (True, len(rows), "OK")

    def verify_chain(self) -> bool:
        """Return True if all ledger records form an unbroken SHA-256 chain."""
        valid, _, _ = self.verify_chain_detailed()
        return valid

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        with self._lock:
            self._conn.close()

    def __enter__(self) -> AuditLedger:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
