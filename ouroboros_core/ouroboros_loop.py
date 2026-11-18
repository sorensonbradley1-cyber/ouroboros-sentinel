"""Ouroboros execution loop wiring ArgonGuard, DeepfleetBus, and AuditLedger."""

from __future__ import annotations

import hashlib
import json
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from ouroboros_core.argon_guard import (
    ArgonDriftError,
    ArgonGuard,
    ArgonLevel,
    ArgonSecurityError,
    OrgothRollResult,
    ValidationReport,
)
from ouroboros_core.deepfleet_bus import DeepfleetBus
from ouroboros_core.ledger import AuditLedger, LedgerRecord


class RingBuffer:
    """Bounded SHA-256 state history buffer for tail-bite loop detection."""

    def __init__(self, capacity: int = 64) -> None:
        if capacity <= 0:
            raise ValueError("RingBuffer capacity must be greater than zero.")
        self.capacity = capacity
        self._queue: deque[str] = deque()
        self._counts: dict[str, int] = {}

    @staticmethod
    def hash_state(state: Any) -> str:
        """Compute deterministic SHA-256 hex digest of an arbitrary state object."""
        encoded = json.dumps(
            state, sort_keys=True, separators=(",", ":"), default=str
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def contains(self, digest: str) -> bool:
        """Return True if a SHA-256 digest is present in the active window."""
        return self._counts.get(digest, 0) > 0

    def push(self, state: Any) -> tuple[str, bool]:
        """Record state digest and return (digest, tail_bite_detected)."""
        digest = state if isinstance(state, str) and len(state) == 64 else self.hash_state(state)
        tail_bite = self.contains(digest)

        if len(self._queue) >= self.capacity:
            evicted = self._queue.popleft()
            remaining = self._counts[evicted] - 1
            if remaining <= 0:
                self._counts.pop(evicted, None)
            else:
                self._counts[evicted] = remaining

        self._queue.append(digest)
        self._counts[digest] = self._counts.get(digest, 0) + 1
        return digest, tail_bite

    @property
    def size(self) -> int:
        """Return current number of tracked digests."""
        return len(self._queue)

    @property
    def history(self) -> list[str]:
        """Return ordered list of digests currently in the buffer."""
        return list(self._queue)


@dataclass(frozen=True, slots=True)
class CycleResult:
    """Structured outcome of a single Ouroboros engine cycle."""

    cycle_id: int
    state_hash: str
    tail_bite_detected: bool
    argon_valid: bool
    argon_message: str
    bus_response: dict[str, Any]
    telemetry: dict[str, Any]
    ledger_record: LedgerRecord
    status: str
    duration_ms: float

    def to_dict(self) -> dict[str, Any]:
        """Convert cycle result to a JSON-serializable dictionary."""
        return {
            "cycle_id": self.cycle_id,
            "state_hash": self.state_hash,
            "tail_bite_detected": self.tail_bite_detected,
            "argon_valid": self.argon_valid,
            "argon_message": self.argon_message,
            "bus_response": self.bus_response,
            "telemetry": self.telemetry,
            "ledger_record": self.ledger_record.to_dict(),
            "status": self.status,
            "duration_ms": round(self.duration_ms, 3),
        }


class OuroborosEngine:
    """Autonomous orchestration engine unifying guardrails, bus quorum, and ledger."""

    def __init__(
        self,
        num_nodes: int = 4,
        rpc_host: str = "127.0.0.1",
        rpc_port: int = 0,
        unix_socket_path: str | None = None,
        db_path: str = "ouroboros_state.db",
        argon_level: ArgonLevel = ArgonLevel.MEDIUM,
        buffer_capacity: int = 64,
    ) -> None:
        self.argon = ArgonGuard(argon_level)
        self.bus = DeepfleetBus(
            num_nodes=num_nodes,
            rpc_host=rpc_host,
            rpc_port=rpc_port,
            unix_socket_path=unix_socket_path,
        )
        self.ledger = AuditLedger(db_path=db_path)
        self.ring_buffer = RingBuffer(capacity=buffer_capacity)
        self._cycle_counter = 0
        self._started = False

    async def start(self) -> None:
        """Start the Deepfleet consensus bus if not already running."""
        if not self._started:
            await self.bus.start()
            self._started = True

    async def stop(self) -> None:
        """Stop the Deepfleet bus and close the audit ledger."""
        if self._started:
            await self.bus.stop()
            self._started = False
        self.ledger.close()

    async def __aenter__(self) -> OuroborosEngine:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.stop()

    def _evaluate_argon(self, payload: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
        """Run payload validation and optional Orgoth roll against ArgonGuard."""
        report: ValidationReport | Any = None
        for method_name in ("validate_payload", "validate", "inspect", "check"):
            fn = getattr(self.argon, method_name, None)
            if callable(fn):
                report = fn(payload)
                break

        meta: dict[str, Any] = {}
        valid = True
        message = "Argon validation passed."

        if isinstance(report, ValidationReport) or report is not None:
            for attr in ("valid", "passed", "ok", "allowed"):
                if hasattr(report, attr):
                    valid = bool(getattr(report, attr))
                    break
            for attr in ("reason", "message", "summary", "detail"):
                if hasattr(report, attr) and getattr(report, attr) is not None:
                    message = str(getattr(report, attr))
                    break
            meta["validation_report"] = repr(report)

        for roll_name in ("orgoth_roll", "roll"):
            roll_fn = getattr(self.argon, roll_name, None)
            if callable(roll_fn):
                roll_res: OrgothRollResult | Any = roll_fn()
                meta["orgoth_roll"] = repr(roll_res)
                if hasattr(roll_res, "passed") and not bool(getattr(roll_res, "passed")):
                    valid = False
                    message = f"Orgoth roll failed: {roll_res!r}"
                break

        return valid, message, meta

    async def run_cycle(
        self,
        payload: dict[str, Any],
        cycle_id: int | None = None,
        direct_bypass: bool = False,
    ) -> CycleResult:
        """Execute one guarded, tail-bite-checked cycle across Deepfleet and Ledger."""
        if not self._started:
            await self.start()

        started_ns = time.perf_counter_ns()
        cid = cycle_id if cycle_id is not None else self._cycle_counter + 1
        self._cycle_counter = max(self._cycle_counter, cid)

        state_hash, tail_bite = self.ring_buffer.push(payload)
        argon_valid = False
        argon_message = ""
        argon_meta: dict[str, Any] = {}
        bus_response: dict[str, Any] = {}
        status = "COMMITTED"

        try:
            argon_valid, argon_message, argon_meta = self._evaluate_argon(payload)
            if not argon_valid:
                status = "REJECTED_ARGON"
            elif tail_bite:
                status = "TAIL_BITE_DETECTED"
                bus_response = await self.bus.dispatch_cycle(
                    payload, cycle_id=cid, direct_bypass=direct_bypass
                )
            else:
                bus_response = await self.bus.dispatch_cycle(
                    payload, cycle_id=cid, direct_bypass=direct_bypass
                )
        except ArgonSecurityError as exc:
            status = "BLOCKED_SECURITY"
            argon_valid = False
            argon_message = str(exc) or "Argon security violation."
        except ArgonDriftError as exc:
            status = "BLOCKED_DRIFT"
            argon_valid = False
            argon_message = str(exc) or "Argon drift threshold exceeded."

        telemetry = self.bus.quorum_telemetry()
        duration_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0

        ledger_record = self.ledger.record_cycle(
            cycle_id=cid,
            payload=payload,
            status=status,
            details={
                "state_hash": state_hash,
                "tail_bite_detected": tail_bite,
                "argon_valid": argon_valid,
                "argon_message": argon_message,
                "argon_meta": argon_meta,
                "bus_response": bus_response,
                "telemetry": telemetry,
                "duration_ms": round(duration_ms, 3),
            },
        )

        return CycleResult(
            cycle_id=cid,
            state_hash=state_hash,
            tail_bite_detected=tail_bite,
            argon_valid=argon_valid,
            argon_message=argon_message,
            bus_response=bus_response,
            telemetry=telemetry,
            ledger_record=ledger_record,
            status=status,
            duration_ms=duration_ms,
        )

    async def run_cycles(
        self,
        cycles: int,
        seed_payload: dict[str, Any] | None = None,
        direct_bypass: bool = False,
    ) -> list[CycleResult]:
        """Run `cycles` sequential engine cycles and return their results."""
        if cycles < 0:
            raise ValueError("Cycle count must be non-negative.")

        results: list[CycleResult] = []
        prev_hash = self.ring_buffer.history[-1] if self.ring_buffer.size > 0 else "genesis"

        for idx in range(1, cycles + 1):
            if seed_payload is not None:
                payload = dict(seed_payload)
            else:
                payload = {
                    "action": "heartbeat",
                    "seq": idx,
                    "prev_state_hash": prev_hash,
                }
            result = await self.run_cycle(
                payload=payload, cycle_id=idx, direct_bypass=direct_bypass
            )
            prev_hash = result.state_hash
            results.append(result)

        return results
