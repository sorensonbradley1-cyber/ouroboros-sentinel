"""
Argon Guard Module:
Enforces ArgonLevel.MEDIUM lock, performs multi-pass deterministic state validation,
anomaly scoring, token budget clamping, and Duke Devlin / Orgoth 3d6 tactical state rolls.
"""

import enum
import hashlib
import json
import math
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


class ArgonLevel(enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class ArgonSecurityError(Exception):
    """Base exception for Argon Guard security violations."""


class ArgonDriftError(ArgonSecurityError):
    """Raised when Argon thinking_level drifts from MEDIUM and strict rejection is active."""


@dataclass(frozen=True)
class OrgothRollResult:
    dice: Tuple[int, int, int]
    is_triples: bool
    immutable_shield: bool     # 1-2: State lock / immutable shield
    double_throughput: bool    # 3-4: 2x token & payload throughput
    direct_bypass: bool        # 5-6: Direct bypass routing

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dice": list(self.dice),
            "is_triples": self.is_triples,
            "immutable_shield": self.immutable_shield,
            "double_throughput": self.double_throughput,
            "direct_bypass": self.direct_bypass,
        }


@dataclass
class ValidationReport:
    valid: bool
    clamped_level: ArgonLevel
    drift_detected: bool
    token_count: int
    effective_ceiling: int
    anomaly_score: float
    roll_result: OrgothRollResult
    sanitized_payload: Dict[str, Any]
    audit_notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.valid,
            "clamped_level": self.clamped_level.value,
            "drift_detected": self.drift_detected,
            "token_count": self.token_count,
            "effective_ceiling": self.effective_ceiling,
            "anomaly_score": round(self.anomaly_score, 6),
            "roll_result": self.roll_result.to_dict(),
            "audit_notes": self.audit_notes,
        }


class ArgonGuard:
    BASE_TOKEN_CEILING: int = 4096
    REQUIRED_LEVEL: ArgonLevel = ArgonLevel.MEDIUM
    ANOMALY_THRESHOLD: float = 0.85

    def __init__(self, strict_drift_reject: bool = False) -> None:
        self.strict_drift_reject = strict_drift_reject
        self._total_tokens_processed: int = 0
        self._passes_executed: int = 0

    @property
    def total_tokens_processed(self) -> int:
        return self._total_tokens_processed

    @property
    def passes_executed(self) -> int:
        return self._passes_executed

    def enforce_level_lock(self, requested_level: Any) -> Tuple[ArgonLevel, bool, str]:
        """
        Enforces ArgonLevel.MEDIUM lock.
        Rejects (if strict) or clamps LOW/HIGH drift to MEDIUM.
        """
        parsed_level: Optional[ArgonLevel] = None
        if isinstance(requested_level, ArgonLevel):
            parsed_level = requested_level
        elif isinstance(requested_level, str):
            try:
                parsed_level = ArgonLevel(requested_level.upper().strip())
            except ValueError:
                parsed_level = None

        if parsed_level == self.REQUIRED_LEVEL:
            return self.REQUIRED_LEVEL, False, "ArgonLevel.MEDIUM verified."

        drift_msg = f"Drift detected: requested '{requested_level}', locked to ArgonLevel.MEDIUM."
        if self.strict_drift_reject:
            raise ArgonDriftError(drift_msg)

        return self.REQUIRED_LEVEL, True, drift_msg

    @staticmethod
    def roll_orgoth_3d6(seed_material: str, cycle_id: int) -> OrgothRollResult:
        """
        Duke Devlin / Orgoth-style 3d6 tactical state roll mechanics.
        Deterministically derived from SHA-256 of state seed + cycle_id,
        ensuring multi-pass deterministic reproducibility.
        Rules:
          - Face 1 or 2: immutable_shield (locks core state parameters)
          - Face 3 or 4: double_throughput (doubles token budget ceiling to 8192)
          - Face 5 or 6: direct_bypass (routes directly past secondary queuing)
          - Triples: triggers all three tactical buffs simultaneously.
        """
        digest = hashlib.sha256(f"{seed_material}::cycle::{cycle_id}".encode("utf-8")).digest()
        d1 = (digest[0] % 6) + 1
        d2 = (digest[1] % 6) + 1
        d3 = (digest[2] % 6) + 1
        dice = (d1, d2, d3)

        is_triples = (d1 == d2 == d3)
        if is_triples:
            return OrgothRollResult(
                dice=dice,
                is_triples=True,
                immutable_shield=True,
                double_throughput=True,
                direct_bypass=True,
            )

        has_1_2 = any(d in (1, 2) for d in dice)
        has_3_4 = any(d in (3, 4) for d in dice)
        has_5_6 = any(d in (5, 6) for d in dice)

        return OrgothRollResult(
            dice=dice,
            is_triples=False,
            immutable_shield=has_1_2,
            double_throughput=has_3_4,
            direct_bypass=has_5_6,
        )

    @staticmethod
    def estimate_tokens(payload_str: str) -> int:
        """
        Deterministic token estimation (approx 4 bytes/chars per token + word boundary weighting).
        """
        if not payload_str:
            return 0
        char_estimate = math.ceil(len(payload_str.encode("utf-8")) / 4.0)
        word_estimate = len(payload_str.split())
        return max(char_estimate, word_estimate)

    @staticmethod
    def _calculate_shannon_entropy(data: str) -> float:
        if not data:
            return 0.0
        counts = Counter(data)
        length = len(data)
        entropy = -sum((count / length) * math.log2(count / length) for count in counts.values())
        # Normalize against 8-bit max entropy (8.0)
        return min(entropy / 8.0, 1.0)

    @staticmethod
    def _max_depth(obj: Any, current_depth: int = 0) -> int:
        if isinstance(obj, dict) and obj:
            return max(ArgonGuard._max_depth(v, current_depth + 1) for v in obj.values())
        if isinstance(obj, list) and obj:
            return max(ArgonGuard._max_depth(i, current_depth + 1) for i in obj)
        return current_depth

    def compute_anomaly_score(self, payload: Dict[str, Any], serialized: str) -> float:
        """
        Computes a deterministic anomaly score in [0.0, 1.0] based on:
        1. Normalized character entropy
        2. Structural nesting depth penalty
        3. Control / non-printable character ratio
        """
        entropy_norm = self._calculate_shannon_entropy(serialized)
        depth = self._max_depth(payload)
        depth_penalty = min(depth / 16.0, 1.0)

        non_printable = sum(1 for c in serialized if not c.isprintable() and c not in ("\n", "\r", "\t"))
        ctrl_ratio = min(non_printable / max(len(serialized), 1), 1.0)

        # Ideal structured JSON has moderate entropy (~0.45 - 0.65). Extreme low or high is anomalous.
        entropy_deviation = abs(entropy_norm - 0.55) / 0.55
        score = (0.45 * min(entropy_deviation, 1.0)) + (0.35 * depth_penalty) + (0.20 * ctrl_ratio)
        return round(min(max(score, 0.0), 1.0), 6)

    def validate_and_clamp(
        self,
        payload: Dict[str, Any],
        cycle_id: int,
        requested_level: Any = None,
    ) -> ValidationReport:
        """
        Executes a 3-pass deterministic state validation:
          Pass 1: ArgonLevel.MEDIUM enforcement & structural normalization.
          Pass 2: Orgoth 3d6 tactical roll & token budget enforcement (4096 or 8192 ceiling).
          Pass 3: Anomaly scoring & immutable shield state locking.
        """
        audit_notes: List[str] = []
        working_payload = json.loads(json.dumps(payload, sort_keys=True, default=str))

        # --- PASS 1: Level Lock & Schema Normalization ---
        lvl_input = requested_level or working_payload.get("thinking_level", ArgonLevel.MEDIUM.value)
        clamped_level, drift_detected, drift_note = self.enforce_level_lock(lvl_input)
        audit_notes.append(f"[Pass 1] {drift_note}")
        working_payload["thinking_level"] = clamped_level.value
        working_payload["model_tier"] = "gemini-4-argon"

        # --- PASS 2: Orgoth 3d6 Roll & Token Budget Enforcement ---
        canonical_seed = json.dumps(working_payload, sort_keys=True)
        roll_result = self.roll_orgoth_3d6(canonical_seed, cycle_id)
        audit_notes.append(
            f"[Pass 2] Orgoth 3d6 Roll={roll_result.dice} | "
            f"Triples={roll_result.is_triples} | "
            f"Shield={roll_result.immutable_shield} | "
            f"2xThroughput={roll_result.double_throughput} | "
            f"Bypass={roll_result.direct_bypass}"
        )

        effective_ceiling = (
            self.BASE_TOKEN_CEILING * 2 if roll_result.double_throughput else self.BASE_TOKEN_CEILING
        )

        serialized = json.dumps(working_payload, sort_keys=True)
        token_count = self.estimate_tokens(serialized)

        if token_count > effective_ceiling:
            audit_notes.append(
                f"[Pass 2] Token ceiling exceeded ({token_count} > {effective_ceiling}). Clamping payload."
            )
            working_payload = self._clamp_payload_to_budget(working_payload, effective_ceiling)
            serialized = json.dumps(working_payload, sort_keys=True)
            token_count = self.estimate_tokens(serialized)

        # --- PASS 3: Anomaly Scoring & Tactical State Application ---
        anomaly_score = self.compute_anomaly_score(working_payload, serialized)
        is_valid = anomaly_score <= self.ANOMALY_THRESHOLD

        if not is_valid:
            audit_notes.append(
                f"[Pass 3] Anomaly score {anomaly_score} exceeded threshold {self.ANOMALY_THRESHOLD}."
            )
        else:
            audit_notes.append(f"[Pass 3] Anomaly score {anomaly_score} within safe bounds.")

        if roll_result.immutable_shield:
            state_digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
            working_payload["_argon_shield_lock"] = state_digest
            audit_notes.append(f"[Pass 3] Immutable Shield active; state locked ({state_digest[:12]}).")

        working_payload["_orgoth_tactical"] = roll_result.to_dict()
        working_payload["_argon_validated_at"] = round(time.time(), 4)

        self._total_tokens_processed += token_count
        self._passes_executed += 3

        return ValidationReport(
            valid=is_valid,
            clamped_level=clamped_level,
            drift_detected=drift_detected,
            token_count=token_count,
            effective_ceiling=effective_ceiling,
            anomaly_score=anomaly_score,
            roll_result=roll_result,
            sanitized_payload=working_payload,
            audit_notes=audit_notes,
        )

    def _clamp_payload_to_budget(
        self, payload: Dict[str, Any], ceiling: int
    ) -> Dict[str, Any]:
        """
        Deterministically trims oversized string/list fields to fit inside the token ceiling.
        """
        clamped = dict(payload)
        max_chars = ceiling * 3  # Conservative char limit to stay safely under token ceiling

        if "history_trace" in clamped and isinstance(clamped["history_trace"], list):
            while len(clamped["history_trace"]) > 2 and self.estimate_tokens(json.dumps(clamped)) > ceiling:
                clamped["history_trace"].pop(0)

        for key in sorted(clamped.keys()):
            if self.estimate_tokens(json.dumps(clamped)) <= ceiling:
                break
            val = clamped[key]
            if isinstance(val, str) and len(val) > 256:
                clamped[key] = val[:256] + "...[ARGON_CLAMPED]"
            elif isinstance(val, list) and len(val) > 5:
                clamped[key] = val[:5]

        raw_str = json.dumps(clamped, sort_keys=True)
        if self.estimate_tokens(raw_str) > ceiling:
            clamped = {
                "model_tier": clamped.get("model_tier", "gemini-4-argon"),
                "thinking_level": ArgonLevel.MEDIUM.value,
                "clamped_summary": raw_str[: max_chars // 2] + "...[HARD_TRUNCATED]",
            }
        return clamped
