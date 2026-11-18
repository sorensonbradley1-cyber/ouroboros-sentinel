"""Command-line interface for running Ouroboros cycles and inspecting the ledger."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from ouroboros_core.ledger import AuditLedger
from ouroboros_core.ouroboros_loop import OuroborosEngine


def _build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="ouroboros",
        description="Ouroboros Engine CLI: execute consensus cycles and inspect audit ledger.",
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=None,
        help="Number of Ouroboros cycles to execute (default: 5, or 0 with --dump-ledger).",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=4,
        help="Number of Deepfleet bus nodes (default: 4).",
    )
    parser.add_argument(
        "--rpc-port",
        type=int,
        default=0,
        help="RPC bind port for DeepfleetBus (default: 0 for ephemeral port).",
    )
    parser.add_argument(
        "--inject-json",
        type=str,
        default=None,
        help="JSON object payload to inject into each executed cycle.",
    )
    parser.add_argument(
        "--dump-ledger",
        action="store_true",
        help="Verify SHA-256 chain integrity and dump recent audit ledger records.",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="ouroboros_state.db",
        help="Path to the SQLite audit ledger file (default: ouroboros_state.db).",
    )
    return parser


def _parse_injected_json(raw: str | None) -> dict[str, Any] | None:
    """Parse and validate --inject-json payload as a JSON object."""
    if raw is None:
        return None
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("--inject-json must decode to a JSON object.")
    return parsed


def _dump_ledger(db_path: str, limit: int = 50) -> int:
    """Print chain verification summary and recent ledger records as JSON."""
    with AuditLedger(db_path=db_path) as ledger:
        valid, checked, message = ledger.verify_chain_detailed()
        records = [rec.to_dict() for rec in ledger.fetch_recent(limit=limit)]

    output = {
        "chain_valid": valid,
        "records_checked": checked,
        "verification_message": message,
        "recent_records": records,
    }
    print(json.dumps(output, indent=2))
    return 0 if valid else 2


async def _run_engine(args: argparse.Namespace, seed_payload: dict[str, Any] | None) -> None:
    """Execute requested cycles on OuroborosEngine and print results."""
    cycles = args.cycles if args.cycles is not None else (0 if args.dump_ledger else 5)
    if cycles <= 0:
        return

    async with OuroborosEngine(
        num_nodes=args.nodes,
        rpc_port=args.rpc_port,
        db_path=args.db_path,
    ) as engine:
        results = await engine.run_cycles(cycles=cycles, seed_payload=seed_payload)
        print(json.dumps([item.to_dict() for item in results], indent=2))


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        seed_payload = _parse_injected_json(args.inject_json)
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"Invalid --inject-json input: {exc}", file=sys.stderr)
        return 1

    asyncio.run(_run_engine(args, seed_payload))

    if args.dump_ledger:
        return _dump_ledger(args.db_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
