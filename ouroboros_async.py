#!/usr/bin/env python3
"""
Multi-Agent Orchestrator for Termux (Android)
Stack: Mando, Triple Watchdog, Orthos, The Tribunal, Ouroboros
Engines: Local GGUF models via llama-cpp-python or local llama-server endpoints
"""

import asyncio
import hashlib
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# ==============================================================================
# TERMINAL UI & PRODUCTION LOGGING
# ==============================================================================

class ColorFormatter(logging.Formatter):
    """Terminal color formatter with support for mobile terminal screens."""
    GREY = "\033[90m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    FORMATS = {
        logging.DEBUG: GREY + "[DEBUG] %(message)s" + RESET,
        logging.INFO: CYAN + "[INFO] %(message)s" + RESET,
        logging.WARNING: YELLOW + "[WARN] %(message)s" + RESET,
        logging.ERROR: RED + "[ERROR] %(message)s" + RESET,
        logging.CRITICAL: BOLD + RED + "[CRITICAL] %(message)s" + RESET,
    }

    def format(self, record: logging.LogRecord) -> str:
        log_fmt = self.FORMATS.get(record.levelno, self.FORMATS[logging.INFO])
        formatter = logging.Formatter(log_fmt, datefmt="%H:%M:%S")
        return formatter.format(record)

logger = logging.getLogger("Orchestrator")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(ColorFormatter())
logger.addHandler(stream_handler)


# ==============================================================================
# AGENT CONFIGURATION & SYSTEM STATE
# ==============================================================================

@dataclass
class AgentConfig:
    name: str
    role: str
    model_tag: str
    system_prompt: str
    temperature: float = 0.2
    max_tokens: int = 1024
    top_p: float = 0.95


@dataclass
class OrchestratorState:
    """Dynamic operational state mutated at runtime by Ouroboros."""
    patch_iteration: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_file_hash: str = ""
    is_processing: bool = False
    
    # Global execution knobs adjusted dynamically
    global_temp_drift: float = 0.0
    strict_security_mode: bool = False
    runtime_overrides: Dict[str, Any] = field(default_factory=dict)


# ==============================================================================
# GGUF INFERENCE BACKEND (Threaded / Network Client Adapter)
# ==============================================================================

class GGUFAsyncEngine:
    """
    Asynchronous bridge for GGUF model execution.
    Wraps CPU-bound llama-cpp-python operations or HTTP llama-server calls
    into non-blocking asyncio routines.
    """
    def __init__(self, concurrency_limit: int = 2):
        # Bound concurrency to prevent Termux OOM crashes on Android
        self.semaphore = asyncio.Semaphore(concurrency_limit)

    async def execute_inference(
        self, 
        agent: AgentConfig, 
        prompt: str, 
        temp_override: Optional[float] = None
    ) -> str:
        """Executes LLM inference without blocking the asyncio event loop."""
        temperature = temp_override if temp_override is not None else agent.temperature
        
        async with self.semaphore:
            logger.info(f"\033[94m[{agent.name}]\033[0m Generating with temp={temperature:.2f}...")
            
            try:
                # Runs the synchronous inference within a thread pool
                response = await asyncio.to_thread(
                    self._mockable_gguf_call,
                    agent.system_prompt,
                    prompt,
                    temperature,
                    agent.max_tokens
                )
                return response
            except Exception as e:
                logger.error(f"Inference failed for {agent.name}: {str(e)}")
                return f"[ERROR: Inference Failed - {str(e)}]"

    def _mockable_gguf_call(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        temperature: float, 
        max_tokens: int
    ) -> str:
        """
        Placeholder / Binding for actual llama-cpp-python or IPC calls.
        To use real local models:
          from llama_cpp import Llama
          llm = Llama(model_path="...")
          return llm.create_chat_completion(...)['choices'][0]['message']['content']
        """
        # Emulating execution latency for production pipeline demonstration
        time.sleep(1.1)
        
        # Deterministic simulation outputs for demonstration
        if "Mando" in system_prompt:
            return "ROUTE: Refactor performance while patching arbitrary execution paths."
        elif "Triple Watchdog" in system_prompt:
            if "os.system" in user_prompt or "eval" in user_prompt:
                return "VULNERABILITY DETECTED: CWE-78 Command Injection in os.system execution."
            return "SECURITY AUDIT PASS: No severe injection vectors found."
        elif "Orthos" in system_prompt:
            return "OPTIMIZATION: Replace blocking calls with subprocess.run and isolate scope."
        elif "Tribunal" in system_prompt:
            if "VULNERABILITY DETECTED" in user_prompt:
                return "VERDICT: FAIL\nPATCH: import subprocess\nsubprocess.run(['safe_cmd'], check=True)"
            return "VERDICT: PASS\nPATCH: None required. Source accepted."
        elif "Ouroboros" in system_prompt:
            return "SYSTEM_MUTATION: Increase temperature for broader exploration; enforce strict static sanitization."
        
        return "Acknowledged."


# ==============================================================================
# PIPELINE AGENT DEFINITIONS
# ==============================================================================

MANDO = AgentConfig(
    name="Mando",
    role="Core Orchestrator & Task Router",
    model_tag="Qwen2.5-Coder",
    system_prompt="You are Mando, the Core Orchestrator. Parse and route code tasks efficiently.",
    temperature=0.1
)

WATCHDOG = AgentConfig(
    name="Triple Watchdog",
    role="Red-Team Security Auditor",
    model_tag="SmolLM2",
    system_prompt="You are Triple Watchdog. Aggressively identify security bugs, memory issues, and injections.",
    temperature=0.2
)

ORTHOS = AgentConfig(
    name="Orthos",
    role="Structural Refactor & Optimization Engine",
    model_tag="Gemma-2",
    system_prompt="You are Orthos. Refactor code for optimal performance, elegance, and low memory footprints.",
    temperature=0.3
)

TRIBUNAL = AgentConfig(
    name="The Tribunal",
    role="Consensus Debate Judge",
    model_tag="Llama-3",
    system_prompt="You are The Tribunal. Adjudicate between Security and Optimization proposals. Issue PASS or FAIL with final PATCH.",
    temperature=0.1
)

OUROBOROS = AgentConfig(
    name="Ouroboros",
    role="Meta-Recursive Watcher & Param Mutator",
    model_tag="Meta-Core",
    system_prompt="You are Ouroboros. Analyze workflow results and dynamically calibrate system parameters.",
    temperature=0.7
)


# ==============================================================================
# CORE WORKFLOW ORCHESTRATION PIPELINE
# ==============================================================================

class MultiAgentPipeline:
    def __init__(self, engine: GGUFAsyncEngine, state: OrchestratorState):
        self.engine = engine
        self.state = state

    async def execute_ouroboros_mutation(self, verdict: str, audit_logs: str) -> None:
        """
        Dynamic prompt and hyperparameter rewrite function.
        Modifies system prompts and operational temperatures at runtime.
        """
        logger.info("\033[95m[Ouroboros]\033[0m Commencing dynamic operational rewrite...")
        
        prompt = (
            f"Tribunal Verdict: {verdict}\n"
            f"Execution Logs:\n{audit_logs}\n"
            f"Current Consecutive Failures: {self.state.consecutive_failures}\n"
            "Calibrate system temperature drift and mutate agent system prompts."
        )

        analysis = await self.engine.execute_inference(OUROBOROS, prompt)

        if "FAIL" in verdict:
            self.state.consecutive_failures += 1
            self.state.consecutive_successes = 0
            # Broaden search space by bumping temperature; tighten red-team rigor
            self.state.global_temp_drift = min(0.5, self.state.global_temp_drift + 0.1)
            WATCHDOG.temperature = min(0.8, WATCHDOG.temperature + 0.15)
            WATCHDOG.system_prompt += " STRICT_HEURISTICS: Look specifically for indirect eval/shell calls."
            logger.warning(
                f"\033[95m[Ouroboros Self-Healing]\033[0m Tribunal REJECTED patch. "
                f"Escalating Watchdog temp to {WATCHDOG.temperature:.2f}."
            )
        else:
            self.state.consecutive_successes += 1
            self.state.consecutive_failures = 0
            # Settle parameters toward deterministic convergence
            self.state.global_temp_drift = max(-0.2, self.state.global_temp_drift - 0.05)
            WATCHDOG.temperature = max(0.1, WATCHDOG.temperature - 0.05)
            logger.info(
                f"\033[92m[Ouroboros Stabilization]\033[0m Tribunal PASSED patch. "
                f"Consolidating stability. Temp Drift: {self.state.global_temp_drift:.2f}"
            )

    async def run_pipeline(self, target_filepath: Path) -> None:
        """Full audit, optimization, consensus, and self-patching lifecycle."""
        if self.state.is_processing:
            logger.warning("Pipeline is currently engaged. Skipping trigger.")
            return

        self.state.is_processing = True
        start_time = time.time()
        logger.info(f"\033[1m=== Commencing Audit Loop for: {target_filepath.name} ===\033[0m")

        try:
            with open(target_filepath, "r", encoding="utf-8") as f:
                code_content = f.read()

            # Step 1: Mando decomposes and formulates the routing task
            mando_plan = await self.engine.execute_inference(
                MANDO, 
                f"Code:\n```python\n{code_content}\n```\nFormulate routing plan."
            )
            logger.info(f"\033[96m[Mando Plan]\033[0m {mando_plan}")

            # Step 2: Asynchronous concurrent execution of Watchdog & Orthos
            logger.info("Dispatching Watchdog and Orthos concurrently...")
            watchdog_task = self.engine.execute_inference(
                WATCHDOG,
                f"Source:\n```python\n{code_content}\n```\nAudit for exploits.",
                temp_override=WATCHDOG.temperature + self.state.global_temp_drift
            )
            orthos_task = self.engine.execute_inference(
                ORTHOS,
                f"Source:\n```python\n{code_content}\n```\nPlan performance optimizations.",
                temp_override=ORTHOS.temperature + self.state.global_temp_drift
            )

            # Both run in parallel without blocking each other
            security_audit, optimization_proposal = await asyncio.gather(
                watchdog_task, orthos_task
            )

            logger.info(f"\033[93m[Watchdog Report]\033[0m {security_audit}")
            logger.info(f"\033[92m[Orthos Proposal]\033[0m {optimization_proposal}")

            # Step 3: The Tribunal judges consensus and decides action
            tribunal_prompt = (
                f"Original Code:\n{code_content}\n\n"
                f"Security Audit by Watchdog:\n{security_audit}\n\n"
                f"Refactoring by Orthos:\n{optimization_proposal}\n\n"
                "Reconcile conflicts, judge whether it passes security standards, and issue final code."
            )
            tribunal_response = await self.engine.execute_inference(TRIBUNAL, tribunal_prompt)
            logger.info(f"\033[95m[Tribunal Judgement]\033[0m\n{tribunal_response}")

            # Step 4: Ouroboros inspects Tribunal output and mutates system parameters
            verdict = "PASS" if "VERDICT: PASS" in tribunal_response else "FAIL"
            audit_summary = f"{security_audit}\n{optimization_proposal}"
            await self.execute_ouroboros_mutation(verdict, audit_summary)

            # Step 5: Self-healing disk write-back if a patch was generated
            if "PATCH:" in tribunal_response and verdict == "FAIL":
                patch_code = tribunal_response.split("PATCH:")[1].strip()
                if patch_code and patch_code != "None required. Source accepted.":
                    logger.info("\033[1mWriting self-healed patch back to disk...\033[0m")
                    # Update local state hash before writing to prevent infinite loop
                    self.state.last_file_hash = hashlib.sha256(patch_code.encode("utf-8")).hexdigest()
                    with open(target_filepath, "w", encoding="utf-8") as f:
                        f.write(f"# [Auto-Patched by Multi-Agent Stack]\n{patch_code}\n")

            elapsed = time.time() - start_time
            logger.info(f"\033[1m=== Cycle finished in {elapsed:.2f}s ===\033[0m\n")

        except Exception as e:
            logger.error(f"Error during pipeline execution: {str(e)}", exc_info=True)
        finally:
            self.state.is_processing = False


# ==============================================================================
# ASYNC FILE-WATCHING DAEMON (Debounced Polling Hook)
# ==============================================================================

class AsyncFileWatcher:
    """
    Asynchronous file watcher built with zero native C-dependencies.
    Works reliably inside Termux environments where inotify is restricted.
    """
    def __init__(self, target_path: Path, pipeline: MultiAgentPipeline, poll_interval: float = 1.0):
        self.target_path = target_path
        self.pipeline = pipeline
        self.poll_interval = poll_interval
        self._running = False

    def _calculate_hash(self) -> str:
        if not self.target_path.exists():
            return ""
        with open(self.target_path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    async def watch(self) -> None:
        """Polls for modifications and initiates pipeline runs."""
        self._running = True
        logger.info(f"Target watcher attached to \033[1m{self.target_path}\033[0m")
        self.pipeline.state.last_file_hash = self._calculate_hash()

        while self._running:
            try:
                await asyncio.sleep(self.poll_interval)
                if not self.target_path.exists():
                    continue

                current_hash = self._calculate_hash()
                if current_hash != self.pipeline.state.last_file_hash:
                    # Debounce check: ensure file write has fully settled
                    await asyncio.sleep(0.5)
                    settled_hash = self._calculate_hash()
                    
                    if settled_hash == current_hash and settled_hash != self.pipeline.state.last_file_hash:
                        # Guard against overlapping runs if already processing
                        if getattr(self.pipeline.state, "is_processing", False):
                            continue
                            
                        logger.info(f"Target modification detected on {self.target_path}")
                        self.pipeline.state.last_file_hash = settled_hash
                        
                        # Run the complete agent Loop
                        await self.pipeline.run_pipeline(self.target_path)
                        
                        # Immediately update hash post-run to catch self-heal write-backs
                        self.pipeline.state.last_file_hash = self._calculate_hash()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Watcher error: {str(e)}")

    def stop(self) -> None:
        self._running = False


# ==============================================================================
# MAIN ENTRYPOINT
# ==============================================================================

async def main():
    # Target script to audit and patch
    target_file = Path("target_script.py")
    
    # Initialize target file with an intentional vulnerability if it doesn't exist
    if not target_file.exists():
        with open(target_file, "w", encoding="utf-8") as f:
            f.write(
                'import os\n\n'
                'def run_command(cmd):\n'
                '    # High security vulnerability\n'
                '    os.system("echo Processing: " + cmd)\n'
            )
        logger.info(f"Created sample target file: {target_file}")

    # Initialize Engine, State, Pipeline, and Watcher
    engine = GGUFAsyncEngine(concurrency_limit=2)
    state = OrchestratorState()
    pipeline = MultiAgentPipeline(engine, state)
    watcher = AsyncFileWatcher(target_file, pipeline, poll_interval=1.0)

    # Initial boot audit run
    await pipeline.run_pipeline(target_file)

    # Start watcher background task
    watcher_task = asyncio.create_task(watcher.watch())

    try:
        await watcher_task
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown requested. Terminating agent background loops...")
        watcher.stop()
        watcher_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process halted by operator. Exiting Termux pipeline.")
        sys.exit(0)
