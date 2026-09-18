"""Execute YAML-defined workflows."""

from __future__ import annotations

import logging
from typing import Any, Callable

from workflow_lib.config import WorkflowPaths
from workflow_lib.context import RunContext, RunContextFactory
from workflow_lib.loader import get_agent_record_stage, load_workflow
from workflow_lib.models import StageRunResult, WorkflowRunResult
from workflow_lib.ref_resolver import evaluate_when, resolve_parameters

log = logging.getLogger(__name__)

AgentExecutor = Callable[[dict[str, Any], RunContext], dict[str, Any]]


class WorkflowRunner:
    """Run a declarative workflow from YAML."""

    def __init__(
        self,
        workflow_name: str,
        paths: WorkflowPaths,
        executors: dict[str, AgentExecutor],
        context_factory: RunContextFactory,
        *,
        parameters: dict[str, Any] | None = None,
        defaults_override: dict[str, Any] | None = None,
        run_id: str | None = None,
        from_stage: str | None = None,
        until_stage: str | None = None,
        force: bool = False,
        skip_if_completed: bool | None = None,
    ):
        self.workflow_name = workflow_name
        self.paths = paths
        self.executors = executors
        self.context_factory = context_factory
        self.pipeline = load_workflow(workflow_name, paths)
        if defaults_override:
            merged = dict(self.pipeline.get("defaults", {}))
            merged.update(defaults_override)
            self.pipeline = {**self.pipeline, "defaults": merged}
        self.parameters = self._merge_parameters(parameters or {})
        self.run_id = run_id
        self.from_stage = from_stage
        self.until_stage = until_stage
        self.force = force
        self.skip_if_completed = (
            skip_if_completed
            if skip_if_completed is not None
            else bool(self.pipeline.get("defaults", {}).get("skip_if_completed", True))
        )
        self.context: RunContext | None = None
        self.stage_outputs: dict[str, dict[str, Any]] = {}

    def _merge_parameters(self, cli_parameters: dict[str, Any]) -> dict[str, Any]:
        merged = dict(cli_parameters)
        param_defs = self.pipeline.get("parameters", {})
        for name, spec in param_defs.items():
            if name not in merged and isinstance(spec, dict) and "default" in spec:
                merged[name] = spec["default"]
        missing = [
            name
            for name, spec in param_defs.items()
            if isinstance(spec, dict)
            and spec.get("required")
            and merged.get(name) in (None, "")
        ]
        if missing:
            raise ValueError(
                f"Missing required pipeline parameters: {', '.join(missing)}"
            )
        return merged

    def _ordered_stages(self) -> list[tuple[str, dict[str, Any]]]:
        stages: dict[str, dict[str, Any]] = self.pipeline.get("stages", {})
        ordered: list[tuple[str, dict[str, Any]]] = []
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visited:
                return
            if name not in stages:
                raise ValueError(f"Unknown stage in needs: {name}")
            for dep in stages[name].get("needs", []):
                visit(dep)
            visited.add(name)
            ordered.append((name, stages[name]))

        for stage_name in stages:
            visit(stage_name)
        return ordered

    def _ensure_context(self, first_stage: str) -> None:
        if self.context is not None:
            return
        if self.run_id:
            self.context = self.context_factory.load(self.run_id)
            return
        if first_stage != self.context_factory.create_run_stage:
            raise ValueError(
                f"--run-id is required when not starting from "
                f"{self.context_factory.create_run_stage} "
                f"(requested from_stage={self.from_stage})"
            )
        self.context = self.context_factory.create(self.parameters)
        self.run_id = self.context.run_id

    def _should_skip_stage(self, record_stage: str | None) -> str | None:
        assert self.context is not None
        if self.force or not self.skip_if_completed or not record_stage:
            return None
        if record_stage in self.context.stages_completed():
            return "already completed"
        return None

    def run(self) -> WorkflowRunResult:
        ordered = self._ordered_stages()
        if not ordered:
            raise ValueError(f"Workflow {self.workflow_name} has no stages")

        first_stage_name = self.from_stage or ordered[0][0]
        self._ensure_context(first_stage_name)
        assert self.context is not None
        self.context.setup_logging()
        seed_stage_outputs = getattr(self.context_factory, "seed_stage_outputs", None)
        if seed_stage_outputs:
            seed_stage_outputs(
                self.context,
                self.pipeline,
                self.stage_outputs,
                lambda agent: get_agent_record_stage(agent, self.paths),
            )

        results: list[StageRunResult] = []
        started = self.from_stage is None

        for stage_name, stage_cfg in ordered:
            if self.from_stage and not started:
                if stage_name == self.from_stage:
                    started = True
                else:
                    continue

            agent_name = stage_cfg["agent"]
            record_stage = get_agent_record_stage(agent_name, self.paths)
            run_ref = self.context.to_ref_dict(self.parameters)

            if not evaluate_when(
                stage_cfg.get("when"),
                pipeline=self.pipeline,
                parameters=self.parameters,
                stage_outputs=self.stage_outputs,
                run_context=run_ref,
            ):
                results.append(
                    StageRunResult(
                        name=stage_name,
                        agent=agent_name,
                        status="skipped",
                        skipped=True,
                        reason="when condition false",
                    )
                )
                log.info("Skipping stage %s (when condition false)", stage_name)
                continue

            skip_reason = self._should_skip_stage(record_stage)
            if skip_reason:
                results.append(
                    StageRunResult(
                        name=stage_name,
                        agent=agent_name,
                        status="skipped",
                        skipped=True,
                        reason=skip_reason,
                        outputs=dict(self.stage_outputs.get(stage_name, {})),
                    )
                )
                log.info("Skipping stage %s (%s)", stage_name, skip_reason)
                if stage_name == self.until_stage:
                    break
                continue

            executor = self.executors.get(agent_name)
            if executor is None:
                raise ValueError(f"Unknown agent: {agent_name}")

            resolved = resolve_parameters(
                stage_cfg.get("parameters", {}),
                pipeline=self.pipeline,
                parameters=self.parameters,
                stage_outputs=self.stage_outputs,
                run_context=run_ref,
            )

            log.info("Running stage %s (agent=%s)", stage_name, agent_name)
            outputs = executor(resolved, self.context)
            self.stage_outputs[stage_name] = outputs
            results.append(
                StageRunResult(
                    name=stage_name,
                    agent=agent_name,
                    status="completed",
                    outputs=outputs,
                )
            )

            if stage_name == self.until_stage:
                break

        assert self.context is not None
        run_ref = self.context.to_ref_dict(self.parameters)
        artifacts = {k: str(v) for k, v in run_ref.items() if k.endswith("_file")}

        return WorkflowRunResult(
            pipeline_name=self.workflow_name,
            run_id=self.context.run_id,
            run_dir=str(self.context.run_dir),
            artifacts=artifacts,
            stages=results,
        )
