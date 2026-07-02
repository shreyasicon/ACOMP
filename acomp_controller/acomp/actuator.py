from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .policy_engine import DecisionSet, ScalingDecision, DecisionOutcome
from .context_map import ContextMap

logger = logging.getLogger("acomp.actuator")


class ActuationStatus(str, Enum):
    APPLIED   = "APPLIED"
    SKIPPED   = "SKIPPED"
    DRY_RUN   = "DRY_RUN"
    FAILED    = "FAILED"


@dataclass
class ActuationResult:
    service: str
    status: ActuationStatus
    previous_replicas: int
    target_replicas: int
    error: Optional[str] = None


@dataclass
class ActuatorReport:
    results: list[ActuationResult] = field(default_factory=list)

    def applied(self) -> list[ActuationResult]:
        return [r for r in self.results
                if r.status in (ActuationStatus.APPLIED, ActuationStatus.DRY_RUN)]

    def failed(self) -> list[ActuationResult]:
        return [r for r in self.results if r.status == ActuationStatus.FAILED]

    def to_list(self) -> list[dict]:
        return [
            {
                "service": r.service,
                "status": r.status.value,
                "previous_replicas": r.previous_replicas,
                "target_replicas": r.target_replicas,
                "error": r.error,
            }
            for r in self.results
        ]


class Actuator:
    def __init__(
        self,
        namespace: str,
        context_map: ContextMap,
        dry_run: bool = False,
        max_replicas_hard_cap: int = 20,
    ):
        self.namespace = namespace
        self.context_map = context_map
        self.dry_run = dry_run
        self.max_replicas_hard_cap = max_replicas_hard_cap
        self._k8s_apps_v1 = None

    def _init_k8s_client(self) -> None:
        import kubernetes
        try:
            kubernetes.config.load_incluster_config()
            logger.info("Kubernetes client: using in-cluster service account")
        except kubernetes.config.ConfigException:
            kubernetes.config.load_kube_config()
            logger.info("Kubernetes client: using kubeconfig file")
        self._k8s_apps_v1 = kubernetes.client.AppsV1Api()

    def apply(self, decision_set: DecisionSet) -> ActuatorReport:
        if self._k8s_apps_v1 is None and not self.dry_run:
            self._init_k8s_client()

        report = ActuatorReport()
        actionable = decision_set.actionable()

        if not actionable:
            logger.info("Actuator: no actionable decisions this cycle")
            return report

        ordered = self._sort_by_dependency_order(actionable)

        for decision in ordered:
            result = self._apply_one(decision)
            report.results.append(result)

        applied_count = len(report.applied())
        failed_count = len(report.failed())
        logger.info(
            "Actuator cycle complete: %d applied, %d skipped, %d failed",
            applied_count,
            len(report.results) - applied_count - failed_count,
            failed_count,
        )
        return report

    def _apply_one(self, decision: ScalingDecision) -> ActuationResult:
        service = decision.service
        target = decision.target_replicas

        effective_target = min(target, self.max_replicas_hard_cap,
                               self.context_map.guardrails.max_replicas)
        if effective_target != target:
            logger.warning(
                "%s: target %d exceeds hard cap %d, clamping to %d",
                service, target, self.max_replicas_hard_cap, effective_target
            )
            target = effective_target

        if self.dry_run:
            logger.info(
                "[DRY RUN] Would patch %s/%s replicas: %d -> %d",
                self.namespace, service,
                decision.current_replicas, target,
            )
            return ActuationResult(
                service=service,
                status=ActuationStatus.DRY_RUN,
                previous_replicas=decision.current_replicas,
                target_replicas=target,
            )

        try:
            current = self._get_current_replicas(service)
        except Exception as exc:
            logger.error("Failed to read current replicas for %s: %s", service, exc)
            return ActuationResult(
                service=service,
                status=ActuationStatus.FAILED,
                previous_replicas=decision.current_replicas,
                target_replicas=target,
                error=str(exc),
            )

        if current == target:
            logger.debug("%s already at target replicas (%d), skipping", service, target)
            return ActuationResult(
                service=service,
                status=ActuationStatus.SKIPPED,
                previous_replicas=current,
                target_replicas=target,
            )

        try:
            self._patch_replicas(service, target)
            logger.info("Patched %s/%s: %d -> %d replicas",
                        self.namespace, service, current, target)
            return ActuationResult(
                service=service,
                status=ActuationStatus.APPLIED,
                previous_replicas=current,
                target_replicas=target,
            )
        except Exception as exc:
            logger.error("Failed to patch %s/%s replicas: %s",
                         self.namespace, service, exc)
            return ActuationResult(
                service=service,
                status=ActuationStatus.FAILED,
                previous_replicas=current,
                target_replicas=target,
                error=str(exc),
            )

    def _get_current_replicas(self, service: str) -> int:
        deployment = self._k8s_apps_v1.read_namespaced_deployment_scale(
            name=service, namespace=self.namespace
        )
        return deployment.spec.replicas or 1

    def _patch_replicas(self, service: str, target_replicas: int) -> None:
        import kubernetes
        body = {"spec": {"replicas": target_replicas}}
        self._k8s_apps_v1.patch_namespaced_deployment_scale(
            name=service,
            namespace=self.namespace,
            body=body,
        )

    def _sort_by_dependency_order(
        self, decisions: list[ScalingDecision]
    ) -> list[ScalingDecision]:
        declared_order = {
            svc.name: idx
            for idx, svc in enumerate(self.context_map.services)
        }
        return sorted(
            decisions,
            key=lambda d: declared_order.get(d.service, len(declared_order)),
        )