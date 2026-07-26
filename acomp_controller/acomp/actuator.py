"""
acomp/actuator.py

The Actuator component of ACOMP. Takes the actionable ScalingDecisions
produced by the Policy Engine and applies them to the Kubernetes cluster
by patching Deployment replica counts via the Kubernetes API.

Per the thesis component specification table:
    Input:      DecisionSet from Policy Engine
    Processing: Patches Kubernetes Deployment scale subresource for each
                SCALE_UP decision, in dependency order (upstream first)
    Output:     ActuatorResult per decision (applied / skipped / failed)

Key design decisions (documented for thesis Implementation section):

1. Dependency order execution: decisions are applied upstream-first,
   matching the topological order of the Context Map dependency graph.
   This ensures that when frontend scales up and triggers a downstream
   pre-adjustment for cartservice, the frontend scale-up is applied
   first, preventing a brief window where downstream capacity exceeds
   upstream capacity and creates unnecessary back-pressure.

2. Dry-run mode: when dry_run=True, the Actuator logs what it would do
   without making any Kubernetes API calls. This is used during
   calibration runs and testing, and matches the --dry-run flag of
   kubectl for conceptual consistency.

3. Idempotency: the Actuator reads the current replica count from the
   Kubernetes API before patching, and skips the patch if the cluster
   already has the target replica count (e.g. because a previous cycle
   already applied it or a human operator intervened). This prevents
   spurious audit log entries for no-op cycles.

4. Guardrail double-check: even though the Policy Engine enforces
   max_replicas, the Actuator re-checks before patching as a safety
   net, since the cluster state may have changed between the Collector
   poll and the Actuator execution within the same 15-second cycle.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .policy_engine import DecisionSet, ScalingDecision, DecisionOutcome
from .context_map import ContextMap

logger = logging.getLogger("acomp.actuator")


# ----------------------------------------------------------------------
# Result types
# ----------------------------------------------------------------------

class ActuationStatus(str, Enum):
    APPLIED   = "APPLIED"    # patch sent to Kubernetes API successfully
    SKIPPED   = "SKIPPED"    # already at target replicas, no patch needed
    DRY_RUN   = "DRY_RUN"    # dry_run=True, would have applied
    FAILED    = "FAILED"     # Kubernetes API call failed


@dataclass
class ActuationResult:
    """Result of applying (or attempting to apply) one scaling decision."""
    service: str
    status: ActuationStatus
    previous_replicas: int
    target_replicas: int
    error: Optional[str] = None


@dataclass
class ActuatorReport:
    """Aggregated results from one full actuation pass."""
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


# ----------------------------------------------------------------------
# Actuator
# ----------------------------------------------------------------------

class Actuator:
    """
    Applies ACOMP scaling decisions to the Kubernetes cluster.

    Usage (normal mode, inside cluster):
        actuator = Actuator(namespace="default", context_map=context_map)
        report = actuator.apply(decision_set)

    Usage (dry run, for calibration and testing):
        actuator = Actuator(namespace="default", context_map=context_map,
                            dry_run=True)
        report = actuator.apply(decision_set)

    The Actuator initialises the Kubernetes client automatically:
    - Inside the cluster (as a Pod): uses in-cluster service account credentials
    - Outside the cluster (dev/test via port-forward): uses kubeconfig file
    """

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
        self._k8s_apps_v1 = None  # lazy-initialised on first apply() call

    def _init_k8s_client(self) -> None:
        """Initialises the Kubernetes API client, trying in-cluster config
        first (for production Pod deployment) then falling back to kubeconfig
        (for local development via kubectl port-forward or proxy)."""
        import kubernetes
        try:
            kubernetes.config.load_incluster_config()
            logger.info("Kubernetes client: using in-cluster service account")
        except kubernetes.config.ConfigException:
            kubernetes.config.load_kube_config()
            logger.info("Kubernetes client: using kubeconfig file")
        self._k8s_apps_v1 = kubernetes.client.AppsV1Api()

    def apply(self, decision_set: DecisionSet) -> ActuatorReport:
        """
        Applies all actionable scaling decisions from the DecisionSet.

        Decisions are applied in dependency order (upstream first), derived
        from the Context Map's service ordering. Services not declared in the
        Context Map but appearing in decisions are applied last.

        Returns an ActuatorReport summarising what was applied, skipped,
        or failed.
        """
        if self._k8s_apps_v1 is None and not self.dry_run:
            self._init_k8s_client()

        report = ActuatorReport()
        actionable = decision_set.actionable()

        if not actionable:
            logger.info("Actuator: no actionable decisions this cycle")
            return report

        # Sort decisions in dependency order: services declared earlier in
        # the Context Map (i.e. upstream) are applied first.
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
        """Applies a single scaling decision. Reads current replica count
        from the API first for idempotency check, then patches if needed."""
        service = decision.service
        target = decision.target_replicas

        # Hard cap safety net (second layer after Policy Engine guardrail)
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

        # Read current replica count from cluster for idempotency check
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
            logger.debug(
                "%s already at target replicas (%d), skipping patch",
                service, target,
            )
            return ActuationResult(
                service=service,
                status=ActuationStatus.SKIPPED,
                previous_replicas=current,
                target_replicas=target,
            )

        # Apply the patch
        try:
            self._patch_replicas(service, target)
            logger.info(
                "Patched %s/%s: %d -> %d replicas",
                self.namespace, service, current, target,
            )
            # Emit Kubernetes Event for visibility in kubectl get events
            direction = "up" if target > current else "down"
            self._emit_k8s_event(
                self.namespace, service,
                reason="ACOMPScaling",
                message=(
                    f"ACOMP scaled {direction}: {current}→{target} replicas "
                    f"via work-factor pipeline coordination."
                )
            )
            return ActuationResult(
                service=service,
                status=ActuationStatus.APPLIED,
                previous_replicas=current,
                target_replicas=target,
            )
        except Exception as exc:
            logger.error(
                "Failed to patch %s/%s replicas: %s",
                self.namespace, service, exc,
            )
            return ActuationResult(
                service=service,
                status=ActuationStatus.FAILED,
                previous_replicas=current,
                target_replicas=target,
                error=str(exc),
            )

    def _emit_k8s_event(self, namespace: str, deployment: str, reason: str, message: str):
        """Emit a Kubernetes Event visible in kubectl get events."""
        if self._dry_run:
            return
        try:
            import kubernetes
            core_v1 = kubernetes.client.CoreV1Api()
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            body = kubernetes.client.CoreV1Event(
                metadata=kubernetes.client.V1ObjectMeta(
                    generate_name=f"acomp-{deployment}-",
                    namespace=namespace,
                ),
                involved_object=kubernetes.client.V1ObjectReference(
                    api_version="apps/v1",
                    kind="Deployment",
                    name=deployment,
                    namespace=namespace,
                ),
                reason=reason,
                message=message[:1024],
                type="Normal",
                reporting_component="acomp-controller",
                reporting_instance="acomp",
                action="Scaling",
                first_timestamp=now,
                last_timestamp=now,
                count=1,
            )
            core_v1.create_namespaced_event(namespace, body)
        except Exception as e:
            logger.debug("K8s event emission skipped: %s", e)

    def _get_current_replicas(self, service: str) -> int:
        """Reads the current ready replica count from the Kubernetes API."""
        deployment = self._k8s_apps_v1.read_namespaced_deployment_scale(
            name=service, namespace=self.namespace
        )
        return deployment.spec.replicas or 1

    def _patch_replicas(self, service: str, target_replicas: int) -> None:
        """Patches the Deployment's replica count via the Kubernetes scale
        subresource. Uses a strategic merge patch, matching what kubectl
        scale does internally."""
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
        """
        Sorts scaling decisions so upstream services are applied before
        downstream ones, using the Context Map's declared service order
        (i.e. the order services appear in alomp_config.yaml) as the
        topological ordering proxy. Services not explicitly declared in
        the Context Map appear at the end in their original order.
        """
        # Use the declared services list order, not alphabetical service_names()
        declared_order = {
            svc.name: idx
            for idx, svc in enumerate(self.context_map.services)
        }
        return sorted(
            decisions,
            key=lambda d: declared_order.get(d.service, len(declared_order)),
        )
