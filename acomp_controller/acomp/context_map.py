"""
acomp/context_map.py

Loads and parses alomp_config.yaml -- the dependency graph, work factors, and
guardrails that the Policy Engine reasons over each cycle. See Section 4.2
(Context Map Configuration) of the ACOMP thesis for the full schema.

The Context Map is read once at controller startup and re-read on each cycle
start so that operators can adjust guardrails or work factors without
restarting the controller pod.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger("acomp.context_map")


@dataclass
class Dependency:
    """A single downstream dependency edge with its calibrated work factor."""
    service: str
    work_factor: float


@dataclass
class ServiceNode:
    """A service in the pipeline dependency graph."""
    name: str
    downstream: list[Dependency] = field(default_factory=list)


@dataclass
class Guardrails:
    """Operator-configured safety limits, enforced regardless of policy output."""
    min_replicas: int = 1
    max_replicas: int = 20
    propagation_threshold: float = 0.30  # theta_prop from Eq. 4 in the thesis


@dataclass
class ContextMap:
    """
    The full parsed Context Map: dependency graph plus guardrails.

    services:      ordered list of ServiceNode, as declared in the YAML file
    guardrails:     global guardrails applied to every propagation decision
    """
    services: list[ServiceNode]
    guardrails: Guardrails

    def service_names(self) -> list[str]:
        """All service names known to the Context Map, including those that
        only appear as a downstream target and never declare their own
        downstream list (e.g. leaf services like payment-service)."""
        names: set[str] = set()
        for svc in self.services:
            names.add(svc.name)
            for dep in svc.downstream:
                names.add(dep.service)
        return sorted(names)

    def downstream_of(self, service_name: str) -> list[Dependency]:
        """Returns the downstream dependencies declared for a given service,
        or an empty list if the service has none (a pipeline leaf)."""
        for svc in self.services:
            if svc.name == service_name:
                return svc.downstream
        return []

    def work_factor(self, upstream: str, downstream: str) -> float | None:
        """Looks up W(upstream, downstream) as defined in Equation 3.
        Returns None if no such edge exists in the dependency graph."""
        for dep in self.downstream_of(upstream):
            if dep.service == downstream:
                return dep.work_factor
        return None


def load_context_map(path: str | Path) -> ContextMap:
    """
    Parses an alomp_config.yaml file into a ContextMap.

    Expected schema (see thesis Listing: Example alomp_config.yaml):

        pipeline:
          services:
            - name: frontend
              downstream:
                - service: product-catalogue
                  work_factor: 0.85
          guardrails:
            min_replicas: 2
            max_replicas: 20
            propagation_threshold: 0.30

    Raises FileNotFoundError if the path does not exist, and ValueError if
    the YAML is structurally invalid (missing required keys).
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Context Map file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not raw or "pipeline" not in raw:
        raise ValueError(f"Context Map at {path} is missing top-level 'pipeline' key")

    pipeline = raw["pipeline"]
    raw_services = pipeline.get("services", [])
    raw_guardrails = pipeline.get("guardrails", {})

    services: list[ServiceNode] = []
    for entry in raw_services:
        name = entry.get("name")
        if not name:
            raise ValueError(f"Service entry missing 'name': {entry}")

        deps: list[Dependency] = []
        for d in entry.get("downstream", []):
            if "service" not in d or "work_factor" not in d:
                raise ValueError(
                    f"Malformed downstream entry under service '{name}': {d}"
                )
            deps.append(Dependency(service=d["service"], work_factor=float(d["work_factor"])))

        services.append(ServiceNode(name=name, downstream=deps))

    guardrails = Guardrails(
        min_replicas=int(raw_guardrails.get("min_replicas", 1)),
        max_replicas=int(raw_guardrails.get("max_replicas", 20)),
        propagation_threshold=float(raw_guardrails.get("propagation_threshold", 0.30)),
    )

    logger.info(
        "Loaded Context Map from %s: %d services, propagation_threshold=%.2f, "
        "max_replicas=%d",
        path, len(services), guardrails.propagation_threshold, guardrails.max_replicas,
    )

    return ContextMap(services=services, guardrails=guardrails)
