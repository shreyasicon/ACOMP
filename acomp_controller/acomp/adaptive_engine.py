"""
acomp/adaptive_engine.py

ACOMP Adaptive Engine — handles operating modes, event profiles,
maintenance windows, anomaly budgets, traffic patterns and hot reload.

This module is the intelligence layer that modifies ACOMP's behaviour
based on context: upcoming events, time of day, service health budgets
and admin-declared maintenance windows.

All configuration is read from alomp_config.yaml and hot-reloaded
when the file changes — no pod restart required.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import yaml

logger = logging.getLogger("acomp.adaptive_engine")

SLO_THRESHOLD_MS = 500.0


# ── Mode Configuration ────────────────────────────────────────────────────────

@dataclass
class ModeConfig:
    name: str
    description: str
    cpu_threshold: float          = 0.70
    slope_threshold: float        = 0.15
    slope_min_cycles: int         = 2
    pre_scale_cpu_override: float = 0.50
    cooldown_seconds: float       = 30.0
    hysteresis_cycles: int        = 10
    poll_fast_seconds: float      = 5.0
    poll_base_seconds: float      = 15.0
    poll_slow_seconds: float      = 30.0
    max_replicas_cap: int         = 10
    propagation_threshold: float  = 0.30
    slo_violation_window: int     = 3


BUILTIN_MODES = {
    "strategic": ModeConfig(
        name="strategic",
        description="Balanced production mode — SLO-compliant with cost awareness",
        cpu_threshold=0.70, slope_threshold=0.15, slope_min_cycles=2,
        pre_scale_cpu_override=0.50, cooldown_seconds=30, hysteresis_cycles=10,
        poll_fast_seconds=5, poll_base_seconds=15, poll_slow_seconds=30,
        max_replicas_cap=10, propagation_threshold=0.30, slo_violation_window=3,
    ),
    "aggressive": ModeConfig(
        name="aggressive",
        description="Maximum throughput — fastest response, accepts higher cost",
        cpu_threshold=0.50, slope_threshold=0.08, slope_min_cycles=1,
        pre_scale_cpu_override=0.35, cooldown_seconds=10, hysteresis_cycles=5,
        poll_fast_seconds=3, poll_base_seconds=10, poll_slow_seconds=20,
        max_replicas_cap=20, propagation_threshold=0.15, slo_violation_window=1,
    ),
    "conservative": ModeConfig(
        name="conservative",
        description="Cost-optimised — minimal scaling, tolerates higher latency",
        cpu_threshold=0.85, slope_threshold=0.30, slope_min_cycles=4,
        pre_scale_cpu_override=0.75, cooldown_seconds=60, hysteresis_cycles=20,
        poll_fast_seconds=10, poll_base_seconds=20, poll_slow_seconds=60,
        max_replicas_cap=6, propagation_threshold=0.50, slo_violation_window=5,
    ),
}


# ── Event Profile ─────────────────────────────────────────────────────────────

@dataclass
class EventProfile:
    name: str
    status: str                   # active | inactive | draft
    start: datetime
    end: datetime
    expected_users: int
    baseline_users: int
    pre_scale_hours: float        = 2.0
    rampdown_minutes: float       = 30.0
    mode_override: str            = "aggressive"
    notes: str                    = ""

    def is_active_now(self) -> bool:
        now = datetime.now(timezone.utc)
        return self.status == "active" and self.start <= now <= self.end

    def is_pre_scale_window(self) -> bool:
        now = datetime.now(timezone.utc)
        pre_start = self.start.timestamp() - self.pre_scale_hours * 3600
        return (self.status == "active" and
                pre_start <= now.timestamp() < self.start.timestamp())

    def load_multiplier(self) -> float:
        if self.baseline_users > 0:
            return self.expected_users / self.baseline_users
        return 1.0

    def suggested_replicas(self, current_baseline: int) -> int:
        return max(1, math.ceil(current_baseline * self.load_multiplier()))


# ── Maintenance Window ────────────────────────────────────────────────────────

@dataclass
class MaintenanceWindow:
    name: str
    status: str
    cron: str
    duration_minutes: int
    affected_services: list[str]
    action: str                   # freeze_scaling | scale_down | scale_up
    notes: str = ""

    def is_active_now(self) -> bool:
        if self.status != "active":
            return False
        return _cron_active_now(self.cron, self.duration_minutes)


def _cron_active_now(cron: str, duration_minutes: int) -> bool:
    """Check if a cron expression is currently active."""
    try:
        parts = cron.strip().split()
        if len(parts) != 5:
            return False
        minute, hour, dom, month, dow = parts
        now = datetime.now(timezone.utc)

        def matches(field, value, max_val):
            if field == "*":
                return True
            return int(field) == value % (max_val + 1)

        if not matches(dow, now.weekday() + 1, 7):
            return False
        if not matches(hour, now.hour, 23):
            return False
        if not matches(minute, now.minute, 59):
            return False

        # Check if within duration window
        window_start = now.replace(
            hour=int(hour) if hour != "*" else now.hour,
            minute=int(minute) if minute != "*" else now.minute,
            second=0, microsecond=0
        )
        elapsed = (now - window_start).total_seconds()
        return 0 <= elapsed <= duration_minutes * 60
    except Exception:
        return False


# ── Anomaly Budget Tracker ────────────────────────────────────────────────────

class AnomalyBudgetTracker:
    """
    Tracks monthly SLO violation counts per service.
    When a service exceeds its budget, ACOMP automatically switches
    that service to a more conservative CPU threshold for the rest of
    the month — protecting reliability without human intervention.
    """

    def __init__(self, config: dict, budget_file: str):
        self._config = config  # service -> {budget, conservative_threshold}
        self._budget_file = budget_file
        self._violations: dict[str, int] = {}
        self._current_month = datetime.now(timezone.utc).month
        self._load()

    def _load(self):
        if os.path.exists(self._budget_file):
            try:
                with open(self._budget_file) as f:
                    data = json.load(f)
                if data.get("month") == self._current_month:
                    self._violations = data.get("violations", {})
                    return
            except Exception:
                pass
        self._violations = {}

    def _save(self):
        os.makedirs(os.path.dirname(self._budget_file) or ".", exist_ok=True)
        try:
            with open(self._budget_file, "w") as f:
                json.dump({
                    "month": self._current_month,
                    "violations": self._violations,
                    "updated": datetime.now(timezone.utc).isoformat(),
                }, f, indent=2)
        except Exception as e:
            logger.warning("Could not save anomaly budget: %s", e)

    def _reset_if_new_month(self):
        current = datetime.now(timezone.utc).month
        if current != self._current_month:
            logger.info("New month — resetting anomaly budgets")
            self._current_month = current
            self._violations = {}
            self._save()

    def record_violation(self, service: str):
        self._reset_if_new_month()
        self._violations[service] = self._violations.get(service, 0) + 1
        count = self._violations[service]
        budget = self._config.get(service, {}).get("monthly_violation_budget", 999)
        if count == budget:
            logger.warning(
                "ANOMALY BUDGET EXHAUSTED: %s hit %d violations this month "
                "— switching to conservative threshold",
                service, count
            )
        self._save()

    def budget_exhausted(self, service: str) -> bool:
        self._reset_if_new_month()
        count = self._violations.get(service, 0)
        budget = self._config.get(service, {}).get("monthly_violation_budget", 999)
        return count >= budget

    def conservative_threshold(self, service: str) -> float:
        return self._config.get(service, {}).get("conservative_threshold", 0.55)

    def status_summary(self) -> dict:
        self._reset_if_new_month()
        summary = {}
        for svc, cfg in self._config.items():
            count = self._violations.get(svc, 0)
            budget = cfg.get("monthly_violation_budget", 999)
            summary[svc] = {
                "violations": count,
                "budget": budget,
                "remaining": max(0, budget - count),
                "exhausted": count >= budget,
                "pct_used": round(count / budget * 100, 1) if budget > 0 else 0,
            }
        return summary


# ── Traffic Pattern Learner ───────────────────────────────────────────────────

class TrafficPatternTracker:
    """
    Learns hourly request rate patterns from observed data.
    Uses a rolling 7-day average per hour-of-day to predict
    upcoming peaks and pre-scale 15 minutes before they arrive.
    No ML — just a lookup table updated every cycle.
    """

    def __init__(self, pattern_file: str, pre_scale_minutes: int = 15,
                 hourly_defaults: dict | None = None):
        self._file = pattern_file
        self._pre_scale_minutes = pre_scale_minutes
        self._defaults = hourly_defaults or {}
        self._patterns: dict[int, list[float]] = {}  # hour -> last 7 readings
        self._load()

    def _load(self):
        if os.path.exists(self._file):
            try:
                with open(self._file) as f:
                    self._patterns = {int(k): v for k, v in json.load(f).items()}
            except Exception:
                pass

    def _save(self):
        os.makedirs(os.path.dirname(self._file) or ".", exist_ok=True)
        try:
            with open(self._file, "w") as f:
                json.dump(self._patterns, f, indent=2)
        except Exception:
            pass

    def record(self, request_rate: float):
        hour = datetime.now(timezone.utc).hour
        if hour not in self._patterns:
            self._patterns[hour] = []
        self._patterns[hour].append(request_rate)
        # Keep last 7 readings per hour
        self._patterns[hour] = self._patterns[hour][-7:]
        self._save()

    def expected_rate(self, hour: int) -> float | None:
        readings = self._patterns.get(hour)
        if readings:
            return sum(readings) / len(readings)
        # Fall back to default multiplier × 100 (assume 100 req/s baseline)
        mult = self._defaults.get(hour)
        return mult * 100 if mult else None

    def pre_scale_signal(self) -> tuple[bool, float | None]:
        """
        Returns (should_pre_scale, expected_rate_at_peak).
        Fires when the next hour's expected rate is significantly higher
        than current rate and we're within pre_scale_minutes of that hour.
        """
        now = datetime.now(timezone.utc)
        minutes_until_next_hour = 60 - now.minute

        if minutes_until_next_hour > self._pre_scale_minutes:
            return False, None

        next_hour = (now.hour + 1) % 24
        current_expected = self.expected_rate(now.hour)
        next_expected = self.expected_rate(next_hour)

        if current_expected and next_expected:
            if next_expected > current_expected * 1.20:  # 20% jump expected
                logger.info(
                    "Traffic pattern pre-scale: next hour expected %.0f req/s "
                    "vs current %.0f req/s — pre-scaling in %d min",
                    next_expected, current_expected, minutes_until_next_hour
                )
                return True, next_expected

        return False, None


# ── Hot Reload Watcher ────────────────────────────────────────────────────────

class ConfigWatcher:
    """
    Watches alomp_config.yaml for changes and triggers hot reload.
    ACOMP updates its mode and configuration within 5 seconds of
    any file change — no pod restart required.
    """

    def __init__(self, config_path: str, on_change_callback):
        self._path = config_path
        self._callback = on_change_callback
        self._last_mtime = 0.0
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._watch, daemon=True)
        self._thread.start()
        logger.info("Config hot-reload watcher started for %s", self._path)

    def stop(self):
        self._running = False

    def _watch(self):
        while self._running:
            try:
                mtime = os.path.getmtime(self._path)
                if mtime != self._last_mtime and self._last_mtime > 0:
                    logger.info("Config file changed — hot-reloading")
                    self._callback()
                self._last_mtime = mtime
            except Exception as e:
                logger.debug("Config watcher error: %s", e)
            time.sleep(5)


# ── Main Adaptive Engine ──────────────────────────────────────────────────────

class AdaptiveEngine:
    """
    Central coordinator for all adaptive ACOMP features.

    Loaded once at startup from alomp_config.yaml and hot-reloaded
    on file changes. Main loop calls evaluate() each cycle to get
    the current effective configuration.
    """

    def __init__(self, config_path: str):
        self._config_path = config_path
        self._raw: dict = {}
        self._mode: ModeConfig = BUILTIN_MODES["strategic"]
        self._events: list[EventProfile] = []
        self._maintenance: list[MaintenanceWindow] = []
        self._budget_tracker: AnomalyBudgetTracker | None = None
        self._pattern_tracker: TrafficPatternTracker | None = None
        self._watcher = ConfigWatcher(config_path, self._reload)
        self._load()
        self._watcher.start()

    def _load(self):
        try:
            with open(self._config_path) as f:
                self._raw = yaml.safe_load(f)
            self._parse()
            logger.info("AdaptiveEngine loaded — mode=%s", self._mode.name)
        except Exception as e:
            logger.error("Failed to load adaptive config: %s", e)

    def _reload(self):
        self._load()
        logger.info("AdaptiveEngine hot-reloaded — mode=%s", self._mode.name)

    def _parse(self):
        raw = self._raw

        # ── Mode ────────────────────────────────────────────────────
        mode_name = raw.get("mode", "strategic")
        mode_raw = raw.get("modes", {}).get(mode_name, {})
        if mode_raw:
            self._mode = ModeConfig(
                name=mode_name,
                description=mode_raw.get("description", ""),
                cpu_threshold=mode_raw.get("cpu_threshold", 0.70),
                slope_threshold=mode_raw.get("slope_threshold", 0.15),
                slope_min_cycles=mode_raw.get("slope_min_cycles", 2),
                pre_scale_cpu_override=mode_raw.get("pre_scale_cpu_override", 0.50),
                cooldown_seconds=mode_raw.get("cooldown_seconds", 30),
                hysteresis_cycles=mode_raw.get("hysteresis_cycles", 10),
                poll_fast_seconds=mode_raw.get("poll_fast_seconds", 5),
                poll_base_seconds=mode_raw.get("poll_base_seconds", 15),
                poll_slow_seconds=mode_raw.get("poll_slow_seconds", 30),
                max_replicas_cap=mode_raw.get("max_replicas_cap", 10),
                propagation_threshold=mode_raw.get("propagation_threshold", 0.30),
                slo_violation_window=mode_raw.get("slo_violation_window", 3),
            )
        else:
            self._mode = BUILTIN_MODES.get(mode_name, BUILTIN_MODES["strategic"])

        # ── Events ──────────────────────────────────────────────────
        self._events = []
        for e in raw.get("events", []):
            try:
                self._events.append(EventProfile(
                    name=e["name"],
                    status=e.get("status", "inactive"),
                    start=datetime.fromisoformat(e["start"].replace("Z", "+00:00")),
                    end=datetime.fromisoformat(e["end"].replace("Z", "+00:00")),
                    expected_users=e.get("expected_users", 200),
                    baseline_users=e.get("baseline_users", 200),
                    pre_scale_hours=e.get("pre_scale_hours", 2),
                    rampdown_minutes=e.get("rampdown_minutes", 30),
                    mode_override=e.get("mode_override", "aggressive"),
                    notes=e.get("notes", ""),
                ))
            except Exception as ex:
                logger.warning("Could not parse event %s: %s", e.get("name"), ex)

        # ── Maintenance Windows ──────────────────────────────────────
        self._maintenance = []
        for m in raw.get("maintenance_windows", []):
            self._maintenance.append(MaintenanceWindow(
                name=m["name"],
                status=m.get("status", "inactive"),
                cron=m.get("cron", ""),
                duration_minutes=m.get("duration_minutes", 30),
                affected_services=m.get("affected_services", []),
                action=m.get("action", "freeze_scaling"),
                notes=m.get("notes", ""),
            ))

        # ── Anomaly Budgets ──────────────────────────────────────────
        budget_cfg = raw.get("anomaly_budgets", {})
        if budget_cfg.get("enabled", False):
            self._budget_tracker = AnomalyBudgetTracker(
                config=budget_cfg.get("services", {}),
                budget_file=budget_cfg.get("budget_file", "results/anomaly_budgets.json"),
            )

        # ── Traffic Patterns ─────────────────────────────────────────
        tp_cfg = raw.get("traffic_patterns", {})
        if tp_cfg.get("enabled", False):
            self._pattern_tracker = TrafficPatternTracker(
                pattern_file=tp_cfg.get("pattern_file", "results/traffic_patterns.json"),
                pre_scale_minutes=tp_cfg.get("pre_scale_minutes", 15),
                hourly_defaults=tp_cfg.get("hourly_defaults", {}),
            )

    # ── Public API ────────────────────────────────────────────────────

    def evaluate(self, snapshot=None) -> "AdaptiveContext":
        """
        Called every control cycle. Returns AdaptiveContext with:
        - effective_mode: current ModeConfig (may differ from base due to events)
        - frozen_services: services where scaling is suppressed (maintenance)
        - pre_scale_signal: True when event/pattern pre-scaling should fire
        - slo_alerts: services with exhausted anomaly budgets
        """
        ctx = AdaptiveContext(base_mode=self._mode)

        # ── 1. Check active events ───────────────────────────────────
        for event in self._events:
            if event.is_active_now():
                override_mode = BUILTIN_MODES.get(
                    event.mode_override, self._mode
                )
                ctx.effective_mode = override_mode
                ctx.active_event = event
                ctx.pre_scale_signal = True
                logger.info(
                    "Event active: %s — mode overridden to %s",
                    event.name, override_mode.name
                )
                break
            elif event.is_pre_scale_window():
                ctx.pre_scale_signal = True
                ctx.active_event = event
                logger.info(
                    "Pre-scale window: %s starts in <%.0fh — pre-scaling",
                    event.name, event.pre_scale_hours
                )
                break

        # ── 2. Check maintenance windows ─────────────────────────────
        for window in self._maintenance:
            if window.is_active_now():
                svcs = window.affected_services or []
                ctx.frozen_services.update(svcs)
                ctx.active_maintenance = window
                logger.info(
                    "Maintenance window active: %s — freezing: %s",
                    window.name, svcs or "all"
                )

        # ── 3. Check anomaly budgets ──────────────────────────────────
        if self._budget_tracker and snapshot:
            for svc_name in (snapshot.services or {}):
                svc = snapshot.services[svc_name]
                if (svc.latency_p99_ms and
                        svc.latency_p99_ms > SLO_THRESHOLD_MS and
                        self._budget_tracker):
                    self._budget_tracker.record_violation(svc_name)
                if (self._budget_tracker and
                        self._budget_tracker.budget_exhausted(svc_name)):
                    ctx.budget_exhausted_services[svc_name] = (
                        self._budget_tracker.conservative_threshold(svc_name)
                    )

        # ── 4. Traffic pattern pre-scale ─────────────────────────────
        if self._pattern_tracker:
            if snapshot and snapshot.request_rate:
                self._pattern_tracker.record(snapshot.request_rate)
            pattern_signal, expected_rate = self._pattern_tracker.pre_scale_signal()
            if pattern_signal:
                ctx.pre_scale_signal = True
                ctx.pattern_expected_rate = expected_rate

        return ctx

    def current_mode(self) -> ModeConfig:
        return self._mode

    def budget_summary(self) -> dict:
        if self._budget_tracker:
            return self._budget_tracker.status_summary()
        return {}

    def suggest_event_scaling(self, event_name: str,
                               current_replicas: dict[str, int]) -> dict:
        """
        Given an event name and current replica counts, suggest
        pre-scaled replica counts and estimated cost delta.
        """
        event = next((e for e in self._events if e.name == event_name), None)
        if not event:
            return {"error": f"Event '{event_name}' not found"}

        multiplier = event.load_multiplier()
        suggestions = {}
        for svc, current in current_replicas.items():
            suggested = max(1, math.ceil(current * multiplier))
            suggestions[svc] = {
                "current": current,
                "suggested": suggested,
                "delta": suggested - current,
            }

        total_delta = sum(v["delta"] for v in suggestions.values())
        cost_delta_per_hr = total_delta * 0.05  # rough €/hr per replica

        return {
            "event": event_name,
            "load_multiplier": f"{multiplier:.1f}x",
            "expected_users": event.expected_users,
            "mode_override": event.mode_override,
            "pre_scale_hours_before": event.pre_scale_hours,
            "suggestions": suggestions,
            "total_additional_replicas": total_delta,
            "estimated_cost_delta_per_hr": f"€{cost_delta_per_hr:.2f}",
            "notes": event.notes,
        }


@dataclass
class AdaptiveContext:
    """Result of one AdaptiveEngine.evaluate() call."""
    base_mode: ModeConfig
    effective_mode: ModeConfig = None
    active_event: EventProfile | None = None
    active_maintenance: MaintenanceWindow | None = None
    frozen_services: set = field(default_factory=set)
    budget_exhausted_services: dict = field(default_factory=dict)
    pre_scale_signal: bool = False
    pattern_expected_rate: float | None = None

    def __post_init__(self):
        if self.effective_mode is None:
            self.effective_mode = self.base_mode

    def service_frozen(self, service_name: str) -> bool:
        return (service_name in self.frozen_services or
                (self.active_maintenance is not None and
                 len(self.active_maintenance.affected_services) == 0))

    def cpu_threshold_for(self, service_name: str) -> float:
        # Budget-exhausted services get conservative threshold
        if service_name in self.budget_exhausted_services:
            return self.budget_exhausted_services[service_name]
        return self.effective_mode.cpu_threshold
