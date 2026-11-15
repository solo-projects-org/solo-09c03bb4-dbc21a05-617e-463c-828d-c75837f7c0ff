"""Propagation tracing for hourly ammonia-nitrogen surges.

Pure standard-library logic (no FastAPI/SQLite imports) so the matching
algorithm can be unit-tested in isolation, including under ``python -I``.

Definitions
-----------
* An **increment** is the change between two adjacent on-the-hour UTC
  observations.  As specified, increments are taken at ending hours
  01:00..23:00 of each day (the 00:00 delta would need the previous day).
* A **surge** is an increment ``>= threshold`` (threshold must be positive).
* Upstream surges are read from the selected UTC date ``D`` only.
* Downstream surges are read from ``D`` **and** ``D + 1`` so a surge that
  crosses midnight can still be attributed to an upstream event.
* An upstream surge at ``u`` can explain a downstream surge at ``d`` when
  ``1 <= d - u <= max_lag_hours``.

Matching objective: each surge takes part in at most one pair (in particular
every downstream surge is matched at most once).  We first maximise the
number of pairs, then minimise the sum of lags.  This is a min-cost
maximum-cardinality bipartite matching, solved with successive shortest
augmenting paths (costs are the lags, so residual edges may be negative).
"""
from __future__ import annotations

from datetime import date as date_cls, datetime, timedelta
from typing import Iterable

HOURS = tuple(f"{h:02d}:00" for h in range(24))
INCREMENT_HOURS = tuple(range(1, 24))  # 01:00 .. 23:00


class AnalysisInputError(ValueError):
    """The supplied day archives cannot be interpreted."""


def parse_date(value: str) -> date_cls:
    try:
        parsed = date_cls.fromisoformat(value)
    except ValueError as exc:
        raise AnalysisInputError(f"invalid calendar date: {value}") from exc
    if parsed.isoformat() != value:
        raise AnalysisInputError(f"date must be YYYY-MM-DD: {value}")
    return parsed


def next_date(value: str) -> str:
    return (parse_date(value) + timedelta(days=1)).isoformat()


def rows_to_values(rows: Iterable[dict], label: str) -> dict[str, float]:
    """Map ``HH:00`` -> value, requiring exactly one value for every hour."""
    values: dict[str, float] = {}
    for row in rows:
        time = row.get("time")
        if time not in HOURS:
            raise AnalysisInputError(f"{label}: unexpected hour {time!r}")
        if time in values:
            raise AnalysisInputError(f"{label}: duplicate hour {time}")
        value = row.get("nh3n")
        if not isinstance(value, (int, float)) or value != value:
            raise AnalysisInputError(f"{label}: non-numeric NH3-N at {time}")
        values[time] = float(value)
    missing = [h for h in HOURS if h not in values]
    if missing:
        raise AnalysisInputError(f"{label}: incomplete day, missing {missing}")
    return values


def day_increments(values: dict[str, float], day: str) -> list[dict]:
    """Increments ending at 01:00..23:00 for one UTC day."""
    out = []
    for hour in INCREMENT_HOURS:
        delta = round(values[f"{hour:02d}:00"] - values[f"{hour - 1:02d}:00"], 3)
        out.append(
            {
                "date": day,
                "hour": f"{hour:02d}:00",
                "at": datetime.combine(parse_date(day), datetime.min.time())
                + timedelta(hours=hour),
                "delta": delta,
            }
        )
    return out


def find_surges(values: dict[str, float], day: str, threshold: float) -> list[dict]:
    """Positive increments at or above ``threshold`` for one UTC day."""
    surges = []
    for inc in day_increments(values, day):
        # Source values have 3 decimals; compare with a tiny tolerance so an
        # increment equal to the threshold is not lost to float noise.
        if inc["delta"] >= threshold - 1e-9:
            surges.append(
                {
                    "time": inc["at"].isoformat(timespec="minutes"),
                    "date": inc["date"],
                    "hour": inc["hour"],
                    "delta": inc["delta"],
                    "at": inc["at"],
                }
            )
    return surges


def _min_cost_max_matching(
    upstream: list[dict], downstream: list[dict], max_lag: int
) -> list[tuple[int, int]]:
    """Return (upstream_index, downstream_index) pairs.

    Min-cost max-cardinality matching via successive shortest augmenting
    paths.  ``cost(u, d) = lag_hours``; both capacities are 1.
    """
    n_up, n_down = len(upstream), len(downstream)
    if n_up == 0 or n_down == 0:
        return []

    source = 0
    up0 = 1
    down0 = 1 + n_up
    sink = 1 + n_up + n_down
    n_nodes = sink + 1

    # Edge representation for SPFA-based min-cost flow.
    graph: list[list[dict]] = [[] for _ in range(n_nodes)]

    def add_edge(u: int, v: int, cap: int, cost: int) -> None:
        graph[u].append({"to": v, "cap": cap, "cost": cost, "rev": len(graph[v])})
        graph[v].append({"to": u, "cap": 0, "cost": -cost, "rev": len(graph[u]) - 1})

    for i in range(n_up):
        add_edge(source, up0 + i, 1, 0)
    for j in range(n_down):
        add_edge(down0 + j, sink, 1, 0)

    candidates: list[tuple[int, int, int]] = []
    for i, up in enumerate(upstream):
        for j, down in enumerate(downstream):
            lag = int((down["at"] - up["at"]).total_seconds() // 3600)
            if 1 <= lag <= max_lag:
                candidates.append((i, j, lag))

    # Deterministic edge insertion: downstream order, then upstream order.
    for i, j, lag in sorted(candidates, key=lambda c: (c[1], c[0])):
        add_edge(up0 + i, down0 + j, 1, lag)

    flow = 0
    INF = 10**9
    while True:
        dist = [INF] * n_nodes
        in_queue = [False] * n_nodes
        prev_node = [-1] * n_nodes
        prev_edge = [-1] * n_nodes
        dist[source] = 0
        queue = [source]
        in_queue[source] = True
        # SPFA (residual edges can have negative cost).
        while queue:
            u = queue.pop(0)
            in_queue[u] = False
            for ei, edge in enumerate(graph[u]):
                if edge["cap"] <= 0:
                    continue
                nd = dist[u] + edge["cost"]
                if nd < dist[edge["to"]]:
                    dist[edge["to"]] = nd
                    prev_node[edge["to"]] = u
                    prev_edge[edge["to"]] = ei
                    if not in_queue[edge["to"]]:
                        in_queue[edge["to"]] = True
                        queue.append(edge["to"])
        if dist[sink] == INF:
            break
        v = sink
        while v != source:
            u = prev_node[v]
            ei = prev_edge[v]
            edge = graph[u][ei]
            edge["cap"] -= 1
            graph[v][edge["rev"]]["cap"] += 1
            v = u
        flow += 1

    pairs = []
    for i in range(n_up):
        for edge in graph[up0 + i]:
            # A saturated forward edge to a downstream node means it carries flow.
            if down0 <= edge["to"] < sink and edge["cap"] == 0:
                # Ignore edges that were inserted with cap 0 (residuals).
                original = graph[edge["to"]][edge["rev"]]
                if original["cap"] == 1:
                    pairs.append((i, edge["to"] - down0))
    return pairs


def match_surges(
    upstream: list[dict], downstream: list[dict], max_lag: int
) -> list[dict]:
    """Pair upstream/downstream surges, max count then min total lag.

    The reported list is sorted for display only; the assignment itself is
    optimal and deterministic (edges are inserted in a fixed order).
    """
    up_sorted = sorted(upstream, key=lambda s: s["at"])
    down_sorted = sorted(downstream, key=lambda s: s["at"])
    pairs = _min_cost_max_matching(up_sorted, down_sorted, max_lag)
    matches = []
    for i, j in pairs:
        up, down = up_sorted[i], down_sorted[j]
        lag = int((down["at"] - up["at"]).total_seconds() // 3600)
        matches.append(
            {
                "upstream_time": up["time"],
                "downstream_time": down["time"],
                "upstream_date": up["date"],
                "downstream_date": down["date"],
                "lag_hours": lag,
                "upstream_delta": up["delta"],
                "downstream_delta": down["delta"],
            }
        )
    matches.sort(key=lambda m: (m["downstream_time"], m["upstream_time"], m["lag_hours"]))
    return matches


def analyze_days(
    upstream_rows: list[dict],
    downstream_rows: list[dict],
    downstream_next_rows: list[dict],
    date: str,
    threshold: float,
    max_lag: int,
) -> dict:
    """Compute surges and matches from three complete published day archives."""
    day = parse_date(date)
    day_next = (day + timedelta(days=1)).isoformat()
    if threshold <= 0:
        raise AnalysisInputError("threshold must be a positive number")
    if not (1 <= max_lag <= 6):
        raise AnalysisInputError("max lag must be between 1 and 6 hours")

    up_values = rows_to_values(upstream_rows, f"upstream {date}")
    down_values = rows_to_values(downstream_rows, f"downstream {date}")
    next_values = rows_to_values(downstream_next_rows, f"downstream {day_next}")

    upstream_surges = find_surges(up_values, date, threshold)
    downstream_surges = find_surges(down_values, date, threshold) + find_surges(
        next_values, day_next, threshold
    )
    upstream_surges.sort(key=lambda s: s["time"])

    matches = match_surges(upstream_surges, downstream_surges, max_lag)
    return {
        "upstream_surges": [
            {"time": s["time"], "date": s["date"], "hour": s["hour"], "delta": s["delta"]}
            for s in upstream_surges
        ],
        "downstream_surges": [
            {"time": s["time"], "date": s["date"], "hour": s["hour"], "delta": s["delta"]}
            for s in downstream_surges
        ],
        "matches": matches,
        "match_count": len(matches),
        "total_lag_hours": sum(m["lag_hours"] for m in matches),
    }
