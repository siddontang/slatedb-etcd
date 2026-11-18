#!/usr/bin/env python3
"""Summarize complete benchmark repeats without pooling latency percentiles."""

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
from statistics import median

from reporting import write_report


def summarize(report):
    samples = report.get("samples", [])
    if (report.get("schema_version") != 2 or not report.get("complete")
            or not report.get("passed") or len(samples) != report.get("expected_samples")
            or not samples or not all(row.get("passed") for row in samples)):
        raise ValueError("A summary requires a complete, passing version 2 report")
    groups, seen = defaultdict(list), set()
    fields = ("backend", "members", "concurrency", "read_percent", "keys", "value_bytes", "requests")
    for row in samples:
        row = dict(row, members=row.get("members", report.get("members", 3)))
        key = tuple(row[field] for field in fields)
        repeat = key + (row["repeat"],)
        if repeat in seen or row["successes"] != row["requests"] or row["errors"]:
            raise ValueError("A summary cannot include duplicate repeats or failed requests")
        seen.add(repeat)
        groups[key].append(row)
    result = []
    for key, rows in sorted(groups.items()):
        metrics = {
            "ops_per_second": [row["successful_ops_per_second"] for row in rows],
            "p50_ms": [row["latency_ms"]["p50"] for row in rows],
            "p99_ms": [row["latency_ms"]["p99"] for row in rows],
            "server_cpu_us_per_op": [row["server_cpu_microseconds_per_success"] for row in rows],
            "average_server_cpu_cores": [row["average_server_cpu_cores"] for row in rows],
            "peak_server_rss_mib": [row["peak_process_rss_bytes"] / 1024**2 for row in rows],
            "client_cpu_us_per_op": [row["client_cpu_seconds"] * 1e6 / row["requests"] for row in rows],
        }
        result.append(dict(zip(fields, key), repeats=len(rows), metrics={
            name: {"median": median(values), "min": min(values), "max": max(values)}
            for name, values in metrics.items()}))
    return {"passed": True, "aggregation": "median, min, and max across repeats; no pooled percentiles",
            "groups": result}


def table(summary):
    lines = ["| Backend | Members | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in summary["groups"]:
        metrics = row["metrics"]
        values = [row["backend"], str(row["members"]), str(row["concurrency"]), str(row["read_percent"])]
        for name, precision in [("ops_per_second", 0), ("p50_ms", 2), ("p99_ms", 2),
                                ("server_cpu_us_per_op", 0), ("peak_server_rss_mib", 1)]:
            values.append(f"{metrics[name]['median']:.{precision}f}")
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = args.report.read_bytes()
    summary = summarize(json.loads(payload))
    summary["source_sha256"] = hashlib.sha256(payload).hexdigest()
    write_report(args.output, summary)
    print(table(summary), end="")


if __name__ == "__main__":
    main()
