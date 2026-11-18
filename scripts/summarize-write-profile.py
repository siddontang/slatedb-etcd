#!/usr/bin/env python3
"""Summarize measured-phase allocation requests and cross-stage task wakes."""

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median


STAGES = ("raft", "wal", "apply")


def sample_rows(sample):
    before = sample["state_counters_before"]
    after = sample["state_counters_after"]
    if len(before) != sample["members"] or len(after) != len(before):
        raise ValueError("Incomplete member snapshots")
    for stage in STAGES:
        totals = Counter()
        for start, end in zip(before, after):
            first, last = start["write_profile"][stage], end["write_profile"][stage]
            for key, value in last.items():
                delta = value - first[key]
                if delta < 0:
                    raise ValueError("A member counter regressed")
                totals[key] += delta
        batches = totals["batches"]
        if not batches:
            raise ValueError("No measured batches for " + stage)
        yield stage, [totals["entries"] / batches, totals["allocation_calls"] / batches,
                      totals["requested_bytes"] / batches,
                      *(totals["wakes_from_" + source] / batches for source in STAGES)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--locks", action="store_true", help="Show the totals for the two service locks")
    args = parser.parse_args()
    report = json.loads(args.report.read_text())
    if not report["passed"] or not report["complete"]:
        parser.error("Use a complete successful benchmark report")
    groups = defaultdict(list)
    locks = defaultdict(Counter)
    for sample in report["samples"]:
        if not sample["passed"] or sample["read_percent"] != 0:
            parser.error("Use successful pure write samples")
        rows = list(sample_rows(sample))
        if args.locks:
            for start, end in zip(sample["state_counters_before"], sample["state_counters_after"]):
                for name in ("forwarding", "wal_stop"):
                    first = start["write_profile"]["locks"][name]
                    last = end["write_profile"]["locks"][name]
                    totals = locks[sample["backend"], sample["concurrency"], name]
                    for field in ("calls", "busy", "wait_ns"):
                        delta = last[field] - first[field]
                        if delta < 0:
                            raise ValueError("A lock counter regressed")
                        totals[field] += delta
        for stage, row in rows:
            groups[sample["backend"], sample["concurrency"], stage].append(row)
    print("| Build | C | Stage | Entries | Allocations | Bytes | From Raft | From WAL | From apply |")
    print("| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for (build, concurrency, stage), rows in sorted(groups.items()):
        values = [f"{median(column):.2f}" for column in zip(*rows)]
        print("|", " | ".join([build, str(concurrency), stage, *values]), "|")

    if args.locks:
        print("\n| Build | C | Lock | Calls | Busy | Total slow path µs |")
        print("| --- | ---: | --- | ---: | ---: | ---: |")
        for (build, concurrency, name), counts in sorted(locks.items()):
            print(f"| {build} | {concurrency} | {name} | {counts['calls']} | {counts['busy']} | "
                  f"{counts['wait_ns'] / 1000:.2f} |")


if __name__ == "__main__":
    main()
