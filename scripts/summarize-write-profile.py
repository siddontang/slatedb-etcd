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
    args = parser.parse_args()
    report = json.loads(args.report.read_text())
    if not report["passed"] or not report["complete"]:
        parser.error("Use a complete successful benchmark report")
    groups = defaultdict(list)
    for sample in report["samples"]:
        if not sample["passed"] or sample["read_percent"] != 0:
            parser.error("Use successful pure write samples")
        for stage, row in sample_rows(sample):
            groups[sample["backend"], sample["concurrency"], stage].append(row)
    print("| Build | C | Stage | Entries | Allocations | Bytes | From Raft | From WAL | From apply |")
    print("| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for (build, concurrency, stage), rows in sorted(groups.items()):
        values = [f"{median(column):.2f}" for column in zip(*rows)]
        print("|", " | ".join([build, str(concurrency), stage, *values]), "|")


if __name__ == "__main__":
    main()
