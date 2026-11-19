#!/usr/bin/env python3
"""Translate local process fixtures from etcd v3.6.5 to the service CLI."""

import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit


def local_url(value):
    url = urlsplit(value)
    if (url.scheme not in ("http", "https") or url.hostname not in ("localhost", "127.0.0.1")
            or not url.port or url.path or url.query or url.fragment or url.username or url.password):
        raise ValueError(f"Unsupported fixture URL: {value}")
    return f"{url.scheme}://127.0.0.1:{url.port}"


def service_args(argv=None):
    parser = argparse.ArgumentParser(allow_abbrev=False)
    for flag in ("name", "initial-cluster", "listen-client-urls", "advertise-client-urls",
                 "listen-peer-urls", "initial-advertise-peer-urls", "data-dir"):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--initial-cluster-state", choices=["new"], default="new")
    parser.add_argument("--initial-cluster-token", default="etcd-cluster")
    parser.add_argument("--fixture-client", action="append", default=[])
    parser.add_argument("--snapshot-count")
    parser.add_argument("--listen-metrics-urls")
    passthrough = ("quota-backend-bytes", "strict-reconfig-check", "auth-token", "cert-file",
                   "key-file", "trusted-ca-file", "peer-cert-file", "peer-key-file",
                   "peer-trusted-ca-file", "metrics", "watch-progress-notify-interval",
                   "experimental-watch-progress-notify-interval")
    for flag in passthrough:
        parser.add_argument("--" + flag)
    parser.add_argument("--client-cert-auth", action="store_true")
    args = parser.parse_args(argv)
    peers = dict(entry.split("=", 1) for entry in args.initial_cluster.split(","))
    clients = dict(entry.split("=", 1) for entry in args.fixture_client)
    clients[args.name] = args.advertise_client_urls
    if peers.get(args.name) != args.initial_advertise_peer_urls or clients.keys() != peers.keys():
        parser.error("The fixture must supply one client and one peer URL per member")
    # etcd derives a bootstrap member ID from its advertised peer URL and cluster token.
    ids = {name: int.from_bytes(hashlib.sha1((url + args.initial_cluster_token).encode())
                                .digest()[:8], "big") for name, url in peers.items()}
    listen = urlsplit(local_url(args.listen_client_urls)).netloc
    peer = urlsplit(local_url(args.listen_peer_urls)).netloc
    command = ["--id", str(ids[args.name]), "--listen", listen, "--peer-listen", peer,
               "--dir", args.data_dir, "--cluster-id", args.initial_cluster_token]
    for name in peers:
        command.extend(["--peer", f"{ids[name]}={local_url(peers[name])}",
                        "--client-peer", f"{ids[name]}={local_url(clients[name])}"])
    if args.listen_metrics_urls:
        command.extend(["--listen-metrics-urls",
                        ",".join(local_url(url) for url in args.listen_metrics_urls.split(","))])
    if args.snapshot_count:
        command.extend(["--snapshot-every", args.snapshot_count])
    if args.client_cert_auth:
        command.append("--client-cert-auth")
    for flag in passthrough:
        value = getattr(args, flag.replace("-", "_"))
        if value is not None:
            command.extend(["--" + flag, value])
    return command


if __name__ == "__main__":
    binary = str(Path(sys.argv[0]).absolute().with_name("slatedb-etcd"))
    if sys.argv[1:] == ["--version"]:
        output = subprocess.check_output([binary, "--version"], text=True)
        # The upstream fixture selects test scenarios by the advertised etcd version.
        version = next(line.removeprefix("etcd API version: ") for line in output.splitlines()
                       if line.startswith("etcd API version: "))
        print(output.rstrip())
        print(f"etcd Version: {version}")
    else:
        os.execv(binary, [binary, *service_args()])
