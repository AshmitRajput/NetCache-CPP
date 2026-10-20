#!/usr/bin/env python3
"""Small reproducible workload driver for a running NetCache instance.

The request sequence is generated from a seed and interleaves hot and cold keys so
LRU and cost-aware runs can be compared against the same traffic.
"""
import argparse
import json
import random
import socket
import time


def request(host, port, target, timeout):
    data = ("GET " + target + " HTTP/1.1\r\nHost: bench\r\nConnection: close\r\n\r\n").encode()
    started = time.perf_counter()
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.sendall(data)
        chunks = []
        while True:
            part = sock.recv(65536)
            if not part:
                break
            chunks.append(part)
    response = b"".join(chunks)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    first = response.split(b"\r\n", 1)[0].split()
    status = int(first[1]) if len(first) >= 2 else 0
    headers = response.split(b"\r\n\r\n", 1)[0].lower()
    label = "NONE"
    for line in headers.split(b"\r\n"):
        if line.startswith(b"x-cache:"):
            label = line.split(b":", 1)[1].strip().decode()
            break
    return status, label, elapsed_ms


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--keys", type=int, default=20)
    parser.add_argument("--seed", type=int, default=20261020)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    if args.requests < 1 or args.keys < 1:
        parser.error("--requests and --keys must be positive")

    rng = random.Random(args.seed)
    targets = ["/hdr?v=bench{}&size=256".format(i) for i in range(args.keys)]
    labels = {}
    latencies = []
    failures = 0
    for _ in range(args.requests):
        # Keep a stable hot set while still interleaving cold keys.
        index = rng.randrange(min(4, args.keys)) if rng.random() < 0.7 else rng.randrange(args.keys)
        status, label, elapsed = request(args.host, args.port, targets[index], args.timeout)
        labels[label] = labels.get(label, 0) + 1
        latencies.append(elapsed)
        failures += status < 200 or status >= 500

    latencies.sort()
    result = {
        "seed": args.seed,
        "requests": args.requests,
        "keys": args.keys,
        "labels": labels,
        "failures": failures,
        "mean_ms": sum(latencies) / len(latencies),
        "p95_ms": latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))],
    }
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
