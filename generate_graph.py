"""
generate_graph.py

Generates a random weighted directed graph in the edge-list format required
by dijkstra_spark.py, e.g. the assignment's weighted_graph.txt with
10,000 nodes and 100,000 edges.

Usage:
    python generate_graph.py <num_nodes> <num_edges> <output_file> [max_weight] [seed]

Example (matches the assignment's test file size):
    python generate_graph.py 10000 100000 weighted_graph.txt 20 42
"""

import random
import sys


def generate(num_nodes, num_edges, output_file, max_weight=20, seed=42):
    random.seed(seed)

    # Guarantee every node is reachable from node 0 by first laying down a
    # random spanning structure, then filling the remainder with random edges.
    edges = []
    nodes = list(range(num_nodes))
    random.shuffle(nodes)
    connected = {0}
    remaining = [n for n in nodes if n != 0]

    for v in remaining:
        u = random.choice(list(connected))
        w = random.randint(1, max_weight)
        edges.append((u, v, w))
        connected.add(v)

    while len(edges) < num_edges:
        u = random.randint(0, num_nodes - 1)
        v = random.randint(0, num_nodes - 1)
        if u == v:
            continue
        w = random.randint(1, max_weight)
        edges.append((u, v, w))

    edges = edges[:num_edges]

    with open(output_file, "w") as f:
        f.write(f"{num_nodes} {len(edges)}\n")
        for u, v, w in edges:
            f.write(f"{u} {v} {w}\n")

    print(f"Wrote {len(edges)} edges over {num_nodes} nodes to {output_file}")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python generate_graph.py <num_nodes> <num_edges> <output_file> [max_weight] [seed]")
        sys.exit(1)

    num_nodes = int(sys.argv[1])
    num_edges = int(sys.argv[2])
    output_file = sys.argv[3]
    max_weight = int(sys.argv[4]) if len(sys.argv) > 4 else 20
    seed = int(sys.argv[5]) if len(sys.argv) > 5 else 42

    generate(num_nodes, num_edges, output_file, max_weight, seed)
