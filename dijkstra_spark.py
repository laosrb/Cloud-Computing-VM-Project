"""
dijkstra_spark.py

Parallel Single-Source Shortest Path (SSSP) on Apache Spark using RDDs.

Dijkstra's algorithm is inherently sequential (it repeatedly picks the single
closest un-visited vertex), so it does not parallelize well as written. The
standard way to compute SSSP in a data-parallel engine like Spark is an
iterative "relaxation" approach in the spirit of Bellman-Ford / Dijkstra's
core relaxation step:

    for each iteration:
        for every edge (u, v, w):
            dist[v] = min(dist[v], dist[u] + w)

This is repeated, in parallel across all edges via RDD transformations,
until no distance changes in an iteration (convergence) or a safe upper
bound on the number of iterations (num_nodes - 1, same bound Bellman-Ford
uses) is reached. Because Spark has no shared mutable priority queue across
executors, this relaxation-until-convergence formulation is the accepted
substitute for "Dijkstra on Spark" in distributed-systems coursework, and it
produces the same shortest-path distances as classic Dijkstra on graphs with
non-negative edge weights.

Usage:
    spark-submit dijkstra_spark.py <input_file> <source_node> [output_file]

Input file format:
    num_nodes num_edges
    u1 v1 weight1
    u2 v2 weight2
    ...

Output format (printed and optionally written to output_file):
    Shortest distances from node <source>:
    Node 0: 0
    Node 1: 7
    ...
    Node k: INF        (if unreachable)
"""

import sys
import time
from pyspark import SparkContext, SparkConf

INF = float("inf")


def parse_args():
    if len(sys.argv) < 3:
        print("Usage: spark-submit dijkstra_spark.py <input_file> <source_node> [output_file]")
        sys.exit(1)
    input_file = sys.argv[1]
    source = int(sys.argv[2])
    output_file = sys.argv[3] if len(sys.argv) > 3 else None
    return input_file, source, output_file


def load_graph(sc, input_file):
    """
    Reads the edge-list file. The first line (num_nodes num_edges) is used
    only to know how many nodes exist (in case some nodes have no outgoing
    OR incoming edges and would otherwise never appear in the edge RDD).
    Returns: (num_nodes, edges_rdd) where edges_rdd contains (u, (v, w)).
    """
    raw = sc.textFile(input_file)
    header = raw.first()
    num_nodes, num_edges = map(int, header.split())

    edge_lines = raw.filter(lambda line: line != header and line.strip() != "")

    def parse_edge(line):
        u, v, w = line.split()
        return (int(u), (int(v), float(w)))

    edges = edge_lines.map(parse_edge)
    return num_nodes, edges


def run_sssp(sc, num_nodes, edges, source, max_iterations=None):
    """
    Iterative relaxation SSSP over RDDs.

    distances: RDD of (node_id, dist)
    edges:     RDD of (u, (v, w))   -- kept fixed and re-joined every round

    Each round:
      1. Join distances with edges on the source vertex u.
      2. Compute candidate distance dist[u] + w for the target vertex v.
      3. Union with current distances and reduceByKey(min) so each node
         keeps the smallest distance seen so far.
      4. Stop when the total distance sum stops decreasing (convergence)
         or after max_iterations rounds.
    """
    if max_iterations is None:
        max_iterations = num_nodes - 1 if num_nodes > 1 else 1

    # Initialize: source = 0, every other known node = INF.
    all_nodes = sc.parallelize(range(num_nodes))
    distances = all_nodes.map(lambda n: (n, 0.0 if n == source else INF)).cache()

    edges.cache()

    prev_total = None
    iterations_run = 0

    for i in range(max_iterations):
        iterations_run = i + 1

        # (u, (dist_u, (v, w))) -> (v, dist_u + w), skipping infinite dist_u
        candidates = (
            distances.join(edges)
            .filter(lambda kv: kv[1][0] < INF)
            .map(lambda kv: (kv[1][1][0], kv[1][0] + kv[1][1][1]))
        )

        new_distances = (
            candidates.union(distances)
            .reduceByKey(min)
            .cache()
        )

        # Convergence check: has the sum of (finite) distances stopped changing?
        finite_sum = new_distances.filter(lambda kv: kv[1] < INF).map(lambda kv: kv[1]).sum()

        distances.unpersist()
        distances = new_distances

        if prev_total is not None and finite_sum == prev_total:
            break
        prev_total = finite_sum

    return distances, iterations_run


def format_results(distances, source, num_nodes):
    results = distances.collect()
    results.sort(key=lambda kv: kv[0])
    lines = [f"Shortest distances from node {source}:"]
    for node, dist in results:
        if dist == INF:
            lines.append(f"Node {node}: INF")
        else:
            # Print as int when it's a whole number, matching the sample output.
            if dist == int(dist):
                dist = int(dist)
            lines.append(f"Node {node}: {dist}")
    return "\n".join(lines)


def main():
    input_file, source, output_file = parse_args()

    conf = SparkConf().setAppName("DijkstraSSSP-Spark")
    sc = SparkContext.getOrCreate(conf)

    try:
        start_time = time.time()

        num_nodes, edges = load_graph(sc, input_file)
        load_time = time.time()

        distances, iterations_run = run_sssp(sc, num_nodes, edges, source)
        compute_time = time.time()

        output = format_results(distances, source, num_nodes)
        format_time = time.time()

        print(output)
        print(f"\n--- Performance ---")
        print(f"Graph load time:   {load_time - start_time:.3f}s")
        print(f"SSSP compute time: {compute_time - load_time:.3f}s ({iterations_run} iterations)")
        print(f"Result format time:{format_time - compute_time:.3f}s")
        print(f"Total time:        {format_time - start_time:.3f}s")

        if output_file:
            with open(output_file, "w") as f:
                f.write(output + "\n")
            print(f"\nResults written to {output_file}")
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
