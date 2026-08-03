import heapq
import time

INF = float("inf")


def load(path):
    with open(path) as f:
        header = f.readline()
        num_nodes, num_edges = map(int, header.split())
        edges = []
        for line in f:
            u, v, w = line.split()
            edges.append((int(u), int(v), float(w)))
    return num_nodes, edges


def classic_dijkstra(num_nodes, edges, source):
    adj = [[] for _ in range(num_nodes)]
    for u, v, w in edges:
        adj[u].append((v, w))
    dist = [INF] * num_nodes
    dist[source] = 0
    pq = [(0, source)]
    while pq:
        d, u = heapq.heappop(pq)
        if d > dist[u]:
            continue
        for v, w in adj[u]:
            nd = d + w
            if nd < dist[v]:
                dist[v] = nd
                heapq.heappush(pq, (nd, v))
    return dist


def relaxation_sssp(num_nodes, edges, source, max_iters=None):
    if max_iters is None:
        max_iters = num_nodes - 1
    dist = [INF] * num_nodes
    dist[source] = 0
    iters_run = 0
    for i in range(max_iters):
        iters_run = i + 1
        changed = False
        new_dist = dist[:]
        for u, v, w in edges:
            if dist[u] < INF:
                cand = dist[u] + w
                if cand < new_dist[v]:
                    new_dist[v] = cand
                    changed = True
        dist = new_dist
        if not changed:
            break
    return dist, iters_run


if __name__ == "__main__":
    num_nodes, edges = load("data/weighted_graph.txt")
    source = 0

    t0 = time.time()
    ref = classic_dijkstra(num_nodes, edges, source)
    t1 = time.time()
    print(f"Reference (heap-based) Dijkstra: {t1 - t0:.3f}s")

    t0 = time.time()
    relaxed, iters_run = relaxation_sssp(num_nodes, edges, source)
    t1 = time.time()
    print(f"Relaxation-based SSSP (same algorithm the Spark job runs): {t1 - t0:.3f}s, {iters_run} iterations")

    mismatches = 0
    for n in range(num_nodes):
        a = ref[n]
        b = relaxed[n]
        if a == INF and b == INF:
            continue
        if a != b:
            mismatches += 1
            if mismatches <= 5:
                print(f"MISMATCH node {n}: ref={a} relaxed={b}")

    reachable = sum(1 for d in ref if d < INF)
    print(f"Reachable nodes from source {source}: {reachable}/{num_nodes}")
    print(f"Mismatches between reference Dijkstra and relaxation SSSP: {mismatches}")
