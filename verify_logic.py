INF = float("inf")

# Example graph from the assignment
edges = [
    (0, 1, 7),
    (0, 2, 3),
    (1, 3, 9),
    (2, 4, 4),
    (3, 4, 6),
    (1, 4, 2),
]
num_nodes = 5
source = 0

distances = {n: (0 if n == source else INF) for n in range(num_nodes)}

for it in range(num_nodes - 1):
    candidates = {}
    for u, v, w in edges:
        if distances[u] < INF:
            cand = distances[u] + w
            if v not in candidates or cand < candidates[v]:
                candidates[v] = cand

    new_distances = dict(distances)
    changed = False
    for v, cand in candidates.items():
        if cand < new_distances[v]:
            new_distances[v] = cand
            changed = True

    distances = new_distances
    if not changed:
        print(f"Converged after {it+1} iterations")
        break

print("Shortest distances from node", source)
for n in range(num_nodes):
    d = distances[n]
    print(f"Node {n}: {'INF' if d == INF else int(d)}")

actual = {n: (None if distances[n] == INF else int(distances[n])) for n in range(num_nodes)}
print("\nComputed (directed-graph) shortest distances:", actual)
print("Note: the assignment PDF's sample output lists Node 3: 10, but the only path")
print("into node 3 in its own sample edge list is 0->1->3 (7+9=16); 10 is not")
print("reachable from the given directed edges. This script's result (16) is the")
print("mathematically correct shortest distance for that input -- verified by hand")
print("above. Nodes 0, 1, 2, and 4 match the PDF's sample output exactly.")
