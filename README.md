# Cloud Computing VM Project

**Running Dijkstra's Shortest Path with Apache Spark on Azure VMs**

This project implements a distributed version of **Dijkstra's Shortest Path Algorithm** using **Apache Spark**. It is designed to run on an **Azure Virtual Machine Spark Standalone Cluster**, but it can also be tested locally before deployment.

---

## Project Structure

| File | Description |
|------|-------------|
| `dijkstra_spark.py` | Main Spark application that loads a graph, computes shortest paths from a source node, and prints or saves the results. |
| `generate_graph.py` | Generates a random weighted graph in the required edge-list format for testing. |
| `data/weighted_graph.txt` | Sample graph containing **10,000 nodes** and **100,000 weighted edges**. |
| `Programming Assignment Report.pdf` | Project report covering implementation details, testing, performance, and challenges encountered. |

---

# Running the Project Locally

Before deploying to Azure, verify that everything works on your local machine.

## 1. Install Requirements

Create a virtual environment and install PySpark.

```bash
python3 -m venv venv
source venv/bin/activate
pip install pyspark
```

> **Note:** Spark already includes PySpark, so this is sufficient for local testing.

---

## 2. Generate a Test Graph (Optional)

You can use the provided graph in the `data` directory or generate a new one.

```bash
python3 generate_graph.py 10000 100000 data/weighted_graph.txt 20 42
```

Arguments:

```
generate_graph.py <nodes> <edges> <output_file> <max_weight> <random_seed>
```

---

## 3. Run the Spark Application

```bash
spark-submit dijkstra_spark.py data/weighted_graph.txt 0 output.txt
```

Command format:

```text
<input_file> <source_node> [output_file]
```

---

# Deploying to Azure

The application was tested on a **Spark Standalone Cluster** running on Azure Virtual Machines.

You may use:

- One VM acting as both **Master** and **Worker**, or
- Multiple worker VMs for improved scalability.

---

# Example Output

```text
Shortest distances from node 0:

Node 0: 0
Node 1: ...
Node 2: ...
Node 3: ...
```

---

# Technologies Used

- Apache Spark
- PySpark
- Python 3
- Microsoft Azure Virtual Machines
- Spark Standalone Cluster
- Ubuntu 22.04
- OpenJDK 11

---
