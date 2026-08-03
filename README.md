# Cloud Computing VM Project

**Running Dijkstra's Shortest Path with Apache Spark on Azure VMs**

This project implements a distributed version of **Dijkstra's Shortest Path Algorithm** using **Apache Spark**. It is designed to run on an **Azure Virtual Machine Spark Standalone Cluster**, but it can also be tested locally before deployment. (Used AI to format README file)

---

## Project Structure

| File | Description |
|------|-------------|
| `dijkstra_spark.py` | Main Spark application that loads a graph, computes shortest paths from a source node, and prints or saves the results. |
| `generate_graph.py` | Generates a random weighted graph in the required edge-list format for testing. |
| `scale_test.py` | Runs local performance and correctness comparisons against a standard heap-based Dijkstra implementation. |
| `verify_logic.py` | Verifies the algorithm using the small 5-node example provided in the assignment. |
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

The output file is optional.

---

# Deploying to Azure

The application was tested on a **Spark Standalone Cluster** running on Azure Virtual Machines.

You may use:

- One VM acting as both **Master** and **Worker**, or
- Multiple worker VMs for improved scalability.

---

## Step 1 — Create Azure Virtual Machines

Login to Azure:

```bash
az login
```

Create a resource group:

```bash
az group create --name spark-rg --location eastus
```

### Create the Master VM

```bash
az vm create \
  --resource-group spark-rg \
  --name spark-master \
  --image Ubuntu2204 \
  --size Standard_D4s_v3 \
  --admin-username azureuser \
  --generate-ssh-keys
```

### Create a Worker VM

```bash
az vm create \
  --resource-group spark-rg \
  --name spark-worker1 \
  --image Ubuntu2204 \
  --size Standard_D4s_v3 \
  --admin-username azureuser \
  --generate-ssh-keys
```

Repeat the worker creation command if additional workers are desired.

---

## Step 2 — Open Required Ports

Spark requires several ports for communication and monitoring.

```bash
az vm open-port --resource-group spark-rg --name spark-master --port 7077 --priority 900

az vm open-port --resource-group spark-rg --name spark-master --port 8080 --priority 901

az vm open-port --resource-group spark-rg --name spark-master --port 4040 --priority 902
```

| Port | Purpose |
|------|---------|
| **7077** | Spark Master communication |
| **8080** | Spark Master Web UI |
| **4040** | Spark Job Monitoring UI |

---

## Step 3 — Install Java and Spark

SSH into each VM:

```bash
ssh azureuser@<public-ip>
```

Install Java:

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk python3-pip
```

Download Spark:

```bash
SPARK_VERSION=3.5.1

wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz

tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz

sudo mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark
```

Configure environment variables:

```bash
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc

source ~/.bashrc
```

Install PySpark:

```bash
pip3 install pyspark
```

Repeat these steps on every VM.

---

## Step 4 — Start the Spark Cluster

### On the Master

```bash
$SPARK_HOME/sbin/start-master.sh
```

The command will output a Master URL similar to:

```text
spark://spark-master:7077
```

or

```text
spark://<master-private-ip>:7077
```

### On Each Worker

```bash
$SPARK_HOME/sbin/start-worker.sh spark://<master-private-ip>:7077
```

Open the Spark Master UI:

```text
http://<master-public-ip>:8080
```

You should see all connected worker nodes listed.

---

## Step 5 — Copy Project Files

From your local machine:

```bash
scp dijkstra_spark.py azureuser@<master-public-ip>:~/

scp data/weighted_graph.txt azureuser@<master-public-ip>:~/
```

---

## Step 6 — Submit the Spark Job

Run the application from the master node.

```bash
spark-submit \
  --master spark://<master-private-ip>:7077 \
  --executor-memory 2g \
  --total-executor-cores 4 \
  dijkstra_spark.py weighted_graph.txt 0 output.txt
```

Monitor the running application at:

```text
http://<master-public-ip>:4040
```

---

## Step 7 — Clean Up Azure Resources

To avoid unnecessary Azure charges, delete the resource group after finishing.

```bash
az group delete --name spark-rg --yes --no-wait
```

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

# Notes

- The graph input must be provided as a weighted edge list.
- The algorithm computes shortest paths from a single source node.
- Results can be printed to the console or saved to an output file.
- Local testing is recommended before deploying to Azure.
