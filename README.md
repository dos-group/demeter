# Demeter: Resource-Efficient Distributed Stream Processing under Dynamic Loads with Multi-Configuration Optimization

This repository is associated with the scientific paper titled [Demeter: Resource-Efficient Distributed Stream Processing under Dynamic Loads with Multi-Configuration Optimization](https://arxiv.org/abs/2403.02129). Herein, we provide the prototype, data, and experimental artifacts that form the basis of our research.

## Paper Abstract

Distributed Stream Processing (DSP) focuses on the near real-time processing of large streams of unbounded data. To increase processing capacities, DSP systems are able to dynamically scale across a cluster of commodity nodes, ensuring a good Quality of Service despite variable workloads. However, selecting scaleout configurations which maximize resource utilization remains a challenge. This is especially true in environments where workloads change over time and node failures are all but inevitable. Furthermore, configuration parameters such as memory allocation and checkpointing intervals impact performance and resource usage as well. Sub-optimal configurations easily lead to high operational costs, poor performance, or unacceptable loss of service. 

In this paper, we present Demeter, a method for dynamically optimizing key DSP system configuration parameters for resource efficiency. Demeter uses Time Series Forecasting to predict future workloads and Multi-Objective Bayesian Optimization to model runtime behaviors in relation to parameter settings and workload rates. Together, these techniques allow us to determine whether or not enough is known about the predicted workload rate to proactively initiate short-lived parallel profiling runs for data gathering. Once trained, the models guide the adjustment of multiple, potentially dependent system configuration parameters ensuring optimized performance and resource usage in response to changing workload rates. Our experiments on a commodity cluster using Apache Flink demonstrate that Demeter significantly improves the operational efficiency of long-running benchmark jobs.

---

## Prerequisites

To ensure a smooth setup and operation of the Demeter prototype, the following prerequisites must be met. These components are essential for creating the required infrastructure to deploy and manage the Demeter system within a distributed stream processing environment:

- **Kubernetes Cluster**: A running Kubernetes cluster is necessary as the foundational platform for deploying the Demeter components. This orchestrates the containerized services, enabling them to scale and interact efficiently. For our experiments, we used Kubernetes version 1.18 or newer due to its support for the required APIs and stability features. 

- **Metrics Server**: The Metrics Server is crucial for monitoring resource usage within the Kubernetes cluster. It collects metrics like CPU and memory usage from each node and pod, providing the data necessary for making scaling decisions. Install it using the following Helm command:

    ```shell
    helm upgrade --install metrics-server bitnami/metrics-server --set apiService.create=true --set "extraArgs={--kubelet-insecure-tls=true,--kubelet-preferred-address-types=InternalIP}"
    ```

- **Flink Operator**: The Flink Operator simplifies the management of Apache Flink applications on Kubernetes, supporting the deployment, scaling, and operational oversight of Flink streaming jobs. It is essential for managing the lifecycle of the DSP jobs within the Demeter framework.

    ```shell
    helm install flink-kubernetes-operator flink-kubernetes-operator --repo https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.6.0 --set webhook.create=false --set metrics.port=9999
    kubectl create -f cluster-role-binding-baseline.yaml
    kubectl create -f cluster-role-binding-demeter.yaml
    ```

- **Prometheus**: Prometheus is used for monitoring and alerting purposes, collecting and storing metrics from the Kubernetes cluster and the applications running on it. It is vital for the analytics component of Demeter, which relies on real-time data to make optimization decisions.

    ```shell
    helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack --version 34.9.0 --set prometheus.service.type=NodePort --set prometheus.service.nodePort=30090 --set prometheus.service.targetPort=9090 --set prometheus.service.port=9090
    kubectl create -f flink-podmonitor-jobmanager.yaml
    kubectl create -f flink-podmonitor-taskmanager.yaml
    ```

- **Chaos Mesh**: Chaos Mesh is utilized for introducing controlled disruptions into the system, such as pod failures, network latency, etc., to test the resilience and adaptability of the Demeter system under adverse conditions.

    ```shell
    helm install chaos-mesh chaos-mesh/chaos-mesh -n=chaos-testing --set chaosDaemon.runtime=containerd --set chaosDaemon.socketPath=/run/containerd/containerd.sock --version 2.3.0 --set dashboard.securityMode=false
    ```

- **Redis**: For state management and intermediate data storage in the DSP jobs, Redis is employed. It offers high performance and persistence, essential for the operational efficiency of streaming applications.

    ```shell
    # Create persistent volumes on each node
    mkdir -p /mnt/fast-disks
    # For nodes with multiple disks
    mkdir -p /mnt/fast-disks/disk1
    # Create the storage class and persistent volumes
    kubectl create -f storage-class-definition.yaml
    kubectl apply -f persistent-volumes.yaml
    # Deploy the Redis cluster
    helm install redis-cluster bitnami/redis-cluster --version 8.6.12 -f values.yaml -n baseline
    ```

Yaml files can be found in the repository under the demeter/optimizer/src/main/resources/kubernetes folder.

---

## Repository Structure

This section outlines the organization of the repository, detailing the purpose and contents of each directory to assist in navigating the project and understanding its components.

- **analytics/**: This directory contains the Python-based analytics component, responsible for managing time series forecasting and multiple Bayesian optimization models. It is intended to be executed within the Kubernetes cluster as a service. The `run.sh` file facilitates local execution for testing purposes.

- **binaries/**: Provides Helm executable for deploying Kubernetes resources and examples of Flink jobs. The actual Flink jobs for experiments are deployed from the public Dockerhub repository `morgel/flink`.

- **datasets/**: Contains the workload behaviors utilized in our experiments. They are packaged as part of the morgel/generators Dockerhub repository and are instantiated in the Kubernetes cluster by the optimizer component using Helm charts.

- **optimizer/**: This folder houses the Java Maven-based optimizer component, which is central to the experimental framework of Demeter. It is responsible for orchestrating the environment setup and executing the experiments. Significant subdirectories include:
  - `src/main/resources/`: Contains Kubernetes helm charts for deploying necessary services (analytics, Flink, generators, etc.) and configuration files for experimental runs (e.g., `baseline.properties`, `demeter.properties`).
  - `src/main/java/de/tu_berlin/dos/demeter/optimizer/`: Includes the `Run.java` file, which determines the experiment to execute. Manual adjustment is required to select the relevant experiments.
  - `src/main/java/de/tu_berlin/dos/demeter/optimizer/execution/`: Holds the execution and configuration context for each experimental run, organized into subfolders for different experiments (baseline, Demeter, etc.). Each contains a `Graph.java` for the state machine and a `Context.java` for configuration settings.

- **results/**: Provides the results from experimental runs, including `app.log` files for logs and a `data` folder containing metrics related to various performance indicators like consumer lag, CPU load, latencies, and throughput.

- **Dockerfile**: Used to create the Docker image for the optimizer component, facilitating deployment within the Kubernetes cluster.

---

## Metrics overview

To view the metrics, the following URL can be used to point to the prometheus metrics server on the kubernetes clsuter. Just replace the placeholder with the actual IP address of the cluster master: 

```shell
http://[IP address of K8s Master]:30090/graph?g0.expr=sum(flink_taskmanager_job_task_operator_myLatencyHistogram%7Bjob_name%3D%22demeter%22%7D)%2Fcount(flink_taskmanager_job_task_operator_myLatencyHistogram%7Bjob_name%3D%22demeter%22%7D)&g0.tab=0&g0.stacked=0&g0.show_exemplars=0&g0.range_input=6h&g1.expr=sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate%7Bjob_name%3D%22demeter%22%7D)&g1.tab=0&g1.stacked=0&g1.show_exemplars=0&g1.range_input=18h&g2.expr=sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max%7Bjob_name%3D%22baseline%22%7D)%2Fcount(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max%7Bjob_name%3D%22baseline%22%7D)&g2.tab=0&g2.stacked=0&g2.show_exemplars=0&g2.range_input=2h&g3.expr=flink_jobmanager_taskSlotsTotal%7Bpod%3D~%22demeter.*%22%2Cnamespace%3D%22demeter%22%7D&g3.tab=0&g3.stacked=0&g3.show_exemplars=0&g3.range_input=12h&g4.expr=flink_jobmanager_numRegisteredTaskManagers%7Bpod%3D~%22%5Edemeter.*%22%7D&g4.tab=0&g4.stacked=0&g4.show_exemplars=0&g4.range_input=18h&g5.expr=sum(flink_taskmanager_Status_JVM_CPU_Load%7Bpod%3D~%22%5Ebaseline.*%22%7D)%2Fcount(flink_taskmanager_Status_JVM_CPU_Load%7Bpod%3D~%22%5Ebaseline.*%22%7D)&g5.tab=0&g5.stacked=0&g5.show_exemplars=0&g5.range_input=1h&g6.expr=rate(container_cpu_usage_seconds_total%7Bnamespace%3D%22demeter%22%2C%20pod%3D~%22analytics.*%22%7D%5B5m%5D)&g6.tab=0&g6.stacked=0&g6.show_exemplars=0&g6.range_input=2h&g7.expr=container_memory_usage_bytes%7Bnamespace%3D%22demeter%22%2C%20pod%3D~%22analytics.*%22%7D&g7.tab=0&g7.stacked=0&g7.show_exemplars=0&g7.range_input=6h&g8.expr=flink_jobmanager_taskSlotsTotal%7Bnamespace%3D%22reactive%22%7D&g8.tab=0&g8.stacked=0&g8.show_exemplars=0&g8.range_input=6h&g9.expr=flink_jobmanager_numRegisteredTaskManagers%7Bnamespace%3D%22reactive%22%7D&g9.tab=0&g9.stacked=0&g9.show_exemplars=0&g9.range_input=18h&g10.expr=sum(flink_taskmanager_Status_JVM_CPU_Load%7Bpod%3D~%22%5Eflink-taskmanager.*%22%7D)%2Fcount(flink_taskmanager_Status_JVM_CPU_Load%7Bpod%3D~%22%5Eflink-taskmanager.*%22%7D)&g10.tab=0&g10.stacked=0&g10.show_exemplars=0&g10.range_input=18h
```

---

## Bibliography

@misc{geldenhuys2024demeter,
      title={Demeter: Resource-Efficient Distributed Stream Processing under Dynamic Loads with Multi-Configuration Optimization}, 
      author={Morgan Geldenhuys and Dominik Scheinert and Odej Kao and Lauritz Thamsen},
      year={2024},
      eprint={2403.02129},
      archivePrefix={arXiv},
      primaryClass={cs.DC}
}
