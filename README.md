# Demeter: Resource-Efficient Distributed Stream Processing under Dynamic Loads with Multi-Configuration Optimization

This repository is associated with the scientific paper titled "Demeter: Resource-Efficient Distributed Stream Processing under Dynamic Loads with Multi-Configuration Optimization." Herein, we provide the prototype, data, and experimental artifacts that form the basis of our research.

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

