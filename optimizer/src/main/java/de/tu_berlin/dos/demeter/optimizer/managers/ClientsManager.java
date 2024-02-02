package de.tu_berlin.dos.demeter.optimizer.managers;

import com.google.gson.*;
import de.tu_berlin.dos.demeter.optimizer.clients.analytics.AnalyticsClient;
import de.tu_berlin.dos.demeter.optimizer.clients.flink.FlinkClient;
import de.tu_berlin.dos.demeter.optimizer.clients.generator.GeneratorClient;
import de.tu_berlin.dos.demeter.optimizer.clients.kubernetes.Helm;
import de.tu_berlin.dos.demeter.optimizer.clients.kubernetes.Helm.CommandBuilder;
import de.tu_berlin.dos.demeter.optimizer.clients.kubernetes.Helm.CommandBuilder.Command;
import de.tu_berlin.dos.demeter.optimizer.clients.kubernetes.K8sClient;
import de.tu_berlin.dos.demeter.optimizer.clients.prometheus.PrometheusClient;
import de.tu_berlin.dos.demeter.optimizer.execution.FlinkJob;
import de.tu_berlin.dos.demeter.optimizer.utils.FileManager;
import de.tu_berlin.dos.demeter.optimizer.structures.PropertyTree;
import de.tu_berlin.dos.demeter.optimizer.structures.TimeSeries;
import io.fabric8.chaosmesh.v1alpha1.DelaySpec;
import io.fabric8.chaosmesh.v1alpha1.DelaySpecBuilder;
import io.fabric8.chaosmesh.v1alpha1.NetworkChaosBuilder;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static de.tu_berlin.dos.demeter.optimizer.managers.DataManager.*;

public class ClientsManager implements AutoCloseable {

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = LogManager.getLogger(ClientsManager.class);

    private static final String TO_REPLACE = "TO_REPLACE";
    private static final String LATENCY = "flink_taskmanager_job_task_operator_myLatencyHistogram";
    private static final String THROUGHPUT = "flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate";
    private static final String CHECKPOINT_DURATIONS = "flink_jobmanager_job_lastCheckpointDuration";
    private static final String CONSUMER_LAG = "flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max";
    private static final String NUM_TASK_SLOTS = "flink_jobmanager_taskSlotsTotal";
    private static final String SCALE_OUTS = "flink_jobmanager_numRegisteredTaskManagers";
    private static final String CPU_LOAD = "flink_taskmanager_Status_JVM_CPU_Load";

    private static final String NUM_TASKMANAGERS = "flink_jobmanager_numRegisteredTaskManagers";
    private static final String UPTIME = "flink_jobmanager_job_uptime";
    private static final String SINK_REGEX = "^.*Sink.*$";

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static ClientsManager create(
            String namespace, String masterIP, List<String> genPorts, String promPort, String analyticsPort) throws Exception {

        return new ClientsManager(namespace, masterIP, genPorts, promPort, analyticsPort);
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    public final Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
    public final String namespace;
    public final String masterIP;
    public final K8sClient k8sClient;
    public final List<GeneratorClient> generatorsClients = new ArrayList<>();
    public final PrometheusClient prom;
    public final AnalyticsClient analyticsClient;
    public final ResourceDefinitionContext flinkDeploymentContext;

    public final Map<String, FlinkClient> flinkClients = new LinkedHashMap<>();


    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public ClientsManager(
            String namespace, String masterIP, List<String> genPorts, String promPort, String analyticsPort) {

        this.namespace = namespace;
        this.masterIP = masterIP;
        this.k8sClient = new K8sClient();
        genPorts.forEach(port -> this.generatorsClients.add(new GeneratorClient(String.format("%s:%s", masterIP, port), gson)));
        this.prom = new PrometheusClient(String.format("%s:%s", masterIP, promPort), gson);
        this.analyticsClient = new AnalyticsClient(String.format("%s:%s", masterIP, analyticsPort), gson);
        this.flinkDeploymentContext =
            new ResourceDefinitionContext.Builder()
                .withGroup("flink.apache.org")
                .withVersion("v1beta1")
                .withPlural("flinkdeployments")
                .withKind("FlinkDeployment")
                .withNamespaced(true).build();
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void initializeNamespace() throws Exception {

        if (this.k8sClient.namespaceExists(this.namespace)) {

            this.k8sClient.deleteAllGenericResources(this.flinkDeploymentContext, this.namespace);
            this.k8sClient.deleteNamespace(this.namespace);
        }
        this.k8sClient.createOrReplaceNamespace(this.namespace);
    }

    public void deployHelm(PropertyTree deployment) throws Exception {

        LOG.info("Helm deploy: " + deployment);
        // next, install chart based on config
        CommandBuilder builder = CommandBuilder.builder();
        builder.setCommand(Command.INSTALL);
        builder.setName(deployment.key);
        if (deployment.exists("chart")) {

            //String path = FileManager.GET.path(deployment.find("chart").value);
            //builder.setChart(path);

            String path = deployment.find("chart").value;
            // TODO this is a hack so that the optimizer executes with the JAR and in the IDE
            Path jarFilePath = Paths.get(this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!jarFilePath.toString().endsWith(".jar")) {

                path = FileManager.GET.path(deployment.find("chart").value);
            }
            LOG.info(path);
            builder.setChart(path);
        }
        else builder.setChart(deployment.key);
        if (deployment.exists("repo")) builder.setFlag("--repo", deployment.find("repo").value);
        if (deployment.exists("version")) builder.setFlag("--version", deployment.find("version").value);
        if (deployment.exists("values")) {
            // replace temporary placeholders strings in yaml files with namespace
            File orig = FileManager.GET.resource(deployment.find("values").value, File.class);
            String contents = FileManager.GET.toString(orig).replaceAll(TO_REPLACE, this.namespace);
            File temp = FileManager.GET.tmpWithContents(contents);
            builder.setFlag("--values", temp.getAbsolutePath());
        }
        if (deployment.exists("set")) {
            // for every "set" command passed as a list, add them to the command
            deployment.find("set").forEach((setting) -> builder.setFlag("--set", setting.value));
        }
        builder.setNamespace(this.namespace);
        Helm.get.execute(builder.build());
    }

    public void deployYaml(PropertyTree deployment) throws Exception {

        LOG.info("Yaml deploy: " + deployment);
        deployment.forEach(file -> {
            // replace temporary placeholders strings in yaml files with namespace
            File orig = FileManager.GET.resource(file.value, File.class);
            String contents = FileManager.GET.toString(orig).replaceAll(TO_REPLACE, this.namespace);
            File temp = FileManager.GET.tmpWithContents(contents);
            this.k8sClient.createResourceFromFile(this.namespace, temp.getAbsolutePath());
        });
    }

    public void createJob(FlinkJob job) {

        this.k8sClient.createOrReplaceGenericResourceFromJson(this.flinkDeploymentContext, this.namespace, job.deployment());
        this.k8sClient.createServiceFromJson(this.namespace, job.nodePort());
        this.flinkClients.put(job.jobName, new FlinkClient(String.format("%s:%s", masterIP, job.port), gson));
    }

    public void createJob(String deploymentJson, String nodePort, String jobName, int port) {

        this.k8sClient.createOrReplaceGenericResourceFromJson(this.flinkDeploymentContext, this.namespace, deploymentJson);
        this.k8sClient.createServiceFromJson(this.namespace, nodePort);
        this.flinkClients.put(jobName, new FlinkClient(String.format("%s:%s", masterIP, port), gson));
    }

    public void updateJob(FlinkJob job) {

        this.k8sClient.createOrReplaceGenericResourceFromJson(this.flinkDeploymentContext, this.namespace, job.deployment());
    }

    public void deleteJob(FlinkJob job) {

        this.k8sClient.deleteGenericResource(this.flinkDeploymentContext, this.namespace, job.jobName);
        this.k8sClient.deleteService(this.namespace, job.jobName + "-nodeport");
        this.flinkClients.remove(job.jobName);
    }

    public String getJobId(String jobName) throws Exception {

        FlinkClient flink = this.flinkClients.get(jobName);
        List<String> jobIds = this.getJobs(jobName);
        for (String jobId: jobIds) {

            JsonObject json = flink.getJob(jobId);
            if (jobName.equals(json.get("name").getAsString())) return jobId;
        }
        throw new IllegalStateException("Unknown jobName: " + jobName);
    }

    public List<String> getJobs(String jobName) throws Exception {

        FlinkClient flink = this.flinkClients.get(jobName);
        JsonObject json = flink.getJobs();
        List<String> jobs = new ArrayList<>();
        json.get("jobs").getAsJsonArray().forEach(e -> {

            jobs.add(e.getAsJsonObject().get("id").getAsString());
        });
        return jobs;
    }

    public long getLatestChkTs(String jobName, String jobId) throws Exception {

        FlinkClient flink = this.flinkClients.get(jobName);
        JsonObject response = flink.getChkInfo(jobId);
        long lastCkp = response
                .getAsJsonObject("latest").get("completed")
                .getAsJsonObject().get("latest_ack_timestamp").getAsLong();
        return (long) Math.ceil(lastCkp / 1000.0);
    }

    public List<String> getOperatorIds(String jobName, String jobId) throws Exception {

        FlinkClient flink = this.flinkClients.get(jobName);
        JsonObject response = flink.getVertices(jobId);
        JsonArray nodes = response.getAsJsonObject("plan").getAsJsonArray("nodes");
        List<String> operatorIds = new ArrayList<>();
        nodes.forEach(vertex -> operatorIds.add(vertex.getAsJsonObject().get("id").getAsString()));
        return operatorIds;
    }

    public String getJobManager(String jobName) throws Exception {

        List<String> podNames = this.k8sClient.getPods(this.namespace, Map.of("app", jobName, "component", "jobmanager"));
        if (podNames.size() != 1) throw new IllegalStateException("More than one Jobmanager returned: " + Arrays.toString(podNames.toArray()));
        return podNames.get(0);
    }

    public Set<String> getTaskManagers(String jobName, String jobId) throws Exception {

        FlinkClient flink = this.flinkClients.get(jobName);
        List<String> operatorIds = this.getOperatorIds(jobName, jobId);
        Set<String> taskManagers = new HashSet<>();
        for (String id : operatorIds) {

            JsonObject response = flink.getTaskManagers(jobId, id);
            JsonArray arr = response.getAsJsonArray("taskmanagers");
            arr.forEach(taskManager -> taskManagers.add(taskManager.getAsJsonObject().get("taskmanager-id").getAsString()));
        }
        return taskManagers;
    }

    public String getSinkOperatorId(String jobName, String jobId) throws Exception {

        FlinkClient flink = this.flinkClients.get(jobName);
        JsonObject response = flink.getVertices(jobId);
        JsonArray nodes = response.getAsJsonObject("plan").getAsJsonArray("nodes");
        for (JsonElement node : nodes) {

            if (node.getAsJsonObject().get("description").getAsString().matches(SINK_REGEX)) {

                return node.getAsJsonObject().get("id").getAsString();
            }
        }
        throw new IllegalStateException("Unable to find operator with ID: " + SINK_REGEX);
    }

    public long getLatestTs(String jobName, String jobId) throws Exception {

        FlinkClient flink = this.flinkClients.get(jobName);
        long now = flink.getLatestTs(jobId).get("now").getAsLong();
        return (long) Math.ceil(now / 1000.0);
    }

    public long getLastCptTs(String jobName, String jobId) throws Exception {

        FlinkClient flink = this.flinkClients.get(jobName);
        JsonObject response = flink.getChkInfo(jobId);
        long timestamp = response.get("latest").getAsJsonObject().get("completed").getAsJsonObject().get("latest_ack_timestamp").getAsLong();
        return (long) Math.ceil(timestamp / 1000.0);
    }

    public int getScaleOut(String jobName, String jobId) throws Exception {

        FlinkClient flink = this.flinkClients.get(jobName);
        JsonObject response = flink.getJob(jobId);
        return response.get("vertices").getAsJsonArray().get(0).getAsJsonObject().get("parallelism").getAsInt();
    }

    public ClientsManager initGenerators() throws Exception {

        for (GeneratorClient client : this.generatorsClients) {

            String status = client.status().getAsJsonObject().get("status").getAsString();
            while (!"READY".equalsIgnoreCase(status)) {

                client.stop();
                new CountDownLatch(1).await(1000, TimeUnit.MILLISECONDS);
                status = client.status().getAsJsonObject().get("status").getAsString();
            }
        }
        return this;
    }

    public void startGenerators(JsonObject body) throws Exception {

        for (GeneratorClient client : this.generatorsClients) {

            LOG.info(String.format("Generator started with parameters: %s", client.start(body).toString()));
        }
    }

    /*public void startGenerators(JsonObject body, ExecutorService executor) throws InterruptedException {
        int clientCount = this.generatorsClients.size();
        CountDownLatch latch = new CountDownLatch(clientCount);

        for (GeneratorClient client : this.generatorsClients) {
            executor.submit(() -> {
                try {
                    while (true) { // Infinite loop to keep retrying
                        try {
                            LOG.info(String.format("Generator started with parameters: %s", client.start(body).toString()));
                            break; // If successful, break out of the retry loop
                        }
                        catch (Exception e) {
                            LOG.error("Failed to start generator. Retrying...", e);
                        }
                    }
                }
                finally {
                    latch.countDown(); // Ensure that the latch is decremented even if there's an unexpected error
                }
            });
        }
        latch.await(); // This will block until all generator tasks are done
    }*/

    public void injectFailure(String taskManager) throws Exception {

        this.k8sClient.execCommandOnPod(taskManager, this.namespace, "sh", "-c", "kill 1");
    }

    public void injectDelay(Map<String, String> labels, int duration) {

        String name = "delay-" + labels.entrySet().stream().map(e -> String.format("%s-%s", e.getKey(), e.getValue())).collect(Collectors.joining("-"));
        name = name + '-' + RandomStringUtils.random(10, true, true).toLowerCase();
        DelaySpec delay = new DelaySpecBuilder().withLatency("60000ms").withCorrelation("100").withJitter("0ms").build();
        this.k8sClient.createOrReplaceNetworkChaos(this.namespace, new NetworkChaosBuilder()
            .withNewMetadata().withName(name).endMetadata()
            .withNewSpec()
            .withAction("delay")
            .withMode("one")
            .withNewSelector().withLabelSelectors(labels).endSelector()
            .withDelay(delay)
            .withDuration(duration + "s")
            .endSpec()
            .build());
    }

    public TimeSeries getWorkload(ExecutorService executor, long startTs, long stopTs) throws Exception {

        JsonObject body = new JsonObject();
        body.addProperty("startTs", startTs);
        body.addProperty("stopTs", stopTs);
        List<TimeSeries> workloads = new ArrayList<>();
        for (GeneratorClient client : this.generatorsClients) {

            workloads.add(gson.fromJson(client.workload(body), TimeSeries.class));
        }
        return TimeSeries.asyncMerge(executor, workloads);
    }

    public TimeSeries getThr(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
                "sum(%s{task_name=~\"^.*Source.*$\",job_name=\"%s\",namespace=\"%s\"})",
            THROUGHPUT, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getLat(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
                "sum(%s{job_name=\"%s\",quantile=\"0.95\",namespace=\"%s\"})/" +
                "count(%s{job_name=\"%s\",quantile=\"0.95\",namespace=\"%s\"})",
            LATENCY, jobName, this.namespace, LATENCY, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getChkDur(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{job_name=\"%s\",namespace=\"%s\"}",
            CHECKPOINT_DURATIONS, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getLag(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "sum(%s{job_name=\"%s\",namespace=\"%s\"})/count(%s{job_name=\"%s\",namespace=\"%s\"})",
            CONSUMER_LAG, jobName, this.namespace, CONSUMER_LAG, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);

    }

    public TimeSeries getTaskSlots(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{pod=~\"%s.*\", namespace=\"%s\"}",
            NUM_TASK_SLOTS, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getScaleOuts(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{pod=~\"%s.*\", namespace=\"%s\"}",
            SCALE_OUTS, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getCpuLoad(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "sum(%s{pod=~\"%s.*\",namespace=\"%s\"})/count(%s{pod=~\"%s.*\",namespace=\"%s\"})",
            CPU_LOAD, jobName, this.namespace, CPU_LOAD, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getNumWorkers(long startTs, long stopTs) throws Exception {

        String query = String.format(
                "%s{namespace=\"%s\"}",
                NUM_TASKMANAGERS, this.namespace);
        //LOG.info(query);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public long getUptime(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{job_name=\"%s\",namespace=\"%s\"}",
            UPTIME, jobName, this.namespace);

        TimeSeries ts = this.prom.queryRange(query, startTs, stopTs);
        return (ts.getLast() != null && ts.getLast().value != null) ? ts.getLast().value.longValue() / 1000 : 0;
    }

    public JsonObject evaluate(
            String genType, List<Profile> profiles, float maxClusterCpu, int maxClusterMem, TimeSeries tarWld,
            int minScaleOut, int maxScaleOut, int minChkInt, int maxChkInt, float minConCpu, float maxConCpu,
            int minConMem, int maxConMem, int minTaskSlots, int maxTaskSlots, FlinkJob.Config defaultConfigs,
            Profile currentProfile, int recTimeConst, boolean isRescaleRequest
        ) throws Exception {

        JsonArray profilesArr = new JsonArray();
        for (Profile profile : profiles) {
            // remove profiles where recovery time is unknown
            if (profile.recTime == -1) continue;

            JsonObject configJson = new JsonObject();
            configJson.addProperty("scaleout", profile.scaleOut);
            configJson.addProperty("checkpoint_interval", profile.chkInt);
            configJson.addProperty("container_cpu", profile.cpuVal);
            configJson.addProperty("container_memory", profile.memVal);
            configJson.addProperty("task_slots", profile.taskSlots);

            JsonObject metricsJson = new JsonObject();
            metricsJson.addProperty("throughput_rate", profile.avgThr);
            metricsJson.addProperty("latency", profile.avgLat);
            metricsJson.addProperty("recovery_time", profile.recTime);

            JsonObject profileJson = new JsonObject();
            profileJson.add("configs", new Gson().toJsonTree(configJson));
            profileJson.add("metrics", new Gson().toJsonTree(metricsJson));
            profilesArr.add(profileJson);
        }

        JsonObject searchSpaceJson = new JsonObject();
        searchSpaceJson.addProperty("min_scaleout", minScaleOut);
        searchSpaceJson.addProperty("max_scaleout", maxScaleOut);
        searchSpaceJson.addProperty("min_checkpoint_interval", minChkInt);
        searchSpaceJson.addProperty("max_checkpoint_interval", maxChkInt);
        searchSpaceJson.addProperty("min_container_cpu", minConCpu);
        searchSpaceJson.addProperty("max_container_cpu", maxConCpu);
        searchSpaceJson.addProperty("min_container_memory", minConMem);
        searchSpaceJson.addProperty("max_container_memory", maxConMem);
        searchSpaceJson.addProperty("min_task_slots", minTaskSlots);
        searchSpaceJson.addProperty("max_task_slots", maxTaskSlots);

        JsonObject defaultConfigsJson = new JsonObject();
        defaultConfigsJson.addProperty("scaleout", defaultConfigs.scaleOut);
        defaultConfigsJson.addProperty("checkpoint_interval", defaultConfigs.chkInt);
        defaultConfigsJson.addProperty("container_cpu", defaultConfigs.cpuVal);
        defaultConfigsJson.addProperty("container_memory", defaultConfigs.memVal);
        defaultConfigsJson.addProperty("task_slots", defaultConfigs.taskSlots);

        JsonObject currentConfigsJson = new JsonObject();
        currentConfigsJson.addProperty("scaleout", currentProfile.scaleOut);
        currentConfigsJson.addProperty("checkpoint_interval", currentProfile.chkInt);
        currentConfigsJson.addProperty("container_cpu", currentProfile.cpuVal);
        currentConfigsJson.addProperty("container_memory", currentProfile.memVal);
        currentConfigsJson.addProperty("task_slots", currentProfile.taskSlots);

        JsonObject currentMetricsJson = new JsonObject();
        currentMetricsJson.addProperty("throughput_rate", currentProfile.avgThr);
        currentMetricsJson.addProperty("latency", currentProfile.avgLat);
        currentMetricsJson.addProperty("recovery_time", currentProfile.recTime);

        JsonObject currentProfileJson = new JsonObject();
        currentProfileJson.add("configs", new Gson().toJsonTree(currentConfigsJson));
        currentProfileJson.add("metrics", new Gson().toJsonTree(currentMetricsJson));

        JsonObject constraintsJson = new JsonObject();
        constraintsJson.addProperty("recovery_time", recTimeConst);

        JsonObject request = new JsonObject();
        request.addProperty("job", genType);
        request.addProperty("max_cluster_cpu", maxClusterCpu);
        request.addProperty("max_cluster_memory", maxClusterMem);
        request.add("forecast_updates", new Gson().toJsonTree(tarWld));
        request.add("profiles", new Gson().toJsonTree(profilesArr));
        request.add("search_space", new Gson().toJsonTree(searchSpaceJson));
        request.add("default_configs", new Gson().toJsonTree(defaultConfigsJson));
        request.add("current_profile", new Gson().toJsonTree(currentProfileJson));
        request.add("constraints", new Gson().toJsonTree(constraintsJson));

        //LOG.info(request.toString());

        if (isRescaleRequest){
            return this.analyticsClient.rescale(request);
        }
        else {
            return this.analyticsClient.profile(request);
        }

    }

    @Override
    public void close() throws Exception {

        this.k8sClient.close();
    }
}
