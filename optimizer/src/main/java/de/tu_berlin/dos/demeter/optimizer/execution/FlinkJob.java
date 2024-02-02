package de.tu_berlin.dos.demeter.optimizer.execution;

import io.fabric8.kubernetes.api.model.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlinkJob {

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class Config {

        public final int scaleOut;
        public final int taskSlots;
        public final float cpuVal;
        public final int memVal;
        public final int chkInt;

        public Config(int scaleOut, int taskSlots, float cpuVal, int memVal, int chkInt) {

            this.scaleOut = scaleOut;
            this.taskSlots = taskSlots;
            this.cpuVal = cpuVal;
            this.memVal = memVal;
            this.chkInt = chkInt;
        }

        @Override
        public String toString() {

            return "{scaleOut: " + scaleOut + ", taskSlots: " + taskSlots + ", cpu: " + cpuVal + ", memory: " + memVal + ", chkInt: " + chkInt + '}';
        }
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    public final int port;
    public final String entryClass;
    public final String jobName;
    public final String brokerList;
    public final String consTopic;
    public final String prodTopic;
    public final Map<String, String> extraArgs;
    public final String chkDir;
    public final String saveDir;

    public final int timeout;

    private Config config;
    private final List<Long> tsList = new ArrayList<>();
    private String jobId;
    private String jobManager;
    private boolean isActive = true;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public FlinkJob(
            int port, String entryClass, String jobName, String brokerList, String consTopic,
            String prodTopic, Map<String, String> extraArgs, String chkDir, String saveDir, int timeout) {

        this.port = port;
        this.entryClass = entryClass;
        this.jobName = jobName;
        this.brokerList = brokerList;
        this.consTopic = consTopic;
        this.prodTopic = prodTopic;
        this.extraArgs = extraArgs;
        this.chkDir = chkDir;
        this.saveDir = saveDir;
        this.timeout = timeout;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    public FlinkJob setConfig(Config config) {

        this.config = config;
        return this;
    }

    public Config getConfig() {

        return this.config;
    }

    public FlinkJob setJobId(String jobId) {

        this.jobId = jobId;
        return this;
    }

    public String getJobId() {

        return this.jobId;
    }

    public String getJobManager() {

        return jobManager;
    }

    public void setJobManager(String jobManager) {

        this.jobManager = jobManager;
    }

    public boolean isActive() {

        return this.isActive;
    }

    public void isActive(boolean isActive) {

        this.isActive = isActive;
    }

    public FlinkJob addTs(long timestamp) {

        this.tsList.add(timestamp);
        return this;
    }

    public long getFirstTs() {

        if (this.tsList.size() > 0) return this.tsList.get(0);
        else throw new IllegalStateException("Timestamps is empty for job: " + this);
    }

    public long getLastTs() {

        if (this.tsList.size() > 0) return this.tsList.get(this.tsList.size() - 1);
        else throw new IllegalStateException("Timestamps is empty for job: " + this);
    }

    public FlinkJob initTsList() {

        this.tsList.clear();
        return this;
    }

    public String getArgs() {

        Map<String, String> args = Map.of(
            "jobName", this.jobName,
            "brokerList", this.brokerList,
            "consumerTopic", this.consTopic,
            "producerTopic", this.prodTopic,
            "chkInterval", String.valueOf(this.config.chkInt),
            "chkDir", this.chkDir);

        return Stream
            .concat(args.entrySet().stream(), this.extraArgs.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            .entrySet().stream()
            .map(e -> String.format("\"--%s\", \"%s\"", e.getKey(), e.getValue()))
            .collect(Collectors.joining(", "));
    }

    public String nodePort() {

        return
            "{ " +
                "\"apiVersion\": \"v1\", " +
                "\"kind\": \"Service\", " +
                "\"metadata\": { \"name\": \"" + this.jobName + "-nodeport\" }, " +
                "\"spec\": { " +
                    "\"type\": \"NodePort\", " +
                    "\"ports\": [{ " +
                        "\"name\": \"rest\", " +
                        "\"port\": 8081, " +
                        "\"targetPort\": 8081, " +
                        "\"nodePort\": " + this.port + " " +
                    "}], " +
                    "\"selector\": { \"app\": \"" + this.jobName + "\", \"component\": \"jobmanager\" } " +
                "} " +
            "}";
    }

    public String deployment() {

        return
            "{ " +
                "\"apiVersion\": \"flink.apache.org/v1beta1\", " +
                "\"kind\": \"FlinkDeployment\", "+
                "\"metadata\": { \"name\": \"" + this.jobName + "\" }, "+
                "\"spec\": { "+
                    "\"image\": \"morgel/flink:1.17.0-java11\", "+
                    "\"flinkVersion\": \"v1_17\", " +
                    "\"imagePullPolicy\": \"Always\", " +
                    "\"flinkConfiguration\": { " +
                        "\"taskmanager.numberOfTaskSlots\": \"" + this.config.taskSlots + "\", " +
                        "\"kubernetes.jobmanager.annotations\": \"prometheus.io/scrape:true,prometheus.io/port:9999\", " +
                        "\"kubernetes.taskmanager.annotations\": \"prometheus.io/scrape:true,prometheus.io/port:9999\", " +
                        "\"metrics.reporter.prom.factory.class\": \"org.apache.flink.metrics.prometheus.PrometheusReporterFactory\"," +
                        "\"metrics.reporter.prom.port\": \"9999\"," +
                        "\"heartbeat.timeout\": \"" + this.timeout + "\", " +
                        "\"heartbeat.interval\": \"5000\", " +
                        "\"state.checkpoints.dir\": \"" + this.chkDir + "\", " +
                        "\"state.savepoints.dir\": \"" + this.saveDir + "\" " +
                    "}, " +
                    "\"podTemplate\": { " +
                        "\"spec\": { " +
                            "\"containers\": [{ " +
                                "\"name\": \"flink-main-container\", " +
                                "\"ports\": [{ \"containerPort\": 9999, \"name\": \"metrics\" }] " +
                            "}] " +
                        "} " +
                    "}, " +
                    "\"serviceAccount\": \"default\", " +
                    "\"jobManager\": { \"resource\": { \"memory\": \"" + this.config.memVal + "m\", \"cpu\": " + this.config.cpuVal + " } }, "+
                    "\"taskManager\": { \"resource\": { \"memory\": \"" + this.config.memVal + "m\", \"cpu\": " + this.config.cpuVal + " } }, "+
                    "\"job\": { " +
                        "\"jarURI\": \"local:///opt/flink/usrlib/jobs-1.0.jar\", " +
                        "\"entryClass\": \"" + this.entryClass + "\", " +
                        "\"args\": [" + getArgs() + "], " +
                        "\"parallelism\": " + this.config.scaleOut + ", " +
                        "\"upgradeMode\": \"savepoint\", " +
                        "\"state\": \"running\" " +
                    "}" +
                "}" +
            "}";
    }

    public String deploymentWithAutoscaler() {

        return
            "{ " +
                "\"apiVersion\": \"flink.apache.org/v1beta1\", " +
                "\"kind\": \"FlinkDeployment\", "+
                "\"metadata\": { \"name\": \"" + this.jobName + "\" }, "+
                "\"spec\": { "+
                    "\"image\": \"morgel/flink:1.17.0-java11\", "+
                    "\"flinkVersion\": \"v1_17\", " +
                    "\"imagePullPolicy\": \"Always\", " +
                    "\"flinkConfiguration\": { " +
                        "\"taskmanager.numberOfTaskSlots\": \"" + this.config.taskSlots + "\", " +
                        "\"kubernetes.jobmanager.annotations\": \"prometheus.io/scrape:true,prometheus.io/port:9999\", " +
                        "\"kubernetes.taskmanager.annotations\": \"prometheus.io/scrape:true,prometheus.io/port:9999\", " +
                        "\"metrics.reporter.prom.factory.class\": \"org.apache.flink.metrics.prometheus.PrometheusReporterFactory\"," +
                        "\"metrics.reporter.prom.port\": \"9999\"," +
                        "\"heartbeat.timeout\": \"" + this.timeout + "\", " +
                        "\"heartbeat.interval\": \"5000\", " +
                        "\"state.checkpoints.dir\": \"" + this.chkDir + "\", " +
                        "\"state.savepoints.dir\": \"" + this.saveDir + "\", " +
                        "\"kubernetes.operator.job.autoscaler.enabled\": \"true\", " +
                        "\"kubernetes.operator.job.autoscaler.stabilization.interval\": \"2m\", " +
                        "\"kubernetes.operator.job.autoscaler.metrics.window\": \"1m\", " +
                        "\"kubernetes.operator.job.autoscaler.target.utilization\": \"0.35\", " +
                        "\"kubernetes.operator.job.autoscaler.target.utilization.boundary\": \"0.15\", " +
                        "\"kubernetes.operator.job.autoscaler.restart.time\": \"1m\", " +
                        "\"kubernetes.operator.job.autoscaler.catch-up.duration\": \"5m\", " +
                        "\"pipeline.max-parallelism\": \"24\" " +
                    "}, " +
                    "\"podTemplate\": { " +
                        "\"spec\": { " +
                            "\"containers\": [{ " +
                                "\"name\": \"flink-main-container\", " +
                                "\"ports\": [{ \"containerPort\": 9999, \"name\": \"metrics\" }] " +
                            "}] " +
                        "} " +
                    "}, " +
                    "\"serviceAccount\": \"default\", " +
                    "\"jobManager\": { \"resource\": { \"memory\": \"" + this.config.memVal + "m\", \"cpu\": " + this.config.cpuVal + " } }, "+
                    "\"taskManager\": { \"resource\": { \"memory\": \"" + this.config.memVal + "m\", \"cpu\": " + this.config.cpuVal + " } }, "+
                    "\"job\": { " +
                        "\"jarURI\": \"local:///opt/flink/usrlib/jobs-1.0.jar\", " +
                        "\"entryClass\": \"" + this.entryClass + "\", " +
                        "\"args\": [" + getArgs() + "], " +
                        "\"parallelism\": " + this.config.scaleOut + ", " +
                        "\"upgradeMode\": \"savepoint\", " +
                        "\"state\": \"running\" " +
                    "}" +
                "}" +
            "}";
    }

    @Override
    public String toString() {
        return "FlinkJob{" +
                "port=" + port +
                ", entryClass='" + entryClass + '\'' +
                ", jobName='" + jobName + '\'' +
                ", brokerList='" + brokerList + '\'' +
                ", consTopic='" + consTopic + '\'' +
                ", prodTopic='" + prodTopic + '\'' +
                ", extraArgs=" + extraArgs +
                ", config=" + config +
                ", tsList=" + tsList +
                ", jobId='" + jobId + '\'' +
                ", isActive=" + isActive +
                '}';
    }
}
