package de.tu_berlin.dos.demeter.optimizer.execution.demeter;

import de.tu_berlin.dos.demeter.optimizer.execution.FlinkJob;
import de.tu_berlin.dos.demeter.optimizer.managers.ClientsManager;
import de.tu_berlin.dos.demeter.optimizer.managers.DataManager;
import de.tu_berlin.dos.demeter.optimizer.structures.OrderedProperties;
import de.tu_berlin.dos.demeter.optimizer.structures.PropertyTree;
import de.tu_berlin.dos.demeter.optimizer.utils.EventTimer;
import de.tu_berlin.dos.demeter.optimizer.utils.FileManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class Context implements AutoCloseable {

    Logger LOG = LogManager.getLogger(Context.class);

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static Context create(String propertiesFile) throws Exception {

        return new Context(propertiesFile);
    }

    /******************************************************************************
     * INSTANCE VARIABLES
     ******************************************************************************/

    public final int expId;
    public final String expName;
    public final int expLen;
    public final int evalInt;
    public final int avgWindow;
    public final boolean cleanDb;
    public final String dataPath;
    public final String brokerList;
    public final String tarConsTopic;
    public final String tarProdTopic;
    public final String chkDir;
    public final String saveDir;

    public final int timeout;

    public final String genType;
    public final String limiterType;
    public final float limiterMaxNoise;
    public final String fileName;

    private final Map<Integer, Boolean> nodePorts = new LinkedHashMap<>();

    public final String entryClass;
    public final Map<String, String> extraArgs = new HashMap<>();

    public final FlinkJob.Config defaultConfigs;

    public final FlinkJob job;

    public final int cpuConst;
    public final int memConst;
    public final int recTimeConst;

    public final int minScaleOut;
    public final int maxScaleOut;
    public final int minTaskSlots;
    public final int maxTaskSlots;
    public final int minChkInt;
    public final int maxChkInt;
    public final float minConCpu;
    public final float maxConCpu;
    public final int minConMem;
    public final int maxConMem;

    public final PropertyTree k8s;

    public final ClientsManager cm;
    public final DataManager dm;
    public final EventTimer et;

    public final ExecutorService executor = Executors.newCachedThreadPool();

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private Context(String propertiesFile) throws Exception {

        OrderedProperties props = FileManager.GET.resource(propertiesFile, OrderedProperties.class);

        // general properties
        this.expId = Integer.parseInt(props.getProperty("general.expId"));
        this.expName = props.getProperty("general.expName");
        this.expLen = Integer.parseInt(props.getProperty("general.expLen"));
        this.evalInt = Integer.parseInt(props.getProperty("general.evalInt"));
        this.avgWindow = Integer.parseInt(props.getProperty("general.avgWindow"));
        this.cleanDb = Boolean.parseBoolean(props.getProperty("general.cleanDb"));
        this.dataPath = props.getProperty("general.dataPath");
        this.brokerList = props.getProperty("general.brokerList");
        this.tarConsTopic = props.getProperty("general.consTopic");
        this.tarProdTopic = props.getProperty("general.prodTopic");

        // generator properties
        this.genType = props.getProperty("generators.genType");
        this.limiterType = props.getProperty("generators.limiterType");
        this.limiterMaxNoise = Float.parseFloat(props.getProperty("generators.limiterMaxNoise"));
        this.fileName = props.getProperty("generators.fileName");

        // generate NodePort range
        for (int i = 32007; i <= 33007; i++) this.nodePorts.put(i, true);

        // flink properties
        this.entryClass = props.getProperty("flink.entryClass");
        PropertyTree extraArgs = props.getPropertyList("flink").find("extraArgs");
        if (extraArgs != null) extraArgs.forEach(e -> this.extraArgs.put(e.key, e.value));
        this.chkDir = props.getProperty("flink.chkDir");
        this.saveDir = props.getProperty("flink.saveDir");
        this.timeout = Integer.parseInt(props.getProperty("flink.timeout"));
        this.job = new FlinkJob(
            this.reserveNodePort(), this.entryClass, this.expName,
            this.brokerList, this.tarConsTopic, this.tarProdTopic,
            this.extraArgs, this.chkDir, this.saveDir, this.timeout);
        // set initial config for target flink job
        int scaleOut = Integer.parseInt(props.getProperty("flink.scaleOut"));
        int taskSlots = Integer.parseInt(props.getProperty("flink.taskSlots"));
        float cpu = Float.parseFloat(props.getProperty("flink.cpu"));
        int memory = Integer.parseInt(props.getProperty("flink.memory"));
        int chkInt = Integer.parseInt(props.getProperty("flink.chkInt"));

        this.defaultConfigs = new FlinkJob.Config(scaleOut, taskSlots, cpu, memory, chkInt);
        this.job.setConfig(new FlinkJob.Config(scaleOut, taskSlots, cpu, memory, chkInt));

        // constraints properties
        this.cpuConst = Integer.parseInt(props.getProperty("constraints.cpuConst"));
        this.memConst = Integer.parseInt(props.getProperty("constraints.memConst"));
        this.recTimeConst = Integer.parseInt(props.getProperty("constraints.recTimeConst"));

        // search space properties
        this.minScaleOut = Integer.parseInt(props.getProperty("searchSpace.minScaleOut"));
        this.maxScaleOut = Integer.parseInt(props.getProperty("searchSpace.maxScaleOut"));
        this.minTaskSlots = Integer.parseInt(props.getProperty("searchSpace.minTaskSlots"));
        this.maxTaskSlots = Integer.parseInt(props.getProperty("searchSpace.maxTaskSlots"));
        this.minChkInt = Integer.parseInt(props.getProperty("searchSpace.minChkInt"));
        this.maxChkInt = Integer.parseInt(props.getProperty("searchSpace.maxChkInt"));
        this.minConCpu = Float.parseFloat(props.getProperty("searchSpace.minConCpu"));
        this.maxConCpu = Float.parseFloat(props.getProperty("searchSpace.maxConCpu"));
        this.minConMem = Integer.parseInt(props.getProperty("searchSpace.minConMem"));
        this.maxConMem = Integer.parseInt(props.getProperty("searchSpace.maxConMem"));

        // kubernetes properties
        this.k8s = props.getPropertyList("k8s");

        // clients manager properties
        String masterIP = props.getProperty("clients.masterIP");
        List<String> genPorts = Arrays.asList(props.getProperty("clients.genPorts").split(","));
        String promPort = props.getProperty("clients.promPort");
        String analyticsPort = props.getProperty("clients.analyticsPort");
        this.cm = ClientsManager.create(this.expName, masterIP, genPorts, promPort, analyticsPort);

        // create data manager
        this.dm = DataManager.create();

        // create timer manager used in profiling
        this.et = new EventTimer(executor);
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    public int reserveNodePort() {

        for (Map.Entry<Integer, Boolean> entry : this.nodePorts.entrySet()) {

            if (entry.getValue()) {

                LOG.info("Reserving NodePort: " + entry.getKey());
                this.nodePorts.put(entry.getKey(), false);
                return entry.getKey();
            }
        }
        throw new IllegalStateException("All NopePorts are in use");
    }

    public void releaseNodePort(int nodePort) {

        LOG.info("Releasing NodePort: " + nodePort);
        this.nodePorts.put(nodePort, true);
    }

    @Override
    public void close() throws Exception {

        this.executor.shutdownNow();
    }
}
