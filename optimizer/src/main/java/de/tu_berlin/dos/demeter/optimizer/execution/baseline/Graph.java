package de.tu_berlin.dos.demeter.optimizer.execution.baseline;

import com.google.gson.JsonObject;
import de.tu_berlin.dos.demeter.optimizer.clients.flink.FlinkClient;
import de.tu_berlin.dos.demeter.optimizer.structures.SequenceFSM;
import de.tu_berlin.dos.demeter.optimizer.structures.TimeSeries;
import de.tu_berlin.dos.demeter.optimizer.utils.EventTimer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public enum Graph implements SequenceFSM<Context, Graph> {

    START {

        public Graph runStage(Context ctx) throws Exception {
            //ctx.cm.initializeNamespace();
            return INITIALIZE;
        }
    },
    INITIALIZE {

        public Graph runStage(Context ctx) throws Exception {

            // initialize namespace and deploy helm charts and yaml files from configW
            ctx.cm.initializeNamespace();
            if (ctx.k8s.find("HELM") != null) ctx.k8s.find("HELM").forEach(ctx.cm::deployHelm);
            ctx.et.start(60);

            // setup and start generators
            JsonObject body = new JsonObject();
            body.addProperty("brokerList", ctx.brokerList);
            body.addProperty("topic", ctx.tarConsTopic);
            body.addProperty("generatorType", ctx.genType);
            body.addProperty("limiterType", ctx.limiterType);
            body.addProperty("limiterMaxNoise", ctx.limiterMaxNoise);
            body.addProperty("fileName", ctx.fileName);
            ctx.cm.initGenerators().startGenerators(body);

            // deploy target job via operator
            ctx.cm.createJob(ctx.job);
            LOG.info(ctx.job.deployment());
            ctx.et.start(60);
            ctx.job.setJobId(ctx.cm.getJobId(ctx.job.jobName));
            long currTs = ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId());
            ctx.job.addTs(currTs);
            LOG.info("Started target job: " + ctx.job + " at TS: " + currTs);

            return EXECUTE;
        }
    },
    EXECUTE {

        public Graph runStage(Context ctx) throws Exception {

            EventTimer failureTimer = new EventTimer(ctx.executor);
            EventTimer injectorTimer = new EventTimer(ctx.executor);
            // create event timer and register failures
            failureTimer.register(t -> 0 < t && t % 2700 == 0, (listener) -> {
                //eventTimer.register(new EventTimer.Listener(, () -> {
                while (true) {
                    // get current timestamp and when latest checkpoint was made
                    long currTs = ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId());
                    long lastChkTs = ctx.cm.getLastCptTs(ctx.job.jobName, ctx.job.getJobId());
                    //LOG.info(ctx.job.jobName + ", currTs: " + currTs + ", lastChkTs: " + lastChkTs);
                    // find time to point which is 5 before end of current checkpoint
                    int target = (int) ((ctx.job.getConfig().chkInt / 1000) - (currTs - lastChkTs) - 5);
                    //LOG.info("target: " + target + ", last chkpoint: " + (currTs - lastChkTs));
                    // check if enough time is available in current checkpoint, otherwise wait till next
                    if (target > 0) {
                        // wait until 3 seconds before next checkpoint is scheduled to start
                        injectorTimer.start(target);
                        // Record timestamp and inject failure into taskmanager
                        currTs = ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId());
                        LOG.info("Injecting delay into profiling job: " + ctx.job.jobName + " at TS: " + currTs);
                        ctx.job.addTs(currTs);
                        ctx.cm.injectDelay(Map.of("app", ctx.job.jobName, "component", "taskmanager"), 30);
                        break;
                    }
                    new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
                }
            });
            // execute experiment for user defined length of time
            LOG.info("Starting Experiment: " + ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId()));
            failureTimer.start(ctx.expLen);
            LOG.info("Stopping Experiment: " + ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId()));

            // gather metrics and stop jobs
            long firstTs = ctx.job.getFirstTs();
            long currTs = ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId());
            ctx.job.addTs(currTs);

            LOG.info("Gathering metrics and stopping job with configuration: " + ctx.job);
            Map<String, TimeSeries> metrics = new HashMap<>();
            metrics.put(String.format("%s_%s_workRate.out", ctx.expId, ctx.job.jobName), ctx.cm.getWorkload(ctx.executor, firstTs, currTs));
            metrics.put(String.format("%s_%s_thrRate.out", ctx.expId, ctx.job.jobName), ctx.cm.getThr(ctx.job.jobName, firstTs, currTs));
            metrics.put(String.format("%s_%s_latency.out", ctx.expId, ctx.job.jobName), ctx.cm.getLat(ctx.job.jobName, firstTs, currTs));
            metrics.put(String.format("%s_%s_consLag.out", ctx.expId, ctx.job.jobName), ctx.cm.getLag(ctx.job.jobName, firstTs, currTs));
            metrics.put(String.format("%s_%s_taskSlots.out", ctx.expId, ctx.job.jobName), ctx.cm.getTaskSlots(ctx.job.jobName, firstTs, currTs));
            metrics.put(String.format("%s_%s_scaleOuts.out", ctx.expId, ctx.job.jobName), ctx.cm.getScaleOuts(ctx.job.jobName, firstTs, currTs));
            metrics.put(String.format("%s_%s_cpuLoad.out", ctx.expId, ctx.job.jobName), ctx.cm.getCpuLoad(ctx.job.jobName, firstTs, currTs));

            // write metrics to file
            CountDownLatch latch = new CountDownLatch(8);
            metrics.forEach((name, timeSeries) -> {

                ctx.executor.submit(() -> {

                    try { TimeSeries.toCSV(name, timeSeries, "timestamp|value", "|"); }
                    catch (IOException e) { e.printStackTrace(); }
                    finally { latch.countDown(); }
                });
            });
            latch.await();

            LOG.info("finished!");
            new CountDownLatch(1).await();

            return STOP;
        }
    },
    STOP {

        public Graph runStage(Context ctx) throws Exception {

            ctx.close();
            return this;
        }
    };

    public static void start(String propsFile) throws Exception {

        START.run(Graph.class, Context.create(propsFile));
    }
}
