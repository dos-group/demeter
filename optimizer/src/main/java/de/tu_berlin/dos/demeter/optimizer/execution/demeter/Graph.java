package de.tu_berlin.dos.demeter.optimizer.execution.demeter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import de.tu_berlin.dos.demeter.optimizer.execution.FlinkJob;
import de.tu_berlin.dos.demeter.optimizer.execution.FlinkJob.Config;
import de.tu_berlin.dos.demeter.optimizer.managers.DataManager;
import de.tu_berlin.dos.demeter.optimizer.modeling.AnomalyDetector;
import de.tu_berlin.dos.demeter.optimizer.structures.SequenceFSM;
import de.tu_berlin.dos.demeter.optimizer.structures.TimeSeries;
import de.tu_berlin.dos.demeter.optimizer.utils.EventTimer;
import org.apache.commons.lang.RandomStringUtils;
import org.nd4j.linalg.primitives.AtomicDouble;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public enum Graph implements SequenceFSM<Context, Graph> {

    START {

        public Graph runStage(Context ctx) throws Exception {
            ctx.cm.initializeNamespace();
            return STOP;
        }
    },
    INITIALIZE {

        public Graph runStage(Context ctx) throws Exception {

            // initialize namespace and deploy helm charts and yaml files from configW
            ctx.cm.initializeNamespace();
            if (ctx.k8s.find("HELM") != null) ctx.k8s.find("HELM").forEach(ctx.cm::deployHelm);
            ctx.et.start(300);

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
            ctx.et.start(40);
            ctx.job.setJobId(ctx.cm.getJobId(ctx.job.jobName));
            long currTs = ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId());
            ctx.job.addTs(currTs);
            LOG.info("Started target job: " + ctx.job + " at TS: " + currTs);

            return PROFILE;
        }
    },
    PROFILE {

        public Graph runStage(Context ctx) throws Exception {

            AtomicBoolean mustWait = new AtomicBoolean(true);
            AtomicLong nowTs = new AtomicLong(ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId()));
            AtomicLong lastTs = new AtomicLong(nowTs.get());
            ctx.dm.initProfiles(ctx.expId, ctx.genType, ctx.cleanDb);
            ctx.executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    EventTimer et = new EventTimer(ctx.executor);
                    List<FlinkJob> profiles = new ArrayList<>();
                    try {
                        // if it is the first loop, wait 10 minutes for workload metrics, then begin profiling loop
                        if (mustWait.get()) {
                            LOG.info("Waiting 10 minutes before Profiling begins");
                            et.start(600);
                            mustWait.set(false);
                        }
                        // Gathering metrics from Target Job and update database
                        nowTs.set(ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId()));
                        //long lastTs = ctx.job.getLastTs();
                        LOG.info("LATEST TIMESTAMP " + nowTs);
                        TimeSeries tarWld = ctx.cm.getWorkload(ctx.executor, lastTs.get(), nowTs.get());
                        //LOG.info("Target Job workload: " + tarWld);
                        TimeSeries tarThr = ctx.cm.getThr(ctx.job.jobName, nowTs.get() - ctx.avgWindow, nowTs.get());
                        LOG.info("Profile Job throughput: " + tarThr.avg());
                        TimeSeries tarLat = ctx.cm.getLat(ctx.job.jobName, nowTs.get() - ctx.avgWindow, nowTs.get());
                        LOG.info("Profile Job latency: " + tarLat.avg());

                        // creating target profile for analytics
                        DataManager.Profile targetProfile = new DataManager.Profile(
                            ctx.expId, ctx.genType, ctx.job.jobName, ctx.job.getConfig().scaleOut, ctx.job.getConfig().chkInt,
                            ctx.job.getConfig().cpuVal, ctx.job.getConfig().memVal, ctx.job.getConfig().taskSlots,
                            tarThr.avg(), tarLat.avg(), -1, /*ctx.job.getLastTs()*/lastTs.get(), nowTs.get());

                        // adding target profile to the database
                        ctx.dm.addProfile(targetProfile);

                        JsonObject profileResponse = ctx.cm.evaluate(
                            ctx.genType, ctx.dm.getProfiles(ctx.expId, ctx.genType), ctx.cpuConst, ctx.memConst, tarWld,
                            ctx.minScaleOut, ctx.maxScaleOut, ctx.minChkInt, ctx.maxChkInt, ctx.minConCpu, ctx.maxConCpu, ctx.minConMem, ctx.maxConMem,
                            ctx.minTaskSlots, ctx.maxTaskSlots, ctx.defaultConfigs, targetProfile, ctx.recTimeConst, false);

                        // test the status of the response and based on result, perform certain actions
                        String profileStatus = profileResponse.get("status").getAsString();
                        if ("ERROR".equalsIgnoreCase(profileStatus)) {
                            throw new RuntimeException("Problem with Analytics server");
                        }
                        // determine if profiling runs should be run and execute them
                        else if (profileResponse.get("profiling_configs").getAsJsonArray().size() > 0) {
                            JsonArray configs = profileResponse.get("profiling_configs").getAsJsonArray();
                            LOG.info("Executing profiling runs");
                            for (int i = 0; i < configs.size(); i++) {
                                // flink properties
                                String jobName = "profile" + (i + 1);
                                String proProdTopic = ctx.tarProdTopic + RandomStringUtils.random(10, true, true);
                                FlinkJob profile = new FlinkJob(
                                    ctx.reserveNodePort(), ctx.entryClass, jobName,
                                    ctx.brokerList, ctx.tarConsTopic, proProdTopic,
                                    ctx.extraArgs, ctx.chkDir, ctx.saveDir, ctx.timeout);
                                JsonObject e = configs.get(i).getAsJsonObject().get("configs").getAsJsonObject();
                                Config config = new Config(
                                    e.get("scaleout").getAsInt(),
                                    e.get("task_slots").getAsInt(),
                                    e.get("container_cpu").getAsFloat(),
                                    e.get("container_memory").getAsInt(),
                                    e.get("checkpoint_interval").getAsInt());
                                profile.setConfig(config);
                                profiles.add(profile);
                                ctx.cm.createJob(profile);
                            }
                            // wait 120 seconds for all jobs to come online
                            et.start(120);
                            // retrieve job IDs
                            for (FlinkJob profile : profiles) {
                                profile.setJobId(ctx.cm.getJobId(profile.jobName));
                                profile.addTs(ctx.cm.getLatestTs(profile.jobName, profile.getJobId()));
                                LOG.info("Started profiling job: " + profile);
                            }
                            // wait i minutes for normalized metrics
                            ctx.et.start(60);
                            // gather metrics
                            AtomicInteger counter = new AtomicInteger();
                            CountDownLatch latch = new CountDownLatch(profiles.size());
                            for (FlinkJob profile : profiles) {
                                // create new thread to measure recovery times for each profiling job
                                ctx.executor.submit(() -> {
                                    try {
                                        // added to prevent log pollution with repeated failures
                                        new CountDownLatch(1).await(1000, TimeUnit.MILLISECONDS);
                                        // fetch metrics for profiling job
                                        long stopTs = ctx.cm.getLatestTs(profile.jobName, profile.getJobId());
                                        long startTs = stopTs - ctx.avgWindow;
                                        TimeSeries proThr = ctx.cm.getThr(profile.jobName, startTs, stopTs);
                                        LOG.info(profile.jobName + " throughput: " + proThr.avg());
                                        TimeSeries proLat = ctx.cm.getLat(profile.jobName, stopTs - ctx.avgWindow, stopTs);
                                        LOG.info(profile.jobName + " latency: " + proLat.avg());
                                        TimeSeries proLag = ctx.cm.getLag(profile.jobName, stopTs - ctx.avgWindow, stopTs);
                                        LOG.info(profile.jobName + " consumer lag: " + proLag.avg());
                                        // write metrics to database
                                        DataManager.Profile jobProfile = new DataManager.Profile(
                                            ctx.expId, ctx.genType, profile.jobName, profile.getConfig().scaleOut,
                                            profile.getConfig().chkInt, profile.getConfig().cpuVal, profile.getConfig().memVal,
                                            profile.getConfig().taskSlots, proThr.avg(), proLat.avg(), -1.0, startTs, stopTs);
                                        ctx.dm.addProfile(jobProfile);
                                        profile.addTs(stopTs);
                                        // wait until close to the end of checkpoint interval and then inject failure
                                        while (true) {
                                            // get current timestamp and when latest checkpoint was made
                                            long currTs = ctx.cm.getLatestTs(profile.jobName, profile.getJobId());
                                            long lastChkTs = ctx.cm.getLastCptTs(profile.jobName, profile.getJobId());
                                            LOG.info(profile.jobName + ", currTs: " + currTs + ", lastChkTs: " + lastChkTs);
                                            // find time to point which is 5 before end of current checkpoint
                                            int target = (int) ((profile.getConfig().chkInt / 1000) - (currTs - lastChkTs) - 5);
                                            LOG.info("target: " + target + ", last chkpoint: " + (currTs - lastChkTs));
                                            // check if enough time is available in current checkpoint, otherwise wait till next
                                            if (target > 0) {
                                                // wait until 3 seconds before next checkpoint is scheduled to start
                                                et.start(target);
                                                // Record timestamp and inject failure into taskmanager
                                                LOG.info("Injecting delay into profiling job: " + profile.jobName);
                                                profile.addTs(ctx.cm.getLatestTs(profile.jobName, profile.getJobId()));
                                                ctx.cm.injectDelay(Map.of("app", profile.jobName, "component", "taskmanager"), 30);
                                                break;
                                            }
                                            new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
                                        }
                                        // wait until failure is injected after timeout in seconds and then get timestamp
                                        et.start(ctx.timeout / 1000);
                                        long detectTs = ctx.cm.getLatestTs(profile.jobName, profile.getJobId());
                                        // train anomaly detector with throughput and consumer lag
                                        AnomalyDetector detector = new AnomalyDetector();
                                        detector.train(List.of(proThr, proLag), ctx.avgWindow);
                                        // create event timer with duration of max double the rec time constraint minus timeout
                                        int duration = (ctx.recTimeConst * 2) - (ctx.timeout / 1000);
                                        // register listener which runs after 20 seconds and every 10 seconds to measure the recovery time
                                        //String id = "";
                                        AtomicDouble recTime = new AtomicDouble(-1.0);
                                        et.register(t -> 30 <= t && t % 10 == 0, (listener) -> {
                                            // retrieve metrics for profiling run
                                            long currTs = ctx.cm.getLatestTs(profile.jobName, profile.getJobId());
                                            TimeSeries measureThr = ctx.cm.getThr(profile.jobName, detectTs, currTs);
                                            //LOG.info(profile.jobName + " thr: " + measureThr.avg());
                                            TimeSeries measureLag = ctx.cm.getLag(profile.jobName, detectTs, currTs);
                                            //LOG.info(profile.jobName + " consumer lag: " + measureLag.avg());
                                            //TimeSeries testLat = ctx.cm.getLat(profile.jobName, detectTs, currTs);
                                            //LOG.info("lat: " + testLat.avg());
                                            recTime.set(detector.measure(Arrays.asList(measureThr, measureLag), currTs - detectTs));
                                            //LOG.info("recTime: " + recTime.get());
                                            if (recTime.get() != -1.0) listener.remove();
                                            else {
                                                //LOG.info("Not yet recovered for profile: " + profile.jobName);
                                            }
                                        });
                                        // start the timer to run as long as there are listeners until duration timeout
                                        et.start(duration, true);
                                        // determine if rec time was found or not and write to database
                                        LOG.info("Measuring rec time finished for " + profile.jobName + " with rec time " + recTime.get());
                                        if (recTime.get() == -1.0) recTime.set(duration);
                                        ctx.dm.updateRecTime(ctx.expId, ctx.genType, profile.jobName, ((double) ctx.timeout / 1000) + recTime.get(), stopTs);
                                    }
                                    catch (Exception e) {
                                        LOG.info("Error: " + e);
                                    }
                                    finally {
                                        LOG.info(counter.incrementAndGet() + "/" + profiles.size() + " completed");
                                        latch.countDown();
                                    }
                                });
                            }
                            latch.await();
                            LOG.info("latch counted down, profiling run completed");
                        }
                    }
                    catch(Exception e) {
                        LOG.info("Error: " + e);
                    }
                    finally {
                        for (FlinkJob profile : profiles) {
                            ctx.cm.deleteJob(profile);
                            ctx.releaseNodePort(profile.port);
                            LOG.info("Stopped profiling job: " + profile);
                        }
                        // clear listeners from timer and clear profiles
                        et.clearListeners();
                        lastTs.set(nowTs.get());
                        profiles.clear();
                    }
                }
            });
            return OPTIMIZE;
        }
    },
    OPTIMIZE {

        public Graph runStage(Context ctx) throws Exception {
            // initialize database and execute optimization step in separate thread
            ctx.dm.initPredictions(ctx.expId, ctx.genType,true);
            ctx.executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    EventTimer et = new EventTimer(ctx.executor);
                    //List<FlinkJob> profiles = new ArrayList<>();
                    try {
                        // added to prevent log pollution with repeated failures
                        new CountDownLatch(1).await(1000, TimeUnit.MILLISECONDS);
                        // ensure job uptime and time since last update is over 10 minutes
                        while (true) {
                            et.start(60);
                            long now = ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId());
                            long lastChange = now - ctx.job.getLastTs();
                            LOG.info(String.format("Wait till evaluation interval (%d/%d) has expired", lastChange, ctx.evalInt));
                            if (ctx.evalInt <= lastChange) break;
                        }
                        // Gathering metrics from Target Job and update database
                        long tarNow = ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId());
                        long lastTs = ctx.job.getLastTs();
                        LOG.info("LATEST TIMESTAMP " + tarNow);
                        TimeSeries tarWld = ctx.cm.getWorkload(ctx.executor, lastTs, tarNow);
                        //LOG.info("Target Job workload: " + tarWld);
                        TimeSeries tarThr = ctx.cm.getThr(ctx.job.jobName, /*ctx.job.getLastTs()*/ tarNow - ctx.avgWindow, tarNow);
                        LOG.info("Target Job throughput: " + tarThr.avg());
                        TimeSeries tarLat = ctx.cm.getLat(ctx.job.jobName, /*ctx.job.getLastTs()*/ tarNow - ctx.avgWindow, tarNow);
                        LOG.info("Target Job latency: " + tarLat.avg());

                        // creating target profile for analytics
                        DataManager.Profile targetProfile = new DataManager.Profile(
                            ctx.expId, ctx.genType, ctx.job.jobName, ctx.job.getConfig().scaleOut,
                            ctx.job.getConfig().chkInt, ctx.job.getConfig().cpuVal, ctx.job.getConfig().memVal,
                            ctx.job.getConfig().taskSlots, tarThr.avg(), tarLat.avg(), -1, ctx.job.getLastTs(), tarNow);

                        // Create Json Object for evaluation to analytics
                        JsonObject rescaleResponse = ctx.cm.evaluate(
                            ctx.genType, ctx.dm.getProfiles(ctx.expId, ctx.genType), ctx.cpuConst, ctx.memConst, tarWld,
                            ctx.minScaleOut, ctx.maxScaleOut, ctx.minChkInt, ctx.maxChkInt, ctx.minConCpu, ctx.maxConCpu, ctx.minConMem, ctx.maxConMem,
                            ctx.minTaskSlots, ctx.maxTaskSlots, ctx.defaultConfigs, targetProfile, ctx.recTimeConst, true);
                        //LOG.info(rescaleResponse.toString());

                        // test the status of the response and based on result, perform certain actions
                        String rescaleStatus = rescaleResponse.get("status").getAsString();
                        if ("ERROR".equalsIgnoreCase(rescaleStatus)) {

                            throw new RuntimeException("Problem with Analytics server");
                        }
                        else if ("RESCALE".equalsIgnoreCase(rescaleStatus)) {

                            LOG.info("Rescale has been triggered: " + rescaleResponse);
                            JsonObject e = rescaleResponse.get("production_config").getAsJsonObject().get("configs").getAsJsonObject();
                            Config config = new Config(
                                e.get("scaleout").getAsInt(),
                                e.get("task_slots").getAsInt(),
                                e.get("container_cpu").getAsFloat(),
                                e.get("container_memory").getAsInt(),
                                e.get("checkpoint_interval").getAsInt());
                            ctx.job.setConfig(config);
                            ctx.cm.updateJob(ctx.job);
                            ctx.et.start(60);
                            ctx.job.setJobId(ctx.cm.getJobId(ctx.job.jobName));
                            ctx.job.addTs(ctx.cm.getLatestTs(ctx.job.jobName, ctx.job.getJobId()));
                            LOG.info("Upgrade target job: " + ctx.job);
                        }
                    }
                    catch(Exception e) {

                        LOG.info("Error: " + e);
                    }
                }
            });
            // call next state
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
                        LOG.info("Injecting delay into target job: " + ctx.job.jobName + " at TS: " + currTs);
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
            CountDownLatch latch = new CountDownLatch(7);
            metrics.forEach((name, timeSeries) -> {

                ctx.executor.submit(() -> {

                    try { TimeSeries.toCSV(name, timeSeries, "timestamp|value", "|"); }
                    catch (IOException e) { e.printStackTrace(); }
                    finally { latch.countDown(); }
                });
            });
            latch.await();

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
