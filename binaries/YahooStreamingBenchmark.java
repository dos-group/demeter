package de.tu_berlin.dos.arm.streaming_experiment_suite.jobs;

import com.codahale.metrics.SlidingWindowReservoir;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tu_berlin.dos.arm.streaming_experiment_suite.common.events.AdEvent;
import de.tu_berlin.dos.arm.streaming_experiment_suite.common.utils.FileReader;
import de.tu_berlin.dos.arm.streaming_experiment_suite.common.utils.OrderedProperties;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class YahooStreamingBenchmark {

    /*******************************************************************************************************************
     * INNER CLASSES
     ******************************************************************************************************************/

    public static class Redis {

        public static class LRUHashMap<K, V> extends LinkedHashMap<K, V> {

            private final int cacheSize;

            public LRUHashMap(int cacheSize) {

                super(16, 0.75f, true);
                this.cacheSize = cacheSize;
            }

            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {

                return size() >= cacheSize;
            }
        }

        public static class Connector {

            private static Connector self = null;
            private final JedisCluster cluster;
            //private final RedisClient redisClient;
            //private final StatefulRedisConnection<String, String> conn;
            //private final RedisCommands<String, String> syncCommands;

            private HashMap<String, String> ad_to_campaign;

            private final LRUHashMap<Long, HashMap<String, Window>> campaign_windows;
            private final ExecutorService executor = Executors.newFixedThreadPool(5);

            private Connector(String redisHost, int redisPort) {
            //private Connector(String redis) {

                this.cluster = new JedisCluster(new HostAndPort(redisHost, redisPort));
                //String random = RandomStringUtils.random(10, true, true);
                //this.redisClient = RedisClient.create(redis);
                //this.conn = redisClient.connect();
                //this.syncCommands = conn.sync();
                this.campaign_windows = new LRUHashMap<>(10000);
            }

            public void prepare() {

                ad_to_campaign = new HashMap<>();
            }

            public String execute(String ad_id) {

                String campaign_id = ad_to_campaign.get(ad_id);
                if (campaign_id == null) {

                    campaign_id = cluster.get(ad_id);
                    //campaign_id = syncCommands.get(ad_id);
                    if (campaign_id == null) return "UNKNOWN";
                    else ad_to_campaign.put(ad_id, campaign_id);
                }
                return campaign_id;
            }

            private void writeWindow(String campaign, long count, long timestamp) {

                executor.submit(() -> {

                    String windowUUID = cluster.hmget(campaign, "" + timestamp).get(0);
                    //String windowUUID = syncCommands.hmget(campaign, "" + timestamp).get(0).getValue();
                    LOG.info(windowUUID);
                    if (windowUUID == null) {

                        windowUUID = UUID.randomUUID().toString();
                        cluster.hset(campaign, Long.toString(timestamp), windowUUID);
                        //syncCommands.hset(campaign, Long.toString(timestamp), windowUUID);

                        String windowListUUID = cluster.hmget(campaign, "windows").get(0);
                        //String windowListUUID = syncCommands.hmget(campaign, "windows").get(0).getValue();
                        if (windowListUUID == null) {

                            windowListUUID = UUID.randomUUID().toString();
                            cluster.hset(campaign, "windows", windowListUUID);
                            //syncCommands.hset(campaign, "windows", windowListUUID);
                        }
                        cluster.lpush(windowListUUID, Long.toString(timestamp));
                        //syncCommands.lpush(windowListUUID, Long.toString(timestamp));
                    }
                    synchronized (campaign_windows) {

                        cluster.hset(windowUUID, "seen_count", "" + count);
                        //syncCommands.hset(windowUUID, "seen_count", "" + count);
                    }
                    cluster.hset(windowUUID, "time_updated", Long.toString(System.currentTimeMillis()));
                    //syncCommands.hset(windowUUID, "time_updated", Long.toString(System.currentTimeMillis()));
                    cluster.lpush("time_updated", Long.toString(System.currentTimeMillis()));
                    //syncCommands.lpush("time_updated", Long.toString(System.currentTimeMillis()));
                });
            }

            public static synchronized Connector getInstance(String redisHost, int redisPort) {
            //public static synchronized Connector getInstance(String redis) {

                if (self == null) self = new Connector(redisHost, redisPort);
                //if (self == null) self = new Connector(redis);
                return self;
            }
        }

        public static void prepare(String redisHost, int redisPort) {

            Connector connector = Connector.getInstance(redisHost, redisPort);
            //Connector connector = Connector.getInstance(redis);
            connector.prepare();
        }

        public static String execute(String redisHost, int redisPort, String ad_id) {

            Connector connector = Connector.getInstance(redisHost, redisPort);
            //Connector connector = Connector.getInstance(redis);
            return connector.execute(ad_id);
        }

        //public static void execute(String campaign_id, long count, long timestamp, String redisHost, int redisPort) {
        public static void writeWindow(String redisHost, int redisPort, String campaign_id, long count, long timestamp) {

            Connector connector = Connector.getInstance(redisHost, redisPort);
            //Connector connector = Connector.getInstance(redis);
            connector.writeWindow(campaign_id, count, timestamp);
        }
    }

    /*public static class RedisAdCampaignCache implements AutoCloseable {

        //private final JedisCluster cluster;
        private final RedisClient redisClient;
        private final StatefulRedisConnection<String, String> conn;
        private final RedisCommands<String, String> syncCommands;
        private HashMap<String, String> ad_to_campaign;

        //public RedisAdCampaignCache(String redisHost, int redisPort) {
        public RedisAdCampaignCache(String redis) {

            //this.cluster = new JedisCluster(new HostAndPort(redisHost, redisPort));
            String random = RandomStringUtils.random(10, true, true);
            RedisURI redisURI = RedisURI.builder().withHost(redis).withPort(6379).withClientName(random).build();
            this.redisClient = RedisClient.create(redisURI);
            this.conn = redisClient.connect();
            this.syncCommands = conn.sync();
        }

        public void prepare() {

            ad_to_campaign = new HashMap<>();
        }

        public String execute(String ad_id) {

            String campaign_id = ad_to_campaign.get(ad_id);
            if (campaign_id == null) {

                //campaign_id = cluster.get(ad_id);
                campaign_id = syncCommands.get(ad_id);
                if (campaign_id == null) return "UNKNOWN";
                else ad_to_campaign.put(ad_id, campaign_id);
            }
            return campaign_id;
        }

        @Override
        public void close() throws Exception {

            this.conn.close();
            this.redisClient.shutdown();
        }
    }*/

    /*******************************************************************************************************************
     * CLASS STATE
     ******************************************************************************************************************/

    private static final Logger LOG = LogManager.getLogger(YahooStreamingBenchmark.class);

    public static void main(final String[] args) throws Exception {

        LOG.info(args);
        //--jobName test --brokerList 130.149.248.64:9092 --consumerTopic input --producerTopic output --redisHost redis-cluster --redisPort 6379
        // get command line arguments
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String jobName = parameter.get("jobName");
        String brokerList = parameter.get("brokerList");;
        String consumerTopic = parameter.get("consumerTopic");
        String producerTopic = parameter.get("producerTopic");
        String offset = parameter.get("offset");
        int chkInterval = Integer.parseInt(parameter.get("chkInterval"));

        // retrieve properties from file
        //OrderedProperties props = FileReader.GET.read("advertising.properties", OrderedProperties.class);

        // creating map for global properties
        //String backupFolder = props.getProperty("job.backupFolder");
        
        /***************************************************************************************************************
         * CONFIGURATION
         **************************************************************************************************************/

        // set up streaming execution environment
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().setGlobalJobParameters(parameter);

        // configuring RocksDB state backend to use HDFS
        environment.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // start a checkpoint based on supplied interval
        environment.enableCheckpointing(chkInterval);

        // set mode to exactly-once (this is the default)
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // checkpoints have to complete within two minute, or are discarded
        environment.getCheckpointConfig().setCheckpointTimeout(120000);
        environment.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);

        // enable externalized checkpoints which are deleted after job cancellation
        environment.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // no external services which could take some time to respond, therefore 1
        // allow only one checkpoint to be in progress at the same time
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
        //environment.getCheckpointConfig().enableUnalignedCheckpoints();

        /***************************************************************************************************************
         * CREATE SOURCE(S) & SINK(S)
         **************************************************************************************************************/

        OffsetsInitializer initializer;
        if ("earliest".equalsIgnoreCase(offset)) initializer = OffsetsInitializer.earliest();
        else initializer = OffsetsInitializer.latest();

        KafkaSource<Tuple4<Long, String, String, List<Long>>> myConsumer =
            KafkaSource.<Tuple4<Long, String, String, List<Long>>>builder()
                .setBootstrapServers(brokerList)
                .setTopics(consumerTopic)
                .setGroupId(UUID.randomUUID().toString())
                .setStartingOffsets(initializer)
                .setValueOnlyDeserializer(new DeserializationSchema<>() {

                    final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public Tuple4<Long, String, String, List<Long>> deserialize(byte[] json) throws IOException {

                        AdEvent adEvent = objectMapper.readValue(json, AdEvent.class);
                        List<Long> timestamps = new ArrayList<>();
                        //timestamps.add(System.currentTimeMillis());
                        timestamps.add(adEvent.getTs());
                        return new Tuple4<>(adEvent.getTs(), adEvent.getId(), adEvent.getEt(), timestamps);
                    }

                    @Override
                    public boolean isEndOfStream(Tuple4<Long, String, String, List<Long>> nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<Tuple4<Long, String, String, List<Long>>> getProducedType() {

                        return TypeInformation.of(new TypeHint<>() { });
                    }
                })
                .build();

        // setup kafka producer
        KafkaSink<Tuple3<String, Long, Long>> myProducer =
            KafkaSink.<Tuple3<String, Long, Long>>builder()
                .setBootstrapServers(brokerList)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic(producerTopic)
                    .setValueSerializationSchema(new SerializationSchema<Tuple3<String, Long, Long>>() {

                        @Override
                        public void open(InitializationContext context) throws Exception {

                            SerializationSchema.super.open(context);
                        }

                        @Override
                        public byte[] serialize(Tuple3<String, Long, Long> element) {

                            return element.toString().getBytes(StandardCharsets.UTF_8);
                        }
                    })
                    .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(UUID.randomUUID().toString())
                .build();

/***************************************************************************************************************
 * JOB CODE
 **************************************************************************************************************/

    String random = RandomStringUtils.random(10, true, true);
    WatermarkStrategy<Tuple4<Long, String, String, List<Long>>> watermark =
        WatermarkStrategy
            .<Tuple4<Long, String, String, List<Long>>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((event, timestamp) -> event.f0);

    environment
        .fromSource(myConsumer, watermark, "DeserializeBolt")
        .uid("DeserializeBolt")
        //Filter the records if event type is "view"
        .filter((FilterFunction<Tuple4<Long, String, String, List<Long>>>) adEvent -> adEvent.f2.equals("view"))
        .name("EventFilterBolt")
        .uid("EventFilterBolt")
        // project the event
        .map(new MapFunction<Tuple4<Long, String, String, List<Long>>, Tuple3<String, Long, List<Long>>>() {

            @Override
            public Tuple3<String, Long, List<Long>> map(Tuple4<Long, String, String, List<Long>> adEvent) throws Exception {

                return new Tuple3<>(adEvent.f1, adEvent.f0, adEvent.f3);
            }
        })
        .name("project")
        .uid("project")
        // perform join with redis data
        .flatMap(new RichFlatMapFunction<Tuple3<String, Long, List<Long>>, Tuple4<String, String, Long, List<Long>>>() {

            private String redisHost;
            private int redisPort;

            @Override
            public void open(Configuration parameters) {
                //initialize jedis
                ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                this.redisHost = parameterTool.getRequired("redisHost");
                this.redisPort = Integer.parseInt(parameterTool.getRequired("redisPort"));
                LOG.info(String.format("Opening connection with Jedis to %s:%d", redisHost, redisPort));
                Redis.prepare(redisHost, redisPort);
            }

            @Override
            public void flatMap(Tuple3<String, Long, List<Long>> input, Collector<Tuple4<String, String, Long, List<Long>>> out) throws Exception {

                String ad_id = input.getField(0);
                String campaignId = Redis.execute(redisHost, redisPort, ad_id);

                Tuple4<String, String, Long, List<Long>> tuple = new Tuple4<>(campaignId, input.getField(0), input.getField(1), input.getField(2));
                out.collect(tuple);
            }
        })
        .name("RedisJoinBolt")
        .uid("RedisJoinBolt")
        .map(new RichMapFunction<Tuple4<String, String, Long, List<Long>>, Tuple4<String, String, Long, List<Long>>>() {

            @Override
            public Tuple4<String, String, Long, List<Long>> map(Tuple4<String, String, Long, List<Long>> v) throws Exception {

                v.f3.add(System.currentTimeMillis());
                return Tuple4.of(v.f0, v.f1, v.f2, v.f3);
            }
        })
        .name("latencyBefore")
        .uid("latencyBefore")
        // process campaign
        .keyBy(value -> value.f0)
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
        .process(new ProcessWindowFunction<Tuple4<String, String, Long, List<Long>>, Tuple4<String, Long, Long, List<List<Long>>>, String, TimeWindow>() {

            private String redisHost;
            private int redisPort;

            @Override
            public void open(Configuration parameters) throws Exception {

                ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                this.redisHost = parameterTool.getRequired("redisHost");
                this.redisPort = Integer.parseInt(parameterTool.getRequired("redisPort"));
            }

            @Override
            public void process(String campaignId, Context context, Iterable<Tuple4<String, String, Long, List<Long>>> iterable, Collector<Tuple4<String, Long, Long, List<List<Long>>>> collector) throws Exception {

                // count the number of ads for this campaign
                Iterator<Tuple4<String, String, Long, List<Long>>> iterator = iterable.iterator();
                long count = 0;
                while (iterator.hasNext()) {

                    count++;
                    iterator.next();
                }
                // write campaign id, the ad count, the timestamp of the window to redis
                Redis.writeWindow(redisHost, redisPort, campaignId, count, context.window().getEnd());
                // add timestamps for latency calculation
                iterator = iterable.iterator();
                List<List<Long>> timestamps = new ArrayList<>();
                while (iterator.hasNext()) {

                    Tuple4<String, String, Long, List<Long>> curr = iterator.next();
                    curr.f3.add(System.currentTimeMillis());
                    timestamps.add(curr.f3);
                }
                // create output of operator
                collector.collect(new Tuple4<>(campaignId, count, context.window().getEnd(), timestamps));
            }
        })
        .name("CampaignProcessor")
        .uid("CampaignProcessor")
        .map(new RichMapFunction<Tuple4<String, Long, Long, List<List<Long>>>, Tuple3<String, Long, Long>>() {

            private transient Histogram histogram;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                com.codahale.metrics.Histogram dropwizardHistogram =
                        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

                this.histogram = getRuntimeContext()
                        .getMetricGroup()
                        .histogram("myLatencyHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));
            }

            @Override
            public Tuple3<String, Long, Long> map(Tuple4<String, Long, Long, List<List<Long>>> value) throws Exception {

                for (List<Long> curr : value.f3) {

                    long totDur = System.currentTimeMillis() - curr.get(0);
                    long winDur = curr.get(2) - curr.get(1);
                    this.histogram.update(totDur - winDur);
                }
                return Tuple3.of(value.f0, value.f1, value.f2);
            }
        })
        .name("LatencyAfter")
        .uid("LatencyAfter")
        .sinkTo(myProducer)
        .name("KafkaSink") //  + random
        .uid("KafkaSink"); //  + random

        environment.execute(jobName);
    }
}
