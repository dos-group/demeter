package de.tu_berlin.dos.arm.streaming_experiment_suite.jobs;

import com.codahale.metrics.SlidingWindowReservoir;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tu_berlin.dos.arm.streaming_experiment_suite.common.events.CarEvent;
import de.tu_berlin.dos.arm.streaming_experiment_suite.common.utils.FileReader;
import de.tu_berlin.dos.arm.streaming_experiment_suite.common.utils.OrderedProperties;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TopSpeedWindowing {

    /***************************************************************************************************************
     * PROGRAMME
     **************************************************************************************************************/

    public static final Logger LOG = LogManager.getLogger(TopSpeedWindowing.class);

    public static void main(String[] args) throws Exception {

        LOG.info(args);
        //--jobName test --brokerList 130.149.248.64:9092 --consumerTopic input --producerTopic output
        // get command line arguments
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String jobName = parameter.get("jobName");
        String brokerList = parameter.get("brokerList");;
        String consumerTopic = parameter.get("consumerTopic");
        String producerTopic = parameter.get("producerTopic");
        String offset = parameter.get("offset");
        int chkInterval = Integer.parseInt(parameter.get("chkInterval"));

        // retrieve properties from file
        //OrderedProperties props = FileReader.GET.read("jobs.properties", OrderedProperties.class);
        //String backupFolder = props.getProperty("job.backupFolder");
        //long checkpointInterval = Long.parseLong(props.getProperty("job.checkpointInterval"));

        final ParameterTool params = ParameterTool.fromArgs(args);

        /***************************************************************************************************************
         * CONFIGURATION
         **************************************************************************************************************/

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.disableOperatorChaining();

        // configuring RocksDB state backend to use HDFS
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // start a checkpoint based on supplied interval
        env.enableCheckpointing(chkInterval);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // checkpoints have to complete within 2 minutes, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);

        // enable externalized checkpoints which are deleted after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // no external services which could take some time to respond, therefore 1
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
        //env.getCheckpointConfig().enableUnalignedCheckpoints();

        /***************************************************************************************************************
         * CREATE SOURCE(S) & SINK(S)
         **************************************************************************************************************/

        OffsetsInitializer initializer;
        if ("earliest".equalsIgnoreCase(offset)) initializer = OffsetsInitializer.earliest();
        else initializer = OffsetsInitializer.latest();

        // setup Kafka consumer
        KafkaSource<Tuple5<String, Integer, Double, Long, List<Long>>> myConsumer =
            KafkaSource.<Tuple5<String, Integer, Double, Long, List<Long>>>builder()
                .setBootstrapServers(brokerList)
                .setTopics(consumerTopic)
                .setGroupId(UUID.randomUUID().toString())
                .setStartingOffsets(initializer)
                .setValueOnlyDeserializer(new DeserializationSchema<>() {

                    final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public Tuple5<String, Integer, Double, Long, List<Long>> deserialize(byte[] json) throws IOException {

                        CarEvent carEvent = objectMapper.readValue(json, CarEvent.class);
                        List<Long> timestamps = new ArrayList<>();
                        timestamps.add(carEvent.ts);
                        return new Tuple5<>(carEvent.id, carEvent.sd, carEvent.dt, carEvent.ts, timestamps);
                    }

                    @Override
                    public boolean isEndOfStream(Tuple5<String, Integer, Double, Long, List<Long>> nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<Tuple5<String, Integer, Double, Long, List<Long>>> getProducedType() {

                        return TypeInformation.of(new TypeHint<>() { });
                    }
                })
                .build();

        // setup kafka producer
        KafkaSink<Tuple4<String, Integer, Double, Long>> myProducer =
            KafkaSink.<Tuple4<String, Integer, Double, Long>>builder()
                .setBootstrapServers(brokerList)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic(producerTopic)
                    .setValueSerializationSchema((SerializationSchema<Tuple4<String, Integer, Double, Long>>) e -> {

                        return String.format(
                                "{\"id\":%s,\"sd\":%d,\"dt\"%f,\"ts\":%d}",
                                e.f0, e.f1, e.f2, e.f3)
                            .getBytes(StandardCharsets.UTF_8);
                    })
                    .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(UUID.randomUUID().toString())
                //.setKafkaProducerConfig(kafkaProducerProps)
                .build();

        /***************************************************************************************************************
         * JOB CODE
         **************************************************************************************************************/

        // for flink 1.14
        WatermarkStrategy<Tuple5<String, Integer, Double, Long, List<Long>>> watermark =
            WatermarkStrategy
                .<Tuple5<String, Integer, Double, Long, List<Long>>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.f3);

        DataStream<Tuple5<String, Integer, Double, Long, List<Long>>> carData = env
            .fromSource(myConsumer, watermark, "KafkaSource")
            .uid("kafkaSource");

        int eviction = 10000;
        double triggerMeters = 50;
        String random = RandomStringUtils.random(10, true, true);
        carData
            .map(new RichMapFunction<Tuple5<String, Integer, Double, Long, List<Long>>, Tuple5<String, Integer, Double, Long, List<Long>>>() {

                @Override
                public Tuple5<String, Integer, Double, Long, List<Long>> map(Tuple5<String, Integer, Double, Long, List<Long>> value) throws Exception {
                    value.f4.add(System.currentTimeMillis());
                    return value;
                }
            })
            .name("latencyBefore")
            .uid("latencyAfter")
            .keyBy(carEvent -> carEvent.f0)
            .window(GlobalWindows.create())
            .evictor(TimeEvictor.of(Time.of(eviction, TimeUnit.MILLISECONDS)))
            .trigger(DeltaTrigger.of(triggerMeters, new DeltaFunction<Tuple5<String, Integer, Double, Long, List<Long>>>() {

                private static final long serialVersionUID = 1L;

                @Override
                public double getDelta(
                        Tuple5<String, Integer, Double, Long, List<Long>> oldDataPoint,
                        Tuple5<String, Integer, Double, Long, List<Long>> newDataPoint) {

                    return newDataPoint.f2 - oldDataPoint.f2;
                }
            }, carData.getType().createSerializer(env.getConfig())))
            .maxBy(1)
            .name("speedFilter")
            .uid("speedFilter")
            .map(new RichMapFunction<
                    Tuple5<String, Integer, Double, Long, List<Long>>,
                    Tuple4<String, Integer, Double, Long>>() {

                private static final long serialVersionUID = 1L;
                private transient Histogram histogram;

                @Override
                public void open(Configuration parameters) throws Exception {

                    super.open(parameters);
                    com.codahale.metrics.Histogram dropwizardHistogram =
                            new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
                    this.histogram =
                        getRuntimeContext()
                            .getMetricGroup()
                            .histogram("myLatencyHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));
                }

                @Override
                public Tuple4<String, Integer, Double, Long> map(Tuple5<String, Integer, Double, Long, List<Long>> v) throws Exception {

                    long totDur = System.currentTimeMillis() - v.f4.get(0);
                    long winDur = System.currentTimeMillis() - v.f4.get(1);
                    this.histogram.update(totDur - winDur);
                    return Tuple4.of(v.f0, v.f1, v.f2, v.f3);
                }
            })
            .name("measureLatency")
            .uid("measureLatency")
            .sinkTo(myProducer)
            .name("KafkaSink")
            .uid("KafkaSink");

        env.execute(jobName);
    }
}

