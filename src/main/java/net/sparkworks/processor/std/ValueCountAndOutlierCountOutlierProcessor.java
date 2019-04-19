package net.sparkworks.processor.std;

import net.sparkworks.functions.OutliersOnValuesDetectAggregateFunction;
import net.sparkworks.functions.OutliersOnOutliersDetectAggregateFunction;
import net.sparkworks.functions.OutliersDetectProcessWindowFunction;
import net.sparkworks.functions.OutliersDetectionDataAscendingTimestampExtractor;
import net.sparkworks.functions.STDOutliersCountApplyWindowFunction;
import net.sparkworks.functions.STDValuesCountApplyWindowFunction;
import net.sparkworks.model.FlaggedOutliersResult;
import net.sparkworks.model.OutliersResult;
import net.sparkworks.out.RMQOut;
import net.sparkworks.serialization.OutliersOnOutliersSerializationSchema;
import net.sparkworks.serialization.OutliersOnValuesSerializationSchema;
import net.sparkworks.util.Config;
import net.sparkworks.util.RBQueue;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.concurrent.TimeUnit;

public class ValueCountAndOutlierCountOutlierProcessor {

    public static void main(String[] args) throws Exception {

        Config cfg = new Config();

        // The StreamExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Setup the connection settings to the RabbitMQ broker
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(cfg.getBrokerHost())
                .setPort(cfg.getBrokerPort())
                .setUserName(cfg.getBrokerUsername())
                .setPassword(cfg.getBrokerPassword())
                .setVirtualHost(cfg.getBrokerVHost())
                .build();

        final DataStream<String> rawStream = env
                .addSource(new RBQueue<String>(
                        connectionConfig,                  // config for the RabbitMQ connection
                        cfg.getOutliersInputQueue5Min(),   // name of the RabbitMQ queue to consume
                        true,               // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))         // deserialization schema to turn messages into Java objects
                .setParallelism(1);                        // deserialization schema to turn messages into Java objects

/*
        // Read from .csv
        final String filename;
        Integer parallelism = null;
        try {
            // access the arguments of the command line tool
            final ParameterTool params = ParameterTool.fromArgs(args);
            if (!params.has("filename")) {
                filename = "/tmp/sensordata2.csv";
                System.err.println("No filename specified. Please run 'WindowProcessor " +
                        "--filename <filename>, where filename is the name of the dataset in CSV format");
            } else {
                filename = params.get("filename");
            }

            if (params.has("parallelism")) {
                parallelism = params.getInt("parallelism");
            }

        } catch (Exception ex) {
            System.err.println("No filename specified. Please run 'WindowProcessor " +
                    "--filename <filename>, where filename is the name of the dataset in CSV format");
            return;
        }

        if (Objects.nonNull(parallelism)) {
            env.setParallelism(parallelism);
        }

        // Deserialization schema to turn messages into SensorData objects
        final DataStream<String> rawStream = env.readTextFile(filename);
*/

        // Turn String into OutliersResult urn | timestamp | valuesCount | outliersCount
        DataStream<OutliersResult> countersResultDataStream = rawStream
                .map((MapFunction<String, OutliersResult>) OutliersResult::fromString)
                .assignTimestampsAndWatermarks(new OutliersDetectionDataAscendingTimestampExtractor());

        // KeyBy URN
        // 60' window
        // Detect the outliers on the valuesCount and flag them
        // Turn the OutliersResult into FlaggedOutliersResult urn | timestamp | valuesCount | outliersCount | isOutlier
        final DataStream<FlaggedOutliersResult> flaggedCountersResultDataStream = countersResultDataStream
                .keyBy((KeySelector<OutliersResult, String>) OutliersResult::getUrn)
                .window(TumblingEventTimeWindows.of(Time.minutes(cfg.getOutliersOutliersInterval())))
                .apply(new STDValuesCountApplyWindowFunction());

        // KeyBy URN
        // 60' window
        // Create the OutliersResult urn | timestamp | valuesCount | outliersCount
        final DataStream<OutliersResult> countersResultDataStream1 = flaggedCountersResultDataStream
                .keyBy((KeySelector<FlaggedOutliersResult, String>) FlaggedOutliersResult::getUrn)
                .window(TumblingEventTimeWindows.of(Time.minutes(cfg.getOutliersOutliersInterval())))
                .aggregate(new OutliersOnValuesDetectAggregateFunction(), new OutliersDetectProcessWindowFunction());

/*
        // Write the results in the csv
        DataStream<Tuple4<String, Long, Long, Long>> tuple4DataStream =
                countersResultDataStream1.map(new OutlierTupleDataMapFunction());
        tuple4DataStream.writeAsCsv("/tmp/sensordata3.csv",
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);
*/

        // Output the results
        if (cfg.doOutput()) {
            countersResultDataStream1.addSink(new RMQOut<OutliersResult>(connectionConfig, cfg.getAnalyticsOutputExchange(),
                    Config.OUT_ROUTING_KEY_VALUES_60_MIN, new OutliersOnValuesSerializationSchema()));
        }

        // Print the OutliersResult
        countersResultDataStream1.print();

        // KeyBy URN
        // 60' window
        // Detect the outliers on the outliersCount and flag them
        // Turn the OutliersResult into FlaggedOutliersResult urn | timestamp | valuesCount | outliersCount | isOutlier
        final DataStream<FlaggedOutliersResult> flaggedCountersResultDataStream1 = countersResultDataStream
                .keyBy((KeySelector<OutliersResult, String>) OutliersResult::getUrn)
                .window(TumblingEventTimeWindows.of(Time.minutes(cfg.getOutliersOutliersInterval())))
                .apply(new STDOutliersCountApplyWindowFunction());

        // KeyBy URN
        // 60' window
        // Create the OutliersResult urn | timestamp | valuesCount | outliersCount
        final DataStream<OutliersResult> countersResultDataStream2 = flaggedCountersResultDataStream1
                .keyBy((KeySelector<FlaggedOutliersResult, String>) FlaggedOutliersResult::getUrn)
                .window(TumblingEventTimeWindows.of(Time.minutes(cfg.getOutliersOutliersInterval())))
                .aggregate(new OutliersOnOutliersDetectAggregateFunction(), new OutliersDetectProcessWindowFunction());

/*
        // Write the results in the csv
        DataStream<Tuple4<String, Long, Long, Long>> tuple4DataStream2 =
                countersResultDataStream2.map(new OutlierTupleDataMapFunction());
        tuple4DataStream2.writeAsCsv("/tmp/sensordata4.csv",
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);
*/

        // Output the results
        if (cfg.doOutput()) {
            countersResultDataStream2.addSink(new RMQOut<OutliersResult>(connectionConfig, cfg.getAnalyticsOutputExchange(),
                    Config.OUT_ROUTING_KEY_OUTLIERS_60_MIN, new OutliersOnOutliersSerializationSchema()));
        }

        // Print the OutliersResult
        countersResultDataStream2.print();
        final JobExecutionResult jobExecutionResult = env.execute("SparkWorks Window Processor");

        System.out.println(String.format("SparkWorks Window Processor Job took: %d ms with parallelism: %d",
                jobExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS), env.getParallelism()));
    }
}
