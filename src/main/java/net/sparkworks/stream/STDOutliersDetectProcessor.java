package net.sparkworks.stream;

import net.sparkworks.SparkConfiguration;
import net.sparkworks.functions.OutliersDetectAggregateFunction;
import net.sparkworks.functions.OutliersDetectProcessWindowFunction;
import net.sparkworks.functions.STDOutliersDetectApplyWindowFunction;
import net.sparkworks.functions.SensorDataAscendingTimestampExtractor;
import net.sparkworks.model.FlaggedSensorData;
import net.sparkworks.model.CountersResult;
import net.sparkworks.model.SensorData;
import net.sparkworks.out.RMQOut;
import net.sparkworks.serialization.CountersResultSerializationSchema;
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

public class STDOutliersDetectProcessor {

    public static void main(String[] args) throws Exception {


        // The StreamExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Setup the connection settings to the RabbitMQ broker
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(SparkConfiguration.brokerHost)
                .setPort(SparkConfiguration.brokerPort)
                .setUserName(SparkConfiguration.username)
                .setPassword(SparkConfiguration.password)
                .setVirtualHost(SparkConfiguration.brokerVHost)
                .build();

        final DataStream<String> rawStream = env
                .addSource(new RBQueue<String>(
                        connectionConfig,           // config for the RabbitMQ connection
                        SparkConfiguration.queue,   // name of the RabbitMQ queue to consume
                        true,        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))  // deserialization schema to turn messages into Java objects
                .setParallelism(1);                 // deserialization schema to turn messages into Java objects

/*
        // Read from .csv
        final String filename;
        Integer parallelism = null;
        try {
            // access the arguments of the command line tool
            final ParameterTool params = ParameterTool.fromArgs(args);
            if (!params.has("filename")) {
                filename = "/tmp/sensordata.csv";
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

        // Turn String into SensorData urn | value | timestamp
        DataStream<SensorData> sensorDataStream = rawStream
                .map((MapFunction<String, SensorData>) SensorData::fromString)
                .assignTimestampsAndWatermarks(new SensorDataAscendingTimestampExtractor());

        // KeyBy URN
        // 5' window
        // Detect the SensorData outliers and flag them
        // Turn SensorData into FlaggedSensorData urn | value | timestamp | isOutlier
        final DataStream<FlaggedSensorData> flaggedSensorDataDataStream = sensorDataStream
                .keyBy((KeySelector<SensorData, String>) SensorData::getUrn)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .apply(new STDOutliersDetectApplyWindowFunction());

        // KeyBy URN
        // 5' window
        // Create the CountersResult urn | timestamp | valuesCount | outliersCount
        final DataStream<CountersResult> countersResultDataStream = flaggedSensorDataDataStream
                .keyBy((KeySelector<FlaggedSensorData, String>) FlaggedSensorData::getUrn)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new OutliersDetectAggregateFunction(), new OutliersDetectProcessWindowFunction());

/*
        // Write the results in the csv
        DataStream<Tuple4<String, Long, Long, Long>> tuple4DataStream =
                countersResultDataStream.map(new OutlierTupleDataMapFunction());
        tuple4DataStream.writeAsCsv("/tmp/sensordata2.csv",
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);
*/

        // Output the results
        if (SparkConfiguration.doOutput) {
            countersResultDataStream.addSink(new RMQOut<CountersResult>(connectionConfig, SparkConfiguration.outExchange,
                    SparkConfiguration.outRoutingKey5min, new CountersResultSerializationSchema()));
        }

        // Print the CountersResult
        countersResultDataStream.print();
        final JobExecutionResult jobExecutionResult = env.execute("SparkWorks Window Processor");

        System.out.println(String.format("SparkWorks Window Processor Job took: %d ms with parallelism: %d",
                jobExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS), env.getParallelism()));
    }

}
