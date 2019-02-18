package net.sparkworks.stream;

import net.sparkworks.functions.STDApplyWindowFunction;
import net.sparkworks.functions.SensorDataAscendingTimestampExtractor;
import net.sparkworks.functions.SummaryAggregateFunction;
import net.sparkworks.functions.SummaryProcessWindowFunction;
import net.sparkworks.model.SensorData;
import net.sparkworks.model.SummaryResult;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class STDRemoveOutliersProcessor {

    public static void main(String[] args) throws Exception {


        // The StreamExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Setup the connection settings to the RabbitMQ broker
/*
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(SparkConfiguration.brokerHost)
                .setPort(SparkConfiguration.brokerPort)
                .setUserName(SparkConfiguration.username)
                .setPassword(SparkConfiguration.password)
                .setVirtualHost(SparkConfiguration.brokerVHost)
                .build();
*/

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

        final DataStream<String> rawStream = env.readTextFile(filename); // deserialization schema to turn messages into SensorData objects

        DataStream<SensorData> sensorDataStream = rawStream
                .map((MapFunction<String, SensorData>) line -> SensorData.fromString(line))
                .assignTimestampsAndWatermarks(new SensorDataAscendingTimestampExtractor());

        // Group data by sensor urn
        final WindowedStream<SensorData, String, TimeWindow> windowedStream = sensorDataStream
                .keyBy((KeySelector<SensorData, String>) SensorData::getUrn)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)));

        // Aggregate and print
        windowedStream.aggregate(new SummaryAggregateFunction(), new SummaryProcessWindowFunction()).print();

        // Remove outliers from sensor data stream
        final DataStream<SensorData> dataStreamWithoutOutliers = windowedStream.apply(new STDApplyWindowFunction());

        // Get the summaryResult after having removed the outliers
        final DataStream<SummaryResult> summaryResultWithoutOutliers = dataStreamWithoutOutliers
                .keyBy((KeySelector<SensorData, String>) SensorData::getUrn)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new SummaryAggregateFunction(), new SummaryProcessWindowFunction());

        summaryResultWithoutOutliers.print();

        final JobExecutionResult jobExecutionResult = env.execute("SparkWorks Window Processor");

        System.out.println(String.format("SparkWorks Window Processor Job took: %d ms with parallelism: %d",
                jobExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS), env.getParallelism()));
    }
}
