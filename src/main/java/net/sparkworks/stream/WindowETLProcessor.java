package net.sparkworks.stream;

import net.sparkworks.SparkConfiguration;
import net.sparkworks.functions.ETLApply;
import net.sparkworks.functions.ETLApplyApacheMath;
import net.sparkworks.functions.SensorDataMapFunction;
import net.sparkworks.functions.TimestampMapFunction;
import net.sparkworks.model.SensorData;
import net.sparkworks.out.RMQOut;
import net.sparkworks.serialization.SensorDataSerializationSchema;
import net.sparkworks.util.RBQueue;
import net.sparkworks.util.TimestampExtractor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 * A simple Flink stream processing engine connecting to the SparkWorks message broker.
 * Groups data based on the URN and produces an average value over all values received within a window of 5 minutes.
 * Like WindowProcessor, the timestamp provided by the IoT domain is used for the computation
 * of the windows.
 * This class provides an ETL operation that is implemented using the APPLY operator.
 *
 * <p>Note that this function requires that all data in the windows is buffered until the window
 * is evaluated, as the function provides no means of incremental aggregation.
 *
 * @author ichatz@gmail.com
 */
public class WindowETLProcessor {

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
                        connectionConfig,            // config for the RabbitMQ connection
                        SparkConfiguration.queue, // name of the RabbitMQ queue to consume
                        true,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema())); // deserialization schema to turn messages into Java objects

        // convert RabbitMQ messages to SensorData
        final DataStream<SensorData> dataStream = rawStream
                .map(new SensorDataMapFunction());

        // Assign timestamps
        final DataStream<SensorData> timedStream =
                dataStream.assignTimestampsAndWatermarks(new TimestampExtractor());

        // Key messages based on the URN
        final KeyedStream<SensorData, String> keyedStream = timedStream
                .keyBy(new KeySelector<SensorData, String>() {

                    public String getKey(SensorData value) {
                        return value.getUrn();
                    }
                });

        final int windowMinutes = 5;

        // Define the window
        final WindowedStream<SensorData, String, TimeWindow> resultStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.minutes(windowMinutes)))
                .allowedLateness(Time.days(10000));

        // Execute the ETL for each tumbling window of the grouped values
        final DataStream<SensorData> reducedStream = resultStream.apply(new ETLApplyApacheMath());

        final TimestampMapFunction tmfunc = new TimestampMapFunction();
        tmfunc.setWindowMinutes(windowMinutes);

        // Final transformation to set the timestamp to the start of the window
        final DataStream<SensorData> finalStream = reducedStream.map(tmfunc);

        if (SparkConfiguration.doOutput) {
            finalStream.addSink(new RMQOut<SensorData>(connectionConfig, SparkConfiguration.outExchange, new SensorDataSerializationSchema()));
        }
        // print the results with a single thread, rather than in parallel
//        finalStream.print().setParallelism(1);

        env.execute("SparkWorks Window ETL Processor");
    }

}
