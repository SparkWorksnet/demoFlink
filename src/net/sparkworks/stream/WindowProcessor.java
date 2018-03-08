package net.sparkworks.stream;

import net.sparkworks.SparkConfiguration;
import net.sparkworks.functions.SensorDataAverageReduce;
import net.sparkworks.functions.SensorDataMapFunction;
import net.sparkworks.model.SensorData;
import net.sparkworks.util.RBQueue;
import net.sparkworks.util.TimestampExtractor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * A simple Flink stream processing engine connecting to the SparkWorks message broker.
 * Groups data based on the URN and produces an average value over all values received within a window of 5 minutes.
 * In contrast to StreamProcessor, in this class the timestamp provided by the IoT domain is used for the computation
 * of the windows.
 *
 * @author ichatz@gmail.com
 */
public class WindowProcessor {

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
                        new SimpleStringSchema()))
                .setParallelism(1); // deserialization schema to turn messages into Java objects

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

        // Define the window and apply the reduce transformation
        final DataStream resultStream = keyedStream
                .timeWindow(Time.seconds(10))
                .reduce(new SensorDataAverageReduce());

        // print the results with a single thread, rather than in parallel
        resultStream.print().setParallelism(1);

        env.execute("SparkWorks Window Processor");
    }

}
