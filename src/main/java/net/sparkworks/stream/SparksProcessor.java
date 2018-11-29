package net.sparkworks.stream;

import net.sparkworks.SparkConfiguration;
import net.sparkworks.functions.SensorDataAscendingTimestampExtractor;
import net.sparkworks.functions.SummaryAggregateFunction;
import net.sparkworks.functions.SummaryProcessWindowFunction;
import net.sparkworks.model.SensorData;
import net.sparkworks.model.SummaryResult;
import net.sparkworks.serialization.SensorDataDeserializationSchema;
import net.sparkworks.util.RBQueue;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * Created by akribopo on 09/09/2018.
 */
public class SparksProcessor {

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

        final DataStream<SensorData> rawStream = env
                .addSource(new RBQueue<>(
                        connectionConfig,                        // config for the RabbitMQ connection
                        SparkConfiguration.queue,                // name of the RabbitMQ queue to consume
                        true,                     // use correlation ids; can be false if only at-least-once is required
                        new SensorDataDeserializationSchema())); // deserialization schema to turn messages into SensorData objects

        // convert RabbitMQ messages to SensorData
        final DataStream<SummaryResult> dataStream = rawStream
                // Assign Timestamps
                .assignTimestampsAndWatermarks(new SensorDataAscendingTimestampExtractor())
                // Group by device based on urn
                .keyBy((KeySelector<SensorData, String>) SensorData::getUrn)
                // Split into 5 minutes Time Windows
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                // Aggregate
                .aggregate(new SummaryAggregateFunction(), new SummaryProcessWindowFunction());


        // print the results with a single thread, rather than in parallel
        dataStream.print().setParallelism(1);

        env.execute("SparkWorks Window Processor");
    }
}
