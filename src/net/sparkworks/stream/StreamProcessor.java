package net.sparkworks.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 * A simple Flink stream processing engine connecting to the SparkWorks message broker.
 *
 * @author ichatz@gmail.com
 */
public class StreamProcessor {

    public static void main(String[] args) throws Exception {

        // The StreamExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Setup the connection settings to the RabbitMQ broker
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("broker.sparkworks.net")
                .setPort(5672)
                .setUserName("username")
                .setPassword("password")
                .setVirtualHost("/")
                .build();

        final DataStream<String> rawStream = env
                .addSource(new RMQSource<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        "ichatz-annotated-readings", // name of the RabbitMQ queue to consume
                        true,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema())); // deserialization schema to turn messages into Java objects

        final DataStream<Tuple2<String, Integer>> procStream = rawStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        final String[] items = value.split(",");
                        // extract value
                        final String txtValue = items[1];
                        try {
                            final int intValue = Integer.parseInt(txtValue);

                            out.collect(new Tuple2(items[0], intValue));
                        } catch (Exception ex) {
                            // either district does not exist or it is not an integer
                            // simply ignore
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // print the results with a single thread, rather than in parallel
        procStream.print().setParallelism(1);

        env.execute("SparkWorks Stream Processing Demo");
    }

}
