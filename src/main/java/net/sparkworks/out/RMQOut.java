package net.sparkworks.out;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A Sink for publishing data into RabbitMQ.
 */
public class RMQOut<T> extends RMQSink<T> {

    public RMQOut(RMQConnectionConfig rmqConnectionConfig, String queueName, SerializationSchema<T> schema) {
        super(rmqConnectionConfig, queueName, schema);
    }

    @Override
    protected void setupQueue() throws IOException {
        final Map args = new HashMap();
        args.put("x-message-ttl", 60000);
        this.channel.exchangeDeclare(this.queueName, "topic", true);
    }

    /**
     * Called when new data arrives to the sink, and forwards it to RMQ.
     *
     * @param value The incoming data
     */
    @Override
    public void invoke(T value) {
        try {
            byte[] msg = schema.serialize(value);
            channel.basicPublish(this.queueName, queueName, null, msg);
        } catch (IOException e) {
            throw new RuntimeException("Cannot send RMQ message " + queueName, e);
        }
    }

}
