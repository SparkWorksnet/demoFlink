package net.sparkworks.out;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;

/**
 * A Sink for publishing data into RabbitMQ.
 */
public class RMQOut<T> extends RMQSink<T> {

    private String routingKey;

    public RMQOut(RMQConnectionConfig rmqConnectionConfig, String queueName, SerializationSchema<T> schema) {
        super(rmqConnectionConfig, queueName, schema);
        this.routingKey = queueName;
    }

    public RMQOut(RMQConnectionConfig rmqConnectionConfig, String queueName, String routingKey, SerializationSchema<T> schema) {
        super(rmqConnectionConfig, queueName, schema);
        this.routingKey = routingKey;
    }

    @Override
    protected void setupQueue() throws IOException {
        this.channel.exchangeDeclarePassive(this.queueName);
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
            channel.basicPublish(this.queueName, routingKey, null, msg);
        } catch (IOException e) {
            throw new RuntimeException("Cannot send RMQ message " + queueName, e);
        }
    }

}
