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
        args.put("x-message-ttl", 10000);

        this.channel.queueDeclare(this.queueName,
                true,
                false,
                false, args);
    }

}
