package net.sparkworks.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Overriding RabbitMQ Source class in order to pass custom arguments for the X-MESSAGE-TTL setting.
 *
 * @author ichatz@gmail.com
 * @see RMQSource
 */
public class RBQueue<OUT>
        extends RMQSource<OUT> {

    public RBQueue(RMQConnectionConfig rmqConnectionConfig,
                     String queueName,
                     boolean usesCorrelationId,
                     DeserializationSchema<OUT> deserializationSchema) {
        super(rmqConnectionConfig, queueName, usesCorrelationId, deserializationSchema);
    }

    protected void setupQueue() throws IOException {
        Map args = new HashMap();
        args.put("x-message-ttl", 10000);

        this.channel.queueDeclare(this.queueName,
                true,
                false,
                false, args);
    }

}
