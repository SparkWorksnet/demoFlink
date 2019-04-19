package net.sparkworks.serialization;

import net.sparkworks.model.OutliersResult;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerationException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;

public class OutliersSerializationSchema implements SerializationSchema<OutliersResult> {

    public byte[] serialize(final OutliersResult element) {

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(OutliersResult.class, new OutliersSerializer());
        mapper.registerModule(module);

        String jsonInString = "";
        try {
            jsonInString = mapper.writeValueAsString(element);
        } catch (JsonGenerationException | JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jsonInString.getBytes();
    }
}
