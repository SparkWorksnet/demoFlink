package net.sparkworks.serialization;

import net.sparkworks.model.OutliersResult;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class OutliersOnValuesSerializer extends StdSerializer<OutliersResult> {

    public OutliersOnValuesSerializer() {
        this(null);
    }

    public OutliersOnValuesSerializer(Class<OutliersResult> t) {
        super(t);
    }

    @Override
    public void serialize(OutliersResult outliersResult, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        jgen.writeStringField("urn", outliersResult.getUrn());
        jgen.writeNumberField("timestamp", outliersResult.getTimestamp());
        jgen.writeNumberField("valuesCount", outliersResult.getOutliersCount());
        jgen.writeNumberField("outliersOnValuesCount", outliersResult.getOutliersOnValuesCount());
        jgen.writeEndObject();
    }
}
