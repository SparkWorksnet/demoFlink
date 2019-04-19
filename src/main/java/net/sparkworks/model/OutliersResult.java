package net.sparkworks.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerationException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class OutliersResult {

    private String urn;

    private long timestamp;

    //in OutliersProcessor: counts the number of measurements
    //in ValueCountAndOutlierCountOutlierProcessor: counts the number of 5minute intervals (first time around)
    private long valuesCount;
    
    //in OutliersProcessor: counts the number of outliers in the time interval
    //in ValueCountAndOutlierCountOutlierProcessor (second time): counts the number of measurements
    private long outliersCount;

    // in ValueCountAndOutlierCountOutlierProcessor: counts the number of outliers in the values of the 5minute intervals
    // (first time around)
    private long outliersOnValuesCount;

    // in ValueCountAndOutlierCountOutlierProcessor: counts the number of outliers in the outliers in 5minute intervals
    // (second time around)
    private long outliersOnOutliersCount;

    public OutliersResult(String urn, long timestamp, long valuesCount, long outliersCount) {
        this.urn = urn;
        this.timestamp = timestamp;
        this.valuesCount = valuesCount;
        this.outliersCount = outliersCount;
    }

    public static OutliersResult fromString(String line) {

        ObjectMapper mapper = new ObjectMapper();
        OutliersResult outliersResult = new OutliersResult();
        try {
            outliersResult = mapper.readValue(line, OutliersResult.class);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outliersResult;
    }
}
