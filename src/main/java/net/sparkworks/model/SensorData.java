package net.sparkworks.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * POJO object for a SensorData single value.
 *
 * @author ichatz@gmail.com
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class SensorData {

    private String urn;

    private double value;

    private long timestamp;

    public static SensorData fromString(String line) {
        
        String[] tokens = line.split("(,|;)\\s*");
        if (tokens.length != 3) {
            throw new IllegalStateException("Invalid record: " + line);
        }
        SensorData event = new SensorData();
        
        try {
            event.setUrn(tokens[0]);
            event.setTimestamp(Long.parseLong(tokens[2]));
            event.setValue(Double.parseDouble(tokens[1]));
            
        } catch (Exception e) {
            throw new IllegalStateException("Invalid field: " + line, e);
        }
        
        return event;
    }
    
}
