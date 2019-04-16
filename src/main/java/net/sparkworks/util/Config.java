package net.sparkworks.util;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Config {
    private static final Properties prop = new Properties();
    
    public static final String OUT_ROUTING_KEY_5_MIN = "5min";
    public static final String OUT_ROUTING_KEY_VALUES_60_MIN = "v60min";
    public static final String OUT_ROUTING_KEY_OUTLIERS_60_MIN = "o60min";
    
    public Config() {
        try {
            prop.load(new FileReader("./flink.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public final String getBrokerHost() {
        return prop.getProperty("broker.host", "localhost");
    }
    
    public final int getBrokerPort() {
        return Integer.parseInt(prop.getProperty("broker.port", "5672"));
    }
    
    public final String getBrokerVHost() {
        return prop.getProperty("broker.vhost", "/");
    }
    
    public final String getBrokerUsername() {
        return (String) prop.get("broker.username");
    }
    
    public final String getBrokerPassword() {
        return prop.getProperty("broker.password", "guest");
    }
    
    public final String getReadingsInputQueue() {
        return prop.getProperty("broker.queue.input.readings", "annotated-readings");
    }
    
    public final String getAnalyticsOutputExchange() {
        return prop.getProperty("broker.exchange.output.analytics", "analytics-outliers");
    }
    
    public final String getOutliersInputQueue5Min() {
        return prop.getProperty("broker.queue.input.outliers", "analytics-outliers.5min");
    }
    
    public final int getOutliersInterval() {
        return Integer.parseInt(prop.getProperty("interval.outliers", "5"));
    }
    
    public final int getOutliersOutliersInterval() {
        return Integer.parseInt(prop.getProperty("interval.outliersoutliers", "60"));
    }
    
    public final boolean doOutput() {
        return Boolean.parseBoolean(prop.getProperty("doOutput", "false"));
    }
}
