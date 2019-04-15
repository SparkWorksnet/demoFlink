package net.sparkworks;

/**
 * Central point for setting the configuration parameters
 *
 * @author ichatz@gmail.com
 */
public interface SparkConfiguration {

    public final String brokerHost = "broker.sparkworks.net";

    public final int brokerPort = 5672;

    public final String brokerVHost = "/";

    public final String queue = "ntsironis-annotated-readings";

    public final String username = "ntsironis";

    public final String password = "mxHHiI8L";

    public final String outExchange = "ntsironis-analytics-outliers";

    public final String outliersQueue5min = "ntsironis-analytics-outliers.5min";

    public final String outliersQueueV60min = "ntsironis-analytics-outliers.v60min";

    public final String outliersQueueO60min = "ntsironis-analytics-outliers.o60min";

    public final String outRoutingKey5min = "5min";

    public final String outRoutingKeyValues60min = "v60min";

    public final String outRoutingKeyOutliers60min = "o60min";

    public final boolean doOutput = true;

}
