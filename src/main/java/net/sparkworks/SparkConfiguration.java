package net.sparkworks;

/**
 * Central point for setting the configuration parameters
 *
 * @author ichatz@gmail.com
 */
public interface SparkConfiguration {

    public final String brokerHost = "localhost";

    public final int brokerPort = 5672;

    public final String brokerVHost = "/";

    public final String queue = "annotated-readings";

    public final String username = "guest";

    public final String password = "guest";

    public final String outExchange = "analytics-outliers";

    public final boolean doOutput = false;

}
