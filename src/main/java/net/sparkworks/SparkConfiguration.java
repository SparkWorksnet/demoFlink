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

    public final String queue = "annotated-readings-processing";

    public final String username = "bugs";

    public final String password = "bunny";

    public final String outExchange = "ichatz-flink-result";

    public final boolean doOutput = false;

}
