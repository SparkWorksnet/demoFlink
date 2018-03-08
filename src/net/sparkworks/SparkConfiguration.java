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

    public final String username = "username";

    public final String password = "password";

}
