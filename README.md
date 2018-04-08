The repository includes a collection of examples for stream processing of data arriving from IoT.
Data are collected based on a [RabbitMQ](https://www.rabbitmq.com/) queue and they are processed by [Apache Flink](https://flink.apache.org/).
The code is in JAVA.

# Retrieving IoT Data

The data are retrieved from a live deployment of the [Sparks IoT platform](https://sparks.io) over a collection of buildings.
The Sparks IoT platform delivers all data collected from the IoT deployments through a [RabbitMQ](https://www.rabbitmq.com/) queue.
Using Flink's terminology, data are retrieved using a [Flink RabbitMQ connector](https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/connectors/rabbitmq.html).
The connection parameters are declared using a RMQConnectionConfig object as follows:

```java
RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
        .setHost("broker.sparkworks.net")
        .setPort(5672)
        .setUserName("username")
        .setPassword("password")
        .setVirtualHost("/")
        .build();
```

The queue that stores the messages within the [Sparks IoT platform](https://sparks.io) defines a TTL (time-to-live) for
each message of 10000. Optional queue arguments are supported in flink by extending the
[RMQSource class](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/connectors/rabbitmq/RMQSource.html).
For this reason under the *net.sparkworks.util* package the [RBQueue class](src/net/sparkworks/util/RBQueue.java) extends the [RMQSource class](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/connectors/rabbitmq/RMQSource.html) 
by overwritting the *setupQueue* method as follows:

```java
void setupQueue() throws IOException {
    Map args = new HashMap();
    args.put("x-message-ttl", 10000);

    this.channel.queueDeclare(this.queueName,
            true,
            false,
            false, args);
}
```

Based on the above two steps, the data source is defined as follows:

```java
DataStream<String> rawStream = env
    .addSource(new RBQueue<String>(
            connectionConfig,
            "ichatz-annotated-readings",
            true,
            new SimpleStringSchema()));
```

# Transformation of IoT Data

Each message arriving on the message queue has the following format:

```
device urn,value,timestamp
```

As soon as data are arriving on the flink, the first step is to use a map transformation and convert them into a POJO object.
The [SensorData](src/net/sparkworks/model/SensorData.java) class is defined for this purpose under the *net.sparkworks.model* package.

The transformation step is specified within the [SensorDataMapFunction](src/net/sparkworks/functions/SensorDataMapFunction.java) class that resides with the *net.sparkworks.functions* package.
The map transformation is applied over the data stream as follows:

```java
DataStream<SensorData> dataStream = // convert RabbitMQ messages to SensorData
    rawStream.map(new SensorDataMapFunction());
```

# A simple message listener

Within the *net.sparkworks.stream* package, the [StreamListener](src/net/sparkworks/stream/StreamListener.java) class defines a simple example for retrieving data from the
RabbitMQ queue and applying the above transformation on the data.

The flink job will continuously output the sensor values received from the data stream.

# Aggregation of IoT data using a window

The next example aggregates the IoT data based on a Window of 5 minutes. The code can be found within the [StreamProcessor](src/net/sparkworks/stream/StreamProcessor.java) class.
Data arriving from the same device (i.e., with the same device urn) are grouped together so that an aggregate fuction is applied on them.
To this end a KeyedStream is defined as follows:

```java
KeyedStream<SensorData, String> keyedStream = dataStream
    .keyBy(new KeySelector<SensorData, String>() {

        public String getKey(SensorData value) {
            return value.getUrn();
        }
    });
```

Given the grouping of the stream of data based on the device urn,
the final aggregation is defined within the *net.sparkworks.functions* package,
the [SensorDataAverageReduce](src/net/sparkworks/functions/SensorDataAverageReduce.java) class.
The aggregation is essentially a reduce transformation step where an average overall the values collected is generated.

```java
SensorData reduce(SensorData a, SensorData b) {
    SensorData value = new SensorData();
    value.setUrn(a.getUrn());
    value.setValue((a.getValue() + b.getValue()) / 2);
    return value;
}
```

Based on the above map/reduce transformation, the final step is to define the window of 5 minutes and finalize the processing:

```java
DataStream resultStream = keyedStream
        .timeWindow(Time.minutes(5))
        .reduce(new SensorDataAverageReduce());
```

Every 5 minutes the flink job will output the aggregated values over the sensor values retrieved during this period of time.

# Window processing based on event timestamps

Flink supports different [notions of time in streaming programming](https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/event_time.html).
In the above example the time windows are computed using the system clock of the machines that run the respective operator.
The five minute processing time window will include all data that arrived at a specific operator between the times
when the system clock indicated the five minute period. This is known as *processing time*, and it is the simplest
notion of time and requires no coordination between streams and machines.
It provides the best performance and the lowest latency.
However, in IoT based distributed and asynchronous environments processing time does not provide determinism,
because it is susceptible to the speed at which records arrive in the system (for example from the message queue),
and to the speed at which the records flow between operators inside the system.

In this example the *event time* is used to process the time-based operators. The timestamp provided by the IoT devices
is already embedded on the RabbitMQ messages (see above) before they enter Flink.
This event timestamp is extracted from the record and used by Flink to compute the time windows.
In this example, a five minute time window will contain all records that carry an event timestamp that falls into that
five minute period, regardless of when the records arrive, and in what order they arrive.

Selecting the *event time* approach for time-based operator first requires to set a environment level parameter as follows:

```java
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
```

Event time gives correct results even on out-of-order events, late events, or on replays of data from backups or persistent logs.
In event time, the progress of time depends on the data, not on any wall clocks.
Event time programs must specify how to generate Event Time Watermarks,
which is the mechanism that signals progress in event time.

In order to work with event time, Flink needs to know the events’ timestamps, meaning each element in the stream needs
to have its event timestamp assigned. For this purpose within the *net.sparkworks.util* package,
the [TimestampExtractor](src/net/sparkworks/util/TimestampExtractor.java) class is defined.

```java
long currentTimestamp = Long.MIN_VALUE;

long extractTimestamp(SensorData element, long previousElementTimestamp) {
    currentTimestamp = element.getTimestamp();
    return element.getTimestamp();
}

public final Watermark getCurrentWatermark() {
    return new Watermark(currentTimestamp);
}
```

Therefore, after applying the first map transformation ([SensorDataMapFunction](src/net/sparkworks/functions/SensorDataMapFunction.java))
that parses the messages arriving on the RabbitMQ queue, the timestamps are extracted as follows:

```java
DataStream<SensorData> timedStream =
    dataStream.assignTimestampsAndWatermarks(new TimestampExtractor());
```

