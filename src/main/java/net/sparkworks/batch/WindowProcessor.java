package net.sparkworks.batch;

import net.sparkworks.functions.SensorTupleDataMapFunction;
import net.sparkworks.functions.TimestampMapFunction;
import net.sparkworks.model.SensorData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class WindowProcessor {

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        // the filename to use as input dataset
        final String filename;
        try {
            // access the arguments of the command line tool
            final ParameterTool params = ParameterTool.fromArgs(args);
            if (!params.has("filename")) {
                filename = "/tmp/sensordata.csv";
                System.err.println("No filename specified. Please run 'WindowProcessor " +
                        "--filename <filename>, where filename is the name of the dataset in CSV format");
            } else {
                filename = params.get("filename");
            }

        } catch (Exception ex) {
            System.err.println("No filename specified. Please run 'WindowProcessor " +
                    "--filename <filename>, where filename is the name of the dataset in CSV format");
            return;
        }

        // The StreamExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create a dataset based on the csv file
        final DataSet<Tuple3<String, Long, Double>> rawData =
                env.readCsvFile(filename)
                        .includeFields("111")
                        .types(String.class, Long.class, Double.class);

        // convert CSV lines to SensorData
        final DataSet<SensorData> dataSet = rawData
                .map(new SensorTupleDataMapFunction());

        // print the results
        System.out.println("Total Urn: " + dataSet
                .distinct("urn")
                .count());

        final int windowMinutes = 1;

        final TimestampMapFunction tmfunc = new TimestampMapFunction();
        tmfunc.setWindowMinutes(windowMinutes);

        // Flat the timestamps of the messages based on the 5-minute window
        final DataSet<SensorData> windowedSet = dataSet
                .map(tmfunc);

        // print the results
        System.out.println("Total Timestamp slots: " + windowedSet
                .distinct("timestamp")
                .count());

        // print the results
        System.out.println("Total Urn/Timestamp slots: " + windowedSet
                .distinct("urn", "timestamp")
                .count());

        // Group by messaged based on the URN and Timestamp
        final DataSet<SensorData> resultSet = windowedSet
                .groupBy("urn", "timestamp").reduceGroup(new ApacheReduce());

        // print the results
        resultSet.print();
        System.out.println("END execution took " + (System.currentTimeMillis() - start));
    }


}
