package dfki;

import dfki.connectors.sinks.InfluxDBSink;
import dfki.connectors.sources.MimicDataSourceFunction;
import dfki.data.KeyedDataPoint;
import dfki.functions.MovingAverageFunction;
import dfki.util.UserDefinedFunctions;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

/**
 * run with these parameters:
 *   --input ./src/main/resources/a40834n.csv --orderMA 10 --output signal.csv
 *   The data in the example has one value per minute, so here orderMA 10 means average
 *   every 10 minutes
 */
public class MimicDataJob {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        // Checking input parameters
        if (!parameters.has("input") ) {
            throw new Exception("Input Data is not specified");
        }

        String mimicFile = parameters.get("input");
        String outCsvFile = parameters.get("output");
        int orderMA = Integer.valueOf(parameters.get("orderMA"));
        System.out.println("  Input file: " + mimicFile);


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        @SuppressWarnings({"rawtypes", "serial"})

        // test with file in: src/main/resources/a40834n.csv
        DataStream<KeyedDataPoint<Double>> mimicData = env.addSource(new MimicDataSourceFunction(mimicFile))
                .assignTimestampsAndWatermarks(new UserDefinedFunctions.ExtractTimestamp());


        mimicData.keyBy("key")
                // mimic data is sampled every minute
                //
                .window(SlidingEventTimeWindows.of(Time.minutes(orderMA), Time.minutes(1)))
                .trigger(CountTrigger.of(orderMA))
                //
                .apply(new MovingAverageFunction())
                // simple print in consola
                //.print();
                //
                // If sinking the data in InfluxDB
                //.addSink(new InfluxDBSink<>("mimicData", "ABPMeanAvg"));
                //
                // we can also save the data in a csv file, but for that we need to transform it to Tuple
                .map(new UserDefinedFunctions.KeyedDataPoint2Tuple())
                .writeAsCsv("movingAvg-"+outCsvFile, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);


        // save the input data in a csv file
        mimicData.map(new UserDefinedFunctions.KeyedDataPoint2Tuple())
                .writeAsCsv("input-"+outCsvFile, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);


        System.out.println("  Result saved in file: " + outCsvFile);


        env.execute("MimicDataJob");
    }





}
