package dfki.util;

import dfki.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.text.SimpleDateFormat;
import java.util.Date;


public class UserDefinedFunctions {

    public static class ExtractTimestamp extends AscendingTimestampExtractor<KeyedDataPoint<Double>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(KeyedDataPoint<Double> element) {
            return element.getTimeStampMs();
        }
    }


    public static class KeyedDataPoint2Tuple implements MapFunction<KeyedDataPoint<Double>, Tuple3<String,String,Double>> {
        @Override
        public Tuple3<String,String,Double> map(KeyedDataPoint<Double> in) throws Exception {
            Date date = new Date(in.getTimeStampMs());
            // to print the date in normal format
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            return new Tuple3<String, String,Double>(sdf.format(date),in.getKey(), in.getValue());
        }
    }

}
