package dfki.connectors.sources;

import dfki.data.KeyedDataPoint;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


public class MimicDataSourceFunction implements SourceFunction<KeyedDataPoint<Double>> {
    private volatile boolean isRunning = true;

    private String mimic2wdbFile;  // wave file in .csv format

    /*
     Read the wave file from the .csv file
     */
    public MimicDataSourceFunction(String fileName) {
        this.mimic2wdbFile = fileName;
    }


    public void run(SourceFunction.SourceContext<KeyedDataPoint<Double>> sourceContext) throws Exception {

        // the data look like this... and we want to process ABPMean <- field 4
        // for this example I remove the first line...
        //                     0   1    2      3       4       5      6      7      8    9    10   11   12      13     14
        //            Timeanddate,HR,ABPSys,ABPDias,ABPMean,PAPSys,PAPDias,PAPMean,CVP,PULSE,RESP,SpO2,NBPSys,NBPDias,NBPMean,CO
        // '[10:36:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
        // '[10:37:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
        // '[10:38:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
        // '[10:39:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
        // '[10:40:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000

        // open each file and get line by line
        try {
            System.out.println("  FILE:" + mimic2wdbFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(mimic2wdbFile), "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {
                String rawData = line;
                String[] data = rawData.split(",");
                String var1 = data[0].replace("'[", "");
                String var2 = var1.replace("]'", "");

                long millisSinceEpoch = LocalDateTime.parse(var2, DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/uuuu"))
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
                sourceContext.collect(new KeyedDataPoint<Double>("ABPMean", millisSinceEpoch, Double.valueOf(data[4])));
                sourceContext.collect(new KeyedDataPoint<Double>("PAPMean", millisSinceEpoch, Double.valueOf(data[7])));
                sourceContext.collect(new KeyedDataPoint<Double>("PULSE", millisSinceEpoch, Double.valueOf(data[9])));
            }
            br.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void cancel() {
        this.isRunning = false;
    }



}

