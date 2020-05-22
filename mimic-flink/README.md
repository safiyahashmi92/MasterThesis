## Processing MIMIC wave (time series) data with Flink

This project requires: 
- Java 8.0, Apache Maven 3.6.0, one IDE (Eclipse, IntelliJ) 

After importing the project run the MimicDataJob with the following parameters:

    --input ./src/main/resources/a40834n.csv --orderMA 10 --output signal.csv
A data example is included in resources, the program load three signals from the mimic wave file: ABPMean, PAPMean and PULSE and calcculate a moving average of orderMa for those signals.

The output is saved in two files: 

    input-signal.csv and movingAvg-signal.csv 
 
The signals can be plot with the script in the directory R:

     plot_mimic.R

## Useful links:

## Time series processing

The book: [Forecasting: Principles and Practice, Rob J Hyndman and George Athanasopoulos, Monash University, Australia](https://otexts.com/fpp2/)
has very good material for example:  [Moving average](https://otexts.com/fpp2/moving-averages.html)

## Streaming examples 

[Streaming examples (Apache Flink github repo)](https://github.com/apache/flink/tree/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples)


## Influx vizualisation

https://www.ververica.com/blog/robust-stream-processing-flink-walkthrough

https://www.youtube.com/watch?v=fstKKxvY23c&feature=youtu.be

https://github.com/dataArtisans/oscon

## Kafka-Influx-Grafana vizualisation

https://github.com/antonrud/flink-kafka-influx-grafana-framework

## Grafana vizualisation

https://github.com/dataArtisans/flink-streaming-demo


