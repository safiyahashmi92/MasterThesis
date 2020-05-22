package dfki.connectors.sinks;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.ParseException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

public class ElasticSearch implements ElasticsearchSinkFunction<Tuple6<String,Date,Integer,Double,Double,Double>> {

    public IndexRequest createIndexRequest(Tuple6<String,Date,Integer,Double,Double,Double>element) throws ParseException {
        Map<String, Object> json = new HashMap<>();
        Map<String, Object> jsonLocation = new HashMap<>();


        jsonLocation.put("lat", Double.valueOf(element.f5));
        jsonLocation.put("lon", Double.valueOf(element.f4));
        json.put("location", jsonLocation);

        json.put("countryCode", element.f0);
        json.put("timestamp", element.f1);
        json.put("eventCode", element.f2);
        json.put("NumMentions", element.f3);

        //System.out.println(json);

        return Requests.indexRequest()
                .index("eventdata_with_location")
                .type("_doc")
                .source(json);
    }

    @Override
    public void process(Tuple6<String,Date,Integer,Double,Double,Double> element, RuntimeContext ctx, RequestIndexer indexer) {
        try {
            indexer.add(createIndexRequest(element));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}

