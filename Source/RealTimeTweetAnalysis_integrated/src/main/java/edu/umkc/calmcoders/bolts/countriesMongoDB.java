package edu.umkc.calmcoders.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by latha on 11/11/16.
 */
public class countriesMongoDB extends BaseRichBolt{
    JSONArray ja = new JSONArray();


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {


    }

    @Override
    public void execute(Tuple tuple) {



        String countryname=tuple.getStringByField("country");
        Long count=tuple.getLongByField("count");

        MongoClientURI uri = new MongoClientURI("mongodb://manikanta:manikanta@ds035836.mlab.com:35836/twitterdata");
        MongoClient client = new MongoClient(uri);

        DB db = client.getDB(uri.getDatabase());
        DBCollection location = db.getCollection("countriesdata");

        JSONObject jsonObj  = new JSONObject();



        BasicDBObject searchQuery2 = new BasicDBObject().append("countries", "topcountries");
        //DBObject olddocument=location.findOne(searchQuery2);

        //Object oldlocations =olddocument.get("lattitude");
        //oldlocations.toString();
        //olddocument.put("lattitude",lattitude);

        BasicDBObject topcountries = new BasicDBObject();

        JSONObject jo = new JSONObject();
        jo.put("country", countryname);
        jo.put("count", count);


        ja.add(jo);

        System.out.println("updated array: "+Arrays.deepToString(ja.toArray()));


        //olddocument.put("locations",jo);

        topcountries.put("topcountries",ja);
        BasicDBObject updateDocument2 = new BasicDBObject();


        BasicDBObject updateDocument = new BasicDBObject();
        //updateDocument.append("$set",jo);
        updateDocument.append("$set", new BasicDBObject().append("topcountries", ja));

        location.update(searchQuery2, updateDocument);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
