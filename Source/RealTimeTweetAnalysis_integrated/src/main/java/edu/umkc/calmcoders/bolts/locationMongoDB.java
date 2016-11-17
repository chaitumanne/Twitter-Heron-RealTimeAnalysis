package edu.umkc.calmcoders.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by latha on 11/11/16.
 */
public class locationMongoDB extends BaseRichBolt{
    JSONArray ja = new JSONArray();


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {


    }

    @Override
    public void execute(Tuple tuple) {


        Double lattitude=tuple.getDoubleByField("lattitude");
        Double longitude=tuple.getDoubleByField("longitude");

        MongoClientURI uri = new MongoClientURI("mongodb://manikanta:manikanta@ds035836.mlab.com:35836/twitterdata");
        MongoClient client = new MongoClient(uri);

        DB db = client.getDB(uri.getDatabase());
        DBCollection location = db.getCollection("analyticdata");

        JSONObject jsonObj  = new JSONObject();



        BasicDBObject searchQuery2 = new BasicDBObject().append("geolocation", "coordinates");
        //DBObject olddocument=location.findOne(searchQuery2);

        //Object oldlocations =olddocument.get("lattitude");
        //oldlocations.toString();
        //olddocument.put("lattitude",lattitude);

        BasicDBObject locations = new BasicDBObject();

        JSONObject jo = new JSONObject();
        jo.put("lat", lattitude);
        jo.put("lng", longitude);


        ja.add(jo);

        System.out.println("updated array: "+Arrays.deepToString(ja.toArray()));


        //olddocument.put("locations",jo);

        locations.put("locations",ja);
        BasicDBObject updateDocument2 = new BasicDBObject();


        BasicDBObject updateDocument = new BasicDBObject();
        //updateDocument.append("$set",jo);
        updateDocument.append("$set", new BasicDBObject().append("locations", ja));

        location.update(searchQuery2, updateDocument);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
