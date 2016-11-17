package edu.umkc.calmcoders.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ibm.watson.developer_cloud.alchemy.v1.AlchemyLanguage;
import com.ibm.watson.developer_cloud.alchemy.v1.model.Document;
import com.ibm.watson.developer_cloud.alchemy.v1.model.DocumentSentiment;
import com.mongodb.*;
import org.apache.commons.collections.map.HashedMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by latha on 11/13/16.
 */
public class SentimentAnalysisBolt extends BaseRichBolt {
    String Tweet, tweetlanguaue, result, sentimentType;
    int positive = 0, negative = 0, neutral = 0;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        Tweet = tuple.getStringByField("tweet");

        result = Tweet.replaceAll("[-+.^:,)/@;=_*&%$#!-]","");
        tweetlanguaue = tuple.getStringByField("language");

        //Creating Sentiment Object
        AlchemyLanguage service = new AlchemyLanguage();
        service.setApiKey("7e9d1a8ed86a7a431093541b160cc800559a33c7");

        //Building the HashMap
        if (tweetlanguaue.equalsIgnoreCase("en")) {
            DocumentSentiment sentiment;
            System.out.println("Language:" + tweetlanguaue);
            System.out.println("Tweet:" + result);
            Map<String, Object> params = new HashMap<String, Object>();
            params.put(AlchemyLanguage.TEXT, result);

            //System.out.println("Text Language:" + service.getLanguage(params).execute());
            try {
                sentiment = service.getSentiment(params).execute();
                sentimentType = sentiment.getSentiment().getType().toString();
                System.out.println("Sentiment:" + sentimentType);
                if (sentimentType.equalsIgnoreCase("POSITIVE")){
                    positive++;
                }
                else if (sentimentType.equalsIgnoreCase("NEGATIVE")){
                    negative++;
                }
                else{
                    neutral++;
                }
            }catch (Exception e){
                //System.out.println(e);
            }
            //Storing in MongoDB
            MongoClientURI uri = new MongoClientURI("mongodb://user:user@ds151697.mlab.com:51697/calmcoders");
            MongoClient client = new MongoClient(uri);

            DB db = client.getDB(uri.getDatabase());
            DBCollection twitterdata = db.getCollection("twitterdata");

            BasicDBObject updateDocument = new BasicDBObject();
            updateDocument.append("$set", new BasicDBObject()
                    .append("Positive", positive)
                    .append("Negative", negative)
                    .append("Neutral", neutral));

            BasicDBObject searchQuery2 = new BasicDBObject().append("sentimentType", "count");

            twitterdata.update(searchQuery2, updateDocument);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
