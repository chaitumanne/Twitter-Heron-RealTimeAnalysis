package edu.umkc.calmcoders.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;

import java.util.*;

/**
 * Created by latha on 11/11/16.
 */
public class MongoDriverBolt extends BaseRichBolt{
    BasicDBObject query = new BasicDBObject();
    HashMap<String, Integer> keyTags = new HashMap<String, Integer>();
    int tweetCount = 0;
    String One, Two, Three, Four, Five, Six, Seven, Eight, Nine, Ten;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    @Override
    public void execute(Tuple tuple) {

        String HashTag = tuple.getStringByField("hashtags");
        String[] tags = HashTag.split(",");

        for (String t : tags) {
            if(!t.trim().isEmpty()){
                if (keyTags.containsKey(t.trim())){
                    tweetCount = keyTags.get(t.trim());
                    tweetCount++;
                    keyTags.put(t.trim(),tweetCount);
                    System.out.println("PKeys:" + t.trim() + "PValue:" + keyTags.get(t.trim()));
                }
                else {
                    keyTags.put(t.trim(),1);
                    System.out.println("Keys:" + t.trim() + "Value:" + keyTags.get(t.trim()));
                }
            }
        }

        List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(keyTags.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2){
                return(o2.getValue()).compareTo(o1.getValue());
            }
        });

        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for(Map.Entry<String, Integer> e1 : list){
            sortedMap.put(e1.getKey(), e1.getValue());
        }

        List<String> topTenTags = new ArrayList<String>();
        for (Map.Entry<String, Integer> e : sortedMap.entrySet()){
            if (topTenTags.size() > 9){
                break;
            }
            topTenTags.add("#" + e.getKey() + "," + e.getValue());
        }

        //System.out.println("Top Ten Hash Tags:" + topTenTags);
        for(int i = 0; i < topTenTags.size(); i++){
            One = topTenTags.get(0);
            //System.out.println("Tag " + i + ":" + topTenTags.get(i));
        }


        MongoClientURI uri = new MongoClientURI("mongodb://user:user@ds151697.mlab.com:51697/calmcoders");
        MongoClient client = new MongoClient(uri);

        DB db = client.getDB(uri.getDatabase());
        DBCollection twitterdata = db.getCollection("twitterdata");

        //System.out.println("Length:" + topTenTags.size());
        if (topTenTags.size() >= 10) {
            BasicDBObject updateDocument = new BasicDBObject();
            updateDocument.append("$set", new BasicDBObject()
                    .append("One", topTenTags.get(0))
                    .append("Two", topTenTags.get(1))
                    .append("Three", topTenTags.get(2))
                    .append("Four", topTenTags.get(3))
                    .append("Five", topTenTags.get(4))
                    .append("Six", topTenTags.get(5))
                    .append("Seven", topTenTags.get(6))
                    .append("Eight", topTenTags.get(7))
                    .append("Nine", topTenTags.get(8))
                    .append("Ten", topTenTags.get(9)));

            BasicDBObject searchQuery2 = new BasicDBObject().append("TagName", "Tags");

            twitterdata.update(searchQuery2, updateDocument);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
