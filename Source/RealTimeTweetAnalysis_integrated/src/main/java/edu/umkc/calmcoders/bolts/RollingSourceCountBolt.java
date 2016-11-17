package edu.umkc.calmcoders.bolts;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mongodb.*;
import edu.umkc.calmcoders.tools.NthLastModifiedTimeTracker;
import edu.umkc.calmcoders.tools.SlidingWindowCounter;
import edu.umkc.calmcoders.util.TupleHelpers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by manikanta on 11/13/16.
 */

/**
 * This bolt performs rolling counts of incoming objects, i.e. sliding window based counting.
 * <p/>
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * <p/>
 * The bolt emits a rolling count tuple per object, consisting of the object itself, its latest rolling count, and the
 * actual duration of the sliding window. The latter is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high system load. Note that the actual
 * window length is tracked and calculated for the window, and not individually for each object within a window.
 * <p/>
 * Note: During the startup phase you will usually observe that the bolt warns you about the actual sliding window
 * length being smaller than the expected length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during startup (e.g. you will see this warning
 * during the first ~ five minutes of startup time if the window length is set to five minutes).
 */
public class RollingSourceCountBolt extends BaseRichBolt {


  private static final long serialVersionUID = 5537727428628598519L;
  private static final Logger LOG = LogManager.getLogger(RollingSourceCountBolt.class);
  private static final int NUM_WINDOW_CHUNKS = 5;
  private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
  private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
  private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
      "Actual window length is %d seconds when it should be %d seconds"
          + " (you can safely ignore this warning during the startup phase)";

  private SlidingWindowCounter<Object> counter;
  private final int windowLengthInSeconds;
  private final int emitFrequencyInSeconds;
  private OutputCollector collector;
  private NthLastModifiedTimeTracker lastModifiedTracker;

  public RollingSourceCountBolt() {
    this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
  }

  public RollingSourceCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
    this.windowLengthInSeconds = windowLengthInSeconds;
    this.emitFrequencyInSeconds = emitFrequencyInSeconds;
  }

  private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
    return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    counter = new SlidingWindowCounter<Object>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
        this.emitFrequencyInSeconds));

    this.collector = collector;
    lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
        this.emitFrequencyInSeconds));
  }

  @Override
  public void execute(Tuple tuple) {
    if (TupleHelpers.isTickTuple(tuple)) {
      LOG.debug("Received tick tuple, triggering emit of current window counts");
      emitCurrentWindowCounts();
    }
    else {
      countObjAndAck(tuple);
    }
  }

  private void emitCurrentWindowCounts() {
    Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
    int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
    lastModifiedTracker.markAsModified();
    if (actualWindowLengthInSeconds != windowLengthInSeconds) {
      LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
    }
    emit(counts, actualWindowLengthInSeconds);
  }

  private void emit(Map<Object, Long> counts, int actualWindowLengthInSeconds) {

      Long androidCount = counts.get("androidCount");
      Long webCount = counts.get("webCount");
      Long appleCount = counts.get("appleCount");
      Long windowsCount = counts.get("windowsCount");
      Long blackberryCount;
      if(counts.get("blackberryCount")==null){blackberryCount=(long) 0.0;} else {blackberryCount=counts.get("blackberryCount");}
      Long tweetCount = counts.get("tweetCount");
      Long mobileCount = counts.get("mobileCount");
      //for rolling source count
      MongoClientURI uri = new MongoClientURI("mongodb://manikanta:6353@ds141697.mlab.com:41697/rbdlab7");
      MongoClient client = new MongoClient(uri);
      DB db = client.getDB(uri.getDatabase());
      DBCollection users = db.getCollection("keyframescount");
      BasicDBObject update1 = new BasicDBObject();
      BasicDBObject Search1 = new BasicDBObject();
      update1.append("$set", new BasicDBObject()
              .append("Android_Count", androidCount)
              .append("Web_Count", webCount)
              .append("iPhone_Count", appleCount)
              .append("Windows_Count", windowsCount)
              .append("Blackberry_Count", blackberryCount)
              .append("TotalTweet_Count", tweetCount)
              .append("Mobile_Count",mobileCount));
      Search1.append("count", "sourcecount");
      users.update(Search1,update1);
      System.out.println("Tweets count: " +tweetCount+ " Mobile Count: "+mobileCount + " Web:"+webCount +" iPhone Count "+appleCount+" Windows Count "+windowsCount+" Blackberry Count "+blackberryCount+" Android Count "+androidCount);



//      JSONArray ja = new JSONArray();
//      for (Entry<Object, Long> entry : counts.entrySet()) {
//          if (entry.getValue()==Collections.max(counts.values())) {
//              System.out.println("max count country: "+entry.getKey());// Print the key with max value
//              System.out.println("max count: "+Collections.max(counts.values()));
//
//              MongoClientURI uri = new MongoClientURI("mongodb://manikanta:manikanta@ds035836.mlab.com:35836/twitterdata");
//              MongoClient client = new MongoClient(uri);
//
//              DB db = client.getDB(uri.getDatabase());
//              DBCollection location = db.getCollection("topcountry");
//
//              BasicDBObject searchQuery = new BasicDBObject().append("country", "topcountry");
//
//              BasicDBObject topcountries = new BasicDBObject();
//
//              JSONObject jo = new JSONObject();
//              jo.put("country",entry.getKey());
//              jo.put("count", Collections.max(counts.values()));
//
//              ja.add(jo);
//              //System.out.println("updated array: "+ Arrays.deepToString(ja.toArray()));
//              topcountries.put("topcountries",ja);
//              BasicDBObject updateDocument = new BasicDBObject();
//              updateDocument.append("$set", new BasicDBObject().append("topcountries", ja));
//              location.update(searchQuery, updateDocument);
//
//          }
//
//      Object obj = entry.getKey();
//      Long count = entry.getValue();
//      collector.emit(new Values(obj, count, actualWindowLengthInSeconds));
//    }

  }

  private void countObjAndAck(Tuple tuple) {
//    Object obj = tuple.getValue(0);
    //Object obj = tuple.getStringByField("country");

      counter.incrementCount("tweetCount");
      Pattern p = Pattern.compile("\\b(https|http)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", Pattern.CASE_INSENSITIVE);
      Pattern p2 = Pattern.compile("(?<=>)(.*)(?=<)", Pattern.CASE_INSENSITIVE);
      String sourcetype = tuple.getStringByField("source");
      Matcher m = p.matcher(sourcetype);
      Matcher m1 = p2.matcher(sourcetype);

      while (m.find()) {
      }

      while (m1.find()) {
          System.out.println("Operation System is: " + m1.group());
          if(m1.group().contains("Android"))
          {

              counter.incrementCount("mobileCount");
              counter.incrementCount("androidCount");
          }
          else if(m1.group().contains("iPhone"))
          {
              counter.incrementCount("mobileCount");
              counter.incrementCount("appleCount");
          }
          else if(m1.group().contains("Blackberry")){
                            counter.incrementCount("mobileCount");
              counter.incrementCount("blackberryCount");
          }
          else if(m1.group().contains("Windows")){
              counter.incrementCount("mobileCount");
              counter.incrementCount("windowsCount");
          }
          else
          {

              counter.incrementCount("webCount");
          }
      }

      //counter.incrementCount(obj);
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //declarer.declare(new Fields("country", "count", "actualWindowLengthInSeconds"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
    return conf;
  }
}
