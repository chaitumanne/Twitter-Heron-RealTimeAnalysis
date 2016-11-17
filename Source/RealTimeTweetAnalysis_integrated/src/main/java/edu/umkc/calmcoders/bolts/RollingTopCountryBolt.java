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

import java.util.*;
import java.util.Map.Entry;

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
public class RollingTopCountryBolt extends BaseRichBolt {


    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = LogManager.getLogger(RollingTopCountryBolt.class);
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

    public RollingTopCountryBolt() {
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public RollingTopCountryBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
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

        List<Map.Entry<Object, Long>> list = new LinkedList<Map.Entry<Object, Long>>(counts.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Object, Long>>(){
            public int compare(Map.Entry<Object, Long> o1, Map.Entry<Object, Long> o2){
                return(o2.getValue()).compareTo(o1.getValue());
            }
        });

        Map<Object, Long> sortedMap = new LinkedHashMap<Object, Long>();
        for(Map.Entry<Object, Long> e1 : list){
            sortedMap.put(e1.getKey(), e1.getValue());
        }

//      List<String> topTenTags = new ArrayList<String>();
//      for (Map.Entry<Object, Long> e : sortedMap.entrySet()){
//          if (topTenTags.size() > 9){
//              break;
//          }
//          topTenTags.add("#" + e.getKey() + "," + e.getValue());
//      }
        //Long maxValueInMap=(Collections.max(counts.values()));
        //System.out.println("Should come only once"+maxValueInMap);
//      for (Entry<Object, Long> entry : counts.entrySet()) {
//          if (entry.getValue()==maxValueInMap) {
//              System.out.println("max count country: "+entry.getKey());// Print the key with max value
//              System.out.println("max count: "+maxValueInMap);
//          }
//      }


        JSONArray ja = new JSONArray();
        for (Entry<Object, Long> entry : sortedMap.entrySet()) {
            //if (entry.getValue()==Collections.max(counts.values())) {
            //System.out.println("max count country: "+entry.getKey());// Print the key with max value
            //System.out.println("max count: "+Collections.max(counts.values()));

            MongoClientURI uri = new MongoClientURI("mongodb://manikanta:manikanta@ds035836.mlab.com:35836/twitterdata");
            MongoClient client = new MongoClient(uri);

            DB db = client.getDB(uri.getDatabase());
            DBCollection location = db.getCollection("countriesdata");

            BasicDBObject searchQuery = new BasicDBObject().append("countries", "topcountries");

            BasicDBObject topcountries = new BasicDBObject();

            JSONObject jo = new JSONObject();
            jo.put("country",entry.getKey());
            jo.put("count", entry.getValue());

            ja.add(jo);
            //System.out.println("updated array: "+ Arrays.deepToString(ja.toArray()));
            topcountries.put("topcountries",ja);
            BasicDBObject updateDocument = new BasicDBObject();
            updateDocument.append("$set", new BasicDBObject().append("topcountries", ja));
            location.update(searchQuery, updateDocument);

            //}
            Object obj = entry.getKey();
            Long count = entry.getValue();
            collector.emit(new Values(obj, count, actualWindowLengthInSeconds));
        }
    }

    private void countObjAndAck(Tuple tuple) {
//    Object obj = tuple.getValue(0);
        Object obj = tuple.getStringByField("country");
        counter.incrementCount(obj);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("country", "count", "actualWindowLengthInSeconds"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
