package edu.umkc.calmcoders;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.umkc.calmcoders.bolts.*;
import edu.umkc.calmcoders.spouts.TwitterSpout;


/**
 * Created by manikanta on 11/10/16.
 */
public class TweetTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //Spout to generate tweets
        builder.setSpout("word",
                new TwitterSpout("d4qBKAOldFpihNE0MAVAQbFcZ",
                        "wqgkwoLfDYYuWwEBx7qtfoQ9VMFqz4bkB8pi0gWNp25OtWzNSK",
                        "600259139-pzfcBfEk6xB9zkNILaXqaZMVgZxUQy4V0nBpwsWS",
                        "4vhPRM6cNMPuNZVXkC7uRQIFnAoSTc40uzikjMo5y9mAm"), 1);

        //Bolt for Top Ten Hashtags
        builder.setBolt("hashTagBolt", new MongoDriverBolt(), 1).shuffleGrouping("word");

        //Bolt for Geo Location
        builder.setBolt("coordinates", new coordinatesFilterBolt(),3).shuffleGrouping("word");
        builder.setBolt("locationsavetomongodb",new locationMongoDB(),1).shuffleGrouping("coordinates");

        //Bolt for Sentiment Analysis
        builder.setBolt("sentimentBolt", new SentimentAnalysisBolt(), 1).shuffleGrouping("word");

        //for top countries
        builder.setBolt("countryfilter", new countryFilterBolt(),3).shuffleGrouping("word");
        builder.setBolt("countrycounts",new RollingTopCountryBolt(3600,300),1).fieldsGrouping("countryfilter", new Fields("country"));
        builder.setBolt("sourcecounts",new RollingSourceCountBolt(120,2),1).shuffleGrouping("word");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
        }
    }
}
