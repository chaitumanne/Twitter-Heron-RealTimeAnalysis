package edu.umkc.calmcoders.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Created by manikanta on 11/10/16.
 */
public class TwitterSpout extends BaseRichSpout{


    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream _twitterStream;
    String _custkey;
    String _custsecret;
    String _accesstoken;
    String _accesssecret;
    HashtagEntity[] HashTagEntities;
    String HashTags = "";
    Double lattitude;
    Double longitude;
    String coordinates;
    String language;
    String country;

    public TwitterSpout(String key, String secret) {
        _custkey = key;
        _custsecret = secret;
    }

    public TwitterSpout(String key, String secret, String token, String tokensecret) {
        _custkey = key;
        _custsecret = secret;
        _accesstoken = token;
        _accesssecret = tokensecret;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = collector;

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onStallWarning(StallWarning warning) {
            }

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
            }
        };

        ConfigurationBuilder config =
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(_custkey)
                        .setOAuthConsumerSecret(_custsecret)
                        .setOAuthAccessToken(_accesstoken)
                        .setOAuthAccessTokenSecret(_accesssecret);

        TwitterStreamFactory fact =
                new TwitterStreamFactory(config.build());

        _twitterStream = fact.getInstance();
        _twitterStream.addListener(listener);
        _twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
            //System.out.println("geolocation"+ret.getGeoLocation());

            try{
                //Code for getting hashtag entities
                HashTagEntities = ret.getHashtagEntities();
                HashTags = " ";
                for (HashtagEntity ht : HashTagEntities) {
                    HashTags = HashTags + ht.getText() + ",";
                }
                //code for getting Geo Location of the tweets
                GeoLocation geo=ret.getGeoLocation();


                if (geo==null){
                    //coordinates="0,0";
                    lattitude=0.0;
                    longitude=0.0;

                }  else {
                    coordinates=geo.toString();
                    lattitude=geo.getLatitude();
                    longitude=geo.getLongitude();
                }

                //Code for Sentiment Analysis
                language = ret.getLang();

                Place place=ret.getPlace();

                if(place==null){
                    country="na";
                }
                else {
                    country=place.getCountry();
                }
            }
            catch (Exception e){ }




            _collector.emit(new Values(ret.getText(),ret.getSource(), HashTags, lattitude,longitude, language,country));
        }
    }

    @Override
    public void close() {
        _twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "source", "hashtags","lattitude","longitude", "language","country"));
    }
}
