package edu.umkc.calmcoders.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by manikanta on 11/13/16.
 */
public class countryFilterBolt extends BaseRichBolt{
    OutputCollector _collector;
    String taskName;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        taskName = context.getThisComponentId() + "_" + context.getThisTaskId();

    }

    @Override
    public void execute(Tuple tuple) {

        String country=tuple.getStringByField("country");


        if((!country.equals("na"))){
            //System.out.println(country);

            _collector.emit(tuple, new Values(country));
            _collector.ack(tuple);

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("country"));

    }
}
