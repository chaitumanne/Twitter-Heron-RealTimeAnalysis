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
 * Created by manikanta on 11/11/16.
 */
public class coordinatesFilterBolt extends BaseRichBolt {
    OutputCollector _collector;
    String taskName;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        taskName = context.getThisComponentId() + "_" + context.getThisTaskId();

    }

    @Override
    public void execute(Tuple tuple) {


        //System.out.println("lattitude: "+tuple.getDoubleByField("lattitude"));
        //System.out.println("lattitude: "+tuple.getDoubleByField("longitude"));

        Double lattitude=tuple.getDoubleByField("lattitude");
        Double longitude=tuple.getDoubleByField("longitude");


        if(lattitude!=0.0 && longitude!=0.0){
            System.out.println("lattitude: "+tuple.getDoubleByField("lattitude"));
            System.out.println("lattitude: "+tuple.getDoubleByField("longitude"));


            _collector.emit(tuple, new Values(lattitude,longitude));
            _collector.ack(tuple);

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("lattitude","longitude"));

    }
}
