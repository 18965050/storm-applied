package com.hp.schutale.storm.ch2;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EmailExtractor extends BaseBasicBolt {

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("email"));
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String commit = input.getStringByField("commit");
        String[] parts = commit.split(" ");
        collector.emit(new Values(parts[1]));
    }
}
