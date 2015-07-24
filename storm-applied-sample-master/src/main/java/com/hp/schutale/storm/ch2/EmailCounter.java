package com.hp.schutale.storm.ch2;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class EmailCounter extends BaseBasicBolt {

    private static Logger log = LoggerFactory.getLogger(EmailCounter.class);

    private Map<String, Integer> counts;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // NOOP as no output
    }

    @Override
    public void prepare(Map config, TopologyContext context) {
        counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String email = input.getStringByField("email");
        counts.put(email, countFor(email) + 1);
        printCounts();
    }

    private Integer countFor(String email) {
        Integer count = counts.get(email);
        return count == null ? 0 : count;
    }

    private void printCounts() {
        for (String email : counts.keySet()) {
            log.info("[{}] has count of [{}]", email, counts.get(email));
        }
    }
}
