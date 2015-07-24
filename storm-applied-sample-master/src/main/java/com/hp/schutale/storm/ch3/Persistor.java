package com.hp.schutale.storm.ch3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.code.geocoder.model.LatLng;

public class Persistor extends BaseBasicBolt {

    private final Logger log = LoggerFactory.getLogger(Persistor.class);

    private Jedis jedis;
    private ObjectMapper objectMapper;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        jedis = new Jedis("localhost");
        objectMapper = new ObjectMapper();
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        Long timeInterval = input.getLongByField("time-interval");
        List<LatLng> hz = (List<LatLng>) input.getValueByField("hotzones");
        List<String> hotzones = asListOfStrings(hz);

        try {
            String key = "checkins-" + timeInterval;
            String value = objectMapper.writeValueAsString(hotzones);
            log.info("### PERSISTING: [{}]=[{}]", key, value);
            jedis.set(key, value);
        } catch (Exception ex) {
            log.error("Error persisting for time: {}", timeInterval, ex);
        }
    }

    private List<String> asListOfStrings(List<LatLng> hotzones) {
        List<String> hotzonesStandard = new ArrayList<String>(hotzones.size());
        for (LatLng geoCoordinate : hotzones) {
            hotzonesStandard.add(geoCoordinate.toUrlValue());
        }
        return hotzonesStandard;
    }

    @Override
    public void cleanup() {
        if (jedis.isConnected()) {
            jedis.quit();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields
    }

}
