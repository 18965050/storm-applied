package com.hp.schutale.storm.ch3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.code.geocoder.model.LatLng;

public class HeatmapBuilder extends BaseBasicBolt {

    private Map<Long, List<LatLng>> heatmaps;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time-interval", "hotzones"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        heatmaps = new HashMap<Long, List<LatLng>>();
    }

    public void execute(Tuple input, BasicOutputCollector collector) {

        if (isTickTuple(input)) {
            emitHeatmap(collector);
        } else {
            Long timeInterval = input.getLongByField("time-interval");
            LatLng geocode = (LatLng) input.getValueByField("geocode");

            List<LatLng> checkins = getCheckinsForInterval(timeInterval);
            checkins.add(geocode);
        }
    }

    private void emitHeatmap(BasicOutputCollector collector) {
        Long now = System.currentTimeMillis();
        Long emitUpToTimeInterval = selectTimeInterval(now);
        Set<Long> timeIntervalsAvailable = heatmaps.keySet();
        for (Long timeInterval : timeIntervalsAvailable) {
            if (timeInterval <= emitUpToTimeInterval) {
                List<LatLng> hotzones = heatmaps.remove(timeInterval);
                collector.emit(new Values(timeInterval, hotzones));
            }
        }
    }

    private Long selectTimeInterval(Long time) {
        return time / (15 * 1000);
    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();
        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private List<LatLng> getCheckinsForInterval(Long timeInterval) {
        List<LatLng> hotzones = heatmaps.get(timeInterval);
        if (hotzones == null) {
            hotzones = new ArrayList<LatLng>();
            heatmaps.put(timeInterval, hotzones);
        }
        return hotzones;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        return conf;
    }

}
