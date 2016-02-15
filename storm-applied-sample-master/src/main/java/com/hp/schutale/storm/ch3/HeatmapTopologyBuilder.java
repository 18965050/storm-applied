package com.hp.schutale.storm.ch3;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class HeatmapTopologyBuilder {

    public StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("checkins", new Checkins(), 4);	//4个executors
        builder.setBolt("geocode-lookup", new GeocodeLookup(), 8).setNumTasks(64).shuffleGrouping("checkins");	//8个executors,64个tasks. 为什么这么大是由于此bolt耗时组要在网络IO上面
        builder.setBolt("heatmap-builder", new HeatmapBuilder(), 4).fieldsGrouping("geocode-lookup",
                new Fields("time-interval", "city"));
        builder.setBolt("persistor", new Persistor(), 1).setNumTasks(4).shuffleGrouping("heatmap-builder");

        return builder.createTopology();
    }
}
