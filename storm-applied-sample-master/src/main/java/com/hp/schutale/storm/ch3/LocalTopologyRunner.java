package com.hp.schutale.storm.ch3;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

public class LocalTopologyRunner {

    private static final long MORE_THAN_TWO_MINUTES = 150000;

    public void runTopology() {
        Config config = new Config();

        StormTopology topology = new HeatmapTopologyBuilder().build();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("local-heatmap", config, topology);

        Utils.sleep(MORE_THAN_TWO_MINUTES);
        cluster.killTopology("local-heatmap");
        cluster.shutdown();

    }

}
