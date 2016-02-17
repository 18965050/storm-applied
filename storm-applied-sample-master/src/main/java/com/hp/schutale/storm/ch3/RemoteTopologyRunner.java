package com.hp.schutale.storm.ch3;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;

public class RemoteTopologyRunner {
  public static void main(String[] args) throws Exception {

    Config config = new Config();
    //config.setDebug(true);
    config.setNumWorkers(2);
    config.setMessageTimeoutSecs(60);
    //config.setNumAckers(1);

    StormSubmitter.submitTopology("local-heatmap", config, HeatmapTopologyBuilder.build());
  }
}