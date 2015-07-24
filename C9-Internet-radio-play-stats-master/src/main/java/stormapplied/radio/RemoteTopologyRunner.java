package stormapplied.radio;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;

public class RemoteTopologyRunner {
  public static void main(String[] args) throws Exception {

    Config config = new Config();
    config.setDebug(true);
    config.setNumWorkers(1);
    config.setNumAckers(1);

    StormSubmitter.submitTopology("internet-radio-play-stats", config, TopologyBuilder.buildRemote());
  }
}