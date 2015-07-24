package stormapplied.radio;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.utils.Utils;

public class LocalTopologyRunner {
  private static final int ONE_MINUTE = 60000;

  public static void main(String[] args) throws Exception {
    Config config = new Config();
    config.setDebug(false);

	LocalDRPC drpc = new LocalDRPC();
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("internet-radio-play-stats", config, TopologyBuilder.buildLocal(drpc));

    Utils.sleep(ONE_MINUTE);

    String result = drpc.execute("count-request-by-tag", "Classic Rock,Punk,Post Punk");
    System.out.println("RESULTS");
    System.out.println("==========================================================================");
    System.out.println(result);
    System.out.println("==========================================================================");

    cluster.killTopology("internet-radio-play-stats");
    cluster.shutdown();
    drpc.shutdown();
  }
}
