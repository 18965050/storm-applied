package com.hp.schutale.storm.ch2;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class LocalTopologyRunner {

    private static final int TWO_SECONDS = 2000;

    public void runTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("commit-feed-listener", new CommitFeedListener());

        builder.setBolt("email-extractor", new EmailExtractor()).shuffleGrouping("commit-feed-listener");

        builder.setBolt("email-counter", new EmailCounter()).fieldsGrouping("email-extractor", new Fields("email"));

        Config config = new Config();
        // config.setDebug(true);

        StormTopology topology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("github-commit-count", config, topology);

        Utils.sleep(TWO_SECONDS);
        cluster.killTopology("github-commit-count");
        cluster.shutdown();

    }

}
