package trident;

import java.util.Random;

import com.google.common.collect.ImmutableList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;

public class LogAnalyserTrident {

	public static void main(String[] args) throws Exception {
		System.out.println("Log Analyser Trident");
		TridentTopology topology = new TridentTopology();
		FeederBatchSpout testSpout = new FeederBatchSpout(
				ImmutableList.of("fromMobileNumber", "toMobileNumber", "duration"));
		TridentState callCounts = topology.newStream("fixed-batch-spout", testSpout)
				.each(new Fields("fromMobileNumber", "toMobileNumber"), new FormatCall(), new Fields("call"))
				.groupBy(new Fields("call"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));
		LocalDRPC drpc = new LocalDRPC();
		topology.newDRPCStream("call_count", drpc).stateQuery(callCounts, new Fields("args"), new MapGet(),
				new Fields("count"));
		topology.newDRPCStream("multiple_call_count", drpc).each(new Fields("args"), new CSVSplit(), new Fields("call"))
				.groupBy(new Fields("call"))
				.stateQuery(callCounts, new Fields("call"), new MapGet(), new Fields("count"))
				.each(new Fields("call", "count"), new Debug()).each(new Fields("count"), new FilterNull())
				.aggregate(new Fields("count"), new Sum(), new Fields("sum"));
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("trident", conf, topology.build());
		Random randomGenerator = new Random();
		int idx = 0;
		while (idx < 10) {
			testSpout.feed(ImmutableList.of(new Values("1234123401", "1234123402", randomGenerator.nextInt(60))));
			testSpout.feed(ImmutableList.of(new Values("1234123401", "1234123403", randomGenerator.nextInt(60))));
			testSpout.feed(ImmutableList.of(new Values("1234123401", "1234123404", randomGenerator.nextInt(60))));
			testSpout.feed(ImmutableList.of(new Values("1234123402", "1234123403", randomGenerator.nextInt(60))));
			idx = idx + 1;
		}
		System.out.println("DRPC : Query starts");
		System.out.println(drpc.execute("call_count", "1234123401 - 1234123402"));
		System.out.println(drpc.execute("multiple_call_count", "1234123401 - 1234123402,1234123401 - 1234123403"));
		System.out.println("DRPC : Query ends");
		cluster.shutdown();
		drpc.shutdown();
		// DRPCClient client = new DRPCClient("drpc.server.location", 3772);
	}

}
