package tutorial;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class CallLogCounterBolt implements IRichBolt {

	Map<String, Integer> counterMap;
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counterMap = new HashMap<String, Integer>();
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		String call = tuple.getString(0);
		Integer duration = tuple.getInteger(1);
		if (!counterMap.containsKey(call)) {
			counterMap.put(call, 1);
		} else {
			Integer c = counterMap.get(call) + 1;
			counterMap.put(call, c);
		}
		collector.ack(tuple);

	}

	public void cleanup() {
		for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
