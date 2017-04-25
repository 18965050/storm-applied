package tutorial;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CallLogCreatorBolt implements IRichBolt {

	// Create instance for OutputCollector which collects and emits tuples to produce output
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		String from = tuple.getString(0);
		String to = tuple.getString(1);
		Integer duration = tuple.getInteger(2);
		collector.emit(new Values(from + "-" + to, duration));
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call", "duration"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
