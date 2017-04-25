package tutorial;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FakeCallLogReaderSpout implements IRichSpout {

	// Create instance for SpoutOutputCollector which passes tuples to bolt.
	private SpoutOutputCollector collector;
	private boolean completed = false;
	// Create instance for TopologyContext which contains topology data.
	private TopologyContext context;
	// Create instance for Random class.
	private Random randomGenerator = new Random();
	private Integer idx = 0;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
	}

	public void close() {

	}

	public void activate() {

	}

	public void deactivate() {

	}

	public void nextTuple() {
		if (this.idx <= 1000) {
			List<String> mobileNumbers = new ArrayList<String>();
			mobileNumbers.add("1234123401");
			mobileNumbers.add("1234123402");
			mobileNumbers.add("1234123403");
			mobileNumbers.add("1234123404");
			Integer localIdx = 0;
			while ((localIdx++ < 100) && (this.idx++ < 1000)) {
				String fromMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				String toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				while (fromMobileNumber == toMobileNumber) {
					toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				}
				Integer duration = randomGenerator.nextInt(60);
				this.collector.emit(new Values(fromMobileNumber, toMobileNumber, duration));
			}
		}

	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("from", "to", "duration"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
