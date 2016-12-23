package com.hp.schutale.storm.ch2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CommitFeedListener extends BaseRichSpout {

    private static Logger log = LoggerFactory.getLogger(CommitFeedListener.class);

    private SpoutOutputCollector outputCollector;
    private List<String> commits;
    private Iterator<String> iter;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("commit"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;

        try {
            commits = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("changelog.txt"), Charset
                    .defaultCharset().name());
            iter=commits.iterator();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void nextTuple() {
    	
    	while (iter.hasNext()){
    		this.outputCollector.emit(new Values(iter.next()));

    	}
    	
//        log.info("### emitting tuples again");
//        for (String commit : commits) {
//            outputCollector.emit(new Values(commit));
//        }
    }

}
