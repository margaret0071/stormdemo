package com.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt{
	private OutputCollector collector;
	private HashMap<String, Long> counts = null;

	public void prepare(Map map, TopologyContext topologycontext,
			OutputCollector outputcollector) {
		this.collector=outputcollector;
		this.counts = new HashMap<String, Long>();
	}

	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = this.counts.get(word);
		if(count == null){
			count = 0L;
		}
		count++;
		this.counts.put(word, count);
		this.collector.emit(new Values(word,count));
	}

	public void declareOutputFields(OutputFieldsDeclarer outputfieldsdeclarer) {
		outputfieldsdeclarer.declare(new Fields("word","count"));
	}

}
